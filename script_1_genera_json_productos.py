import pandas as pd
import boto3
import json
import time
import funcionesAws as fa
import funcionesNeo4j as fn
import funcionesNeo4jEC2 as fne

import os
from dotenv import load_dotenv

INSTANCIA_EC2 = 'Neo4j-EC2'

load_dotenv("secrets.env")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


# ─── CONNECT TO NEO4J OFF-INSTANCE ──────────────────────────────────────────────
ec2 = boto3.client('ec2', region_name='us-east-1')
instance_id, public_ip, state = fne.find_instance_by_name(ec2, INSTANCIA_EC2)
bolt_uri = f"bolt://{public_ip}:7687" # principal instance
# bolt_uri = f"bolt://{public_ip}:7688" # neo4j-dev-instance
conn_neo4j = fn.Neo4jConnection(
    uri=bolt_uri,
    user=NEO4J_USER,
    pwd=NEO4J_PASSWORD,
    encrypted=False)


fileDir = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"
INPUT_FILE = "num_proveedores_por_producto.csv"
OUTPUT_FILE = "detalle_productos_proveedor.jsonl"
CANONICAL_CKPT = f"{fileDir}productos_genericos.jsonl"


BATCH_SIZE = 5
MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"


# ─── BEDROCK CLIENT ───────────────────────────────────────────────
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")


# ─── LOAD REGIONS ────────────────────────────────────────────────
with open("regiones_chile.json", "r", encoding="utf-8") as f:
    regiones_data = json.load(f)
REGIONES_LIST = "\n".join(
    f'  - {r["name"]} (número romano: {r["roman"]})'
    for r in regiones_data["regions"]
)

# ─── PROMPT TEMPLATE ─────────────────────────────────────────────
SYSTEM_PROMPT = """Eres un experto en catalogación de productos de compras públicas en Chile (ChileCompra).
Para cada descripción de producto, extrae información estructurada en formato JSON.

Cada descripción de entrada es la concatenación (separada por " | ") de la especificación del comprador y la especificación de un proveedor para el mismo ítem.

REGLAS CRÍTICAS:
1. La especificación del COMPRADOR (primer fragmento, antes de " | ") es la FUENTE AUTORITATIVA sobre qué producto general es (familia/categoría). El campo product_generico DEBE reflejar el producto descrito por el comprador. NO lo reemplaces por un producto de otra categoría.
2. La especificación del PROVEEDOR describe LO QUE EL PROVEEDOR OFRECE PARA ESE ÍTEM — es la oferta real. Aunque el proveedor ofrezca una VARIANTE distinta a lo pedido (ej. comprador pide "inflable" y proveedor ofrece "maleable semirrígida", comprador pide formato X y proveedor ofrece formato Y), SIGUE EXTRAYENDO TODOS SUS ATRIBUTOS (marca, dimensiones, modelo, procedencia, etc.). La variante específica se refleja en atributos_adicionales o en product_generico si corresponde a una subcategoría clara, pero NO se usa para anular el resto de los campos.
3. SÓLO ignora el contenido del proveedor si describe un producto de una CATEGORÍA COMPLETAMENTE DISTINTA (ej. comprador pide prótesis peneana y proveedor habla de anclas de sutura para rodilla).
4. Los conflictos se manejan CAMPO POR CAMPO, no en bloque. Si un campo específico está en conflicto directo entre comprador y proveedor (el comprador dice color rojo y el proveedor dice azul, el comprador pide cantidad 10 y el proveedor ofrece 5), deja SÓLO ese campo en null — NO anules los demás campos que no están en conflicto (marca, procedencia, dimensiones, modelo se extraen igualmente del proveedor).
5. Extrae información SOLAMENTE de la descripción explícita. NUNCA inventes datos ni asumas atributos no escritos. Dimensiones, cantidades o modelos que no aparecen literalmente en el texto deben quedar en null.
6. Cuando la especificación del COMPRADOR es genérica o apunta a un documento adjunto (ej. "VER DETALLES EN ESPECIFICACIONES TECNICAS ADJUNTAS", "según anexo", "reparación de X"), NUNCA devuelvas todos los campos en null. La especificación del PROVEEDOR sigue siendo válida: extrae marca, dimensiones, condiciones_despacho y atributos_adicionales a partir de ella.
7. Reconoce SIEMPRE el patrón "[CÓDIGO] [descripción en español o inglés] [MARCA] [PAÍS] vigencia N [meses|años] despacho en N [horas|días]" y extrae TODOS sus componentes, incluso si la descripción central está en inglés o usa abreviaturas técnicas. Las medidas (ej. "5.0mm", "5.5 x 16.3 mm") van en dimensiones; vigencia/procedencia/código/modelo van en atributos_adicionales; "despacho en N horas/días" va en condiciones_despacho; la marca (ej. CONMED, PROMEDON, RIGICON) va en marca.

Reglas:
- product_generico: nombre genérico normalizado del producto que identifica el SKU solicitado por el comprador. INCLUYE los atributos que definen la identidad/SKU del producto cuando aparecen en la especificación del COMPRADOR: número de vías, calibre/diámetro (FR, Ch), talla, longitud, material cuando es parte del nombre del producto. Ejemplos: "sonda Foley silicona 3 vías FR 22", "canastillo Dormia 1.5 Fr 115 cm", "guante nitrilo talla L", "resma papel fotocopia carta". NO incluyas: marca/nombre comercial, cantidad, tipo de paquete, ni procedencia. Estos atributos definitorios también se repiten en los campos correspondientes (dimensiones, etc.) para su extracción estructurada.
- marca: marca, nombre comercial o fabricante del producto si aparece mencionado. IMPORTANTE: reconoce marcas aunque estén escritas en MAYÚSCULAS, precedidas por la palabra "MARCA", o yuxtapuestas a un país de procedencia (ej. "PROMEDON Argentina", "RIGICON USA"). Ejemplos típicos en este dominio: PROMEDON, RIGICON, Boston Scientific, AMS, Remeex, NUSIL, HP. Si se menciona una marca que claramente corresponde a un accesorio o componente secundario (no al producto principal), NO la uses como marca; anótala en atributos_adicionales. Usa null sólo si realmente no hay una marca del producto principal.
  EXCEPCIÓN CRÍTICA — EPÓNIMOS Y NOMBRES GENÉRICOS: NO confundas nombres epónimos o términos genéricos del tipo de producto con una marca. Ejemplos: "Dormia" (canastillo Dormia = canastillo extractor de cálculos), "Foley" (sonda Foley), "Yankauer" (cánula Yankauer), "Fogarty" (catéter Fogarty), "Tenckhoff" (catéter Tenckhoff), "Nelaton" (sonda Nelaton), "Pezzer" (sonda Pezzer), "Malecot" (sonda Malecot). Aunque aparezcan en mayúsculas o como primera palabra después del tipo de producto, estos términos describen el TIPO/FAMILIA del producto y pertenecen a product_generico (ej. "canastillo Dormia", "sonda Foley"), NO a marca. En estos casos marca debe ser null salvo que se mencione una marca comercial adicional (ej. "sonda Foley MARCA Bard" → marca: "Bard").
- tipo_paquete: tipo de envase o empaque (caja, bolsa, botella, rollo, saco, tambor, resma, kit, etc.), sino null.
- dimensiones: medidas o tamaño físico del producto. Incluye:
    * tamaños estándar: "carta", "A4", "oficio"
    * tallas: "talla L", "XL"
    * medidas numéricas con unidad: "30x40 cm", "2.7 mm", "1/2 pulgada"
    * listas o rangos de medidas disponibles: "12, 15, 18, 20, 22 mm", "3.75–6.25 mm"
    * medidas de componentes clave (cuff, aguja, balón, calibre): "aguja 2.7 mm", "cuff 3.75–6.25 mm"
    * diámetro en French/Fr usado en sondas y catéteres: "14 Fr"
  REGLA CLAVE: si el proveedor menciona VARIAS tallas o medidas disponibles (ej. "tallas M, L y XL", "medidas 12, 15, 18 mm", "5 tamaños de cuff"), el campo dimensiones DEBE incluir la lista COMPLETA del proveedor (ej. "talla M, L, XL"), NO sólo la medida específica que pidió el comprador. Que el comprador pida una talla/medida incluida dentro de la oferta del proveedor NO es un conflicto: el proveedor ofrece todas esas medidas.
  Usa null sólo si no hay ninguna medida física en el texto.
- cantidad: cantidad numérica si se menciona, sino null.
- unidad: unidad de medida (kg, litros, unidades, metros, hojas, etc.), sino null.
- condiciones_despacho: condiciones, plazo o términos de entrega si se mencionan (ej. "despacho en 12 horas", "entrega en 5 días hábiles", "envío gratis a todo Chile", "retiro en bodega"), sino null.
- atributos_adicionales: lista de atributos adicionales relevantes que no encajan en los campos anteriores (variante, color, sabor, modelo, material, procedencia, garantía, esterilización, etc.). Lista vacía si no hay.

Responde SOLO con un JSON array, sin texto adicional. Ejemplo:

Input:
1. RESMA PAPEL FOTOCOPIA CARTA 500 HOJAS
2. TONER HP 85A NEGRO
3. GUANTE NITRILO TALLA L REGIÓN DE VALPARAÍSO
4. PROTESIS INFLABLE MARCA RIGICON, MEDIDAS 12, 15, 18, 20, 22 MM, PROCEDENCIA USA, GARANTIA 12 AÑOS
5. PR-T100 Protesis peneana maleable semirrigida de 4 tamaños PROMEDON Argentina vigencia 36 meses despacho en 12 horas
6. [Reparación del aparato extensor de la rodilla. VER DETALLES EN ESPECIFICACIONES TECNICAS ADJUNTAS.] | [CN-CF6140HN super revo-ft 5.0mm suture anchor w/ 2 h CONMED EE.UU vigencia 12 meses despacho en 12 horas]
7. [340-5900 PROTESIS TESTICULAR TALLA M] | [PR-T-MEDIUM PROTESIS TESTICULAR DE ELASTOMERO DE SILICONA, MATERIAL SOLIDO, ALTA RESISTENCIA, NO PROVOCA IRRITACION, TALLAS, M, L y XL PROMEDON Argentina vigencia 24 meses despacho en 1 dia]

Output:
[
  {"product_generico": "papel fotocopia", "marca": null, "tipo_paquete": "resma", "dimensiones": "carta", "cantidad": 500, "unidad": "hojas", "condiciones_despacho": null, "atributos_adicionales": []},
  {"product_generico": "toner", "marca": "HP", "tipo_paquete": null, "dimensiones": null, "cantidad": null, "unidad": null, "condiciones_despacho": null, "atributos_adicionales": ["modelo: 85A", "color: negro"]},
  {"product_generico": "guante nitrilo", "marca": null, "tipo_paquete": null, "dimensiones": "talla L", "cantidad": null, "unidad": null, "condiciones_despacho": null, "atributos_adicionales": ["ubicación: Región de Valparaíso"]},
  {"product_generico": "prótesis inflable", "marca": "RIGICON", "tipo_paquete": null, "dimensiones": "12, 15, 18, 20, 22 mm", "cantidad": null, "unidad": null, "condiciones_despacho": null, "atributos_adicionales": ["procedencia: USA", "garantía: 12 años"]},
  {"product_generico": "prótesis peneana maleable semirrígida", "marca": "PROMEDON", "tipo_paquete": null, "dimensiones": "4 tamaños", "cantidad": null, "unidad": null, "condiciones_despacho": "despacho en 12 horas", "atributos_adicionales": ["procedencia: Argentina", "vigencia: 36 meses", "código: PR-T100"]},
  {"product_generico": "ancla de sutura", "marca": "CONMED", "tipo_paquete": null, "dimensiones": "5.0 mm", "cantidad": null, "unidad": null, "condiciones_despacho": "despacho en 12 horas", "atributos_adicionales": ["código: CN-CF6140HN", "modelo: super revo-ft", "procedencia: EE.UU", "vigencia: 12 meses"]},
  {"product_generico": "prótesis testicular", "marca": "PROMEDON", "tipo_paquete": null, "dimensiones": "talla M, L, XL", "cantidad": null, "unidad": null, "condiciones_despacho": "despacho en 1 día", "atributos_adicionales": ["código: 340-5900", "código proveedor: PR-T-MEDIUM", "material: elastómero de silicona", "característica: alta resistencia", "característica: no provoca irritación", "procedencia: Argentina", "vigencia: 24 meses"]}
]"""


def build_prompt(nombres):
    items = "\n".join(f"{i+1}. {n}" for i, n in enumerate(nombres))
    return f"Input:\n{items}\n\nOutput:"


def parse_response(text, batch_size):
    text = text.strip()
    start = text.find("[")
    end = text.rfind("]") + 1
    if start != -1 and end > start:
        text = text[start:end]
    parsed = json.loads(text)
    if len(parsed) != batch_size:
        print(f"  WARN: expected {batch_size} items, got {len(parsed)}")
    return parsed


SUMMARY_FIELDS = ["product_generico", "marca", "tipo_paquete", "dimensiones",
                   "cantidad", "unidad", "condiciones_despacho",
                   "atributos_adicionales"]

def build_summary(item):
    parts = []
    for field in SUMMARY_FIELDS:
        val = item.get(field)
        if val is not None and val != []:
            parts.append(f"{field}: {val}")
    return " | ".join(parts)


# ─── PROCESS BATCHES ──────────────────────────────────────────────

query = f"""
MATCH (target:Proveedor {{rut:'78.566.250-4'}})-[:OFRECE]->(Oferta)-[:PARA_ITEM]->(it:ItemLicitacion)<-[:REQUIERE_ITEM]-(lic:Licitacion)
MATCH (bidder:Proveedor)-[]->(of:Oferta)-[]->(it)
WHERE of.precio_equiv_clp > 1000 AND lic.buyer_id IS NOT NULL
  OPTIONAL MATCH (it)-[:ES_PRODUCTO_DE]->(pu:Producto_Ungm)
  RETURN
    bidder.rut             AS rut_proveedor,
    it.id_item             AS id_item,
    lic.buyer_id           AS buyer_id,
    toString(lic.fecha_cierre) AS fecha_cierre,
    of.precio_unitario      AS precio_unitario,
    of.precio_equiv_clp     AS precio_clp,
    of.moneda               AS moneda,
    it.specs_comprador    AS specs_comprador,
    of.specs_proveedor      AS specs_proveedor
  ORDER BY id_item, rut_proveedor;
"""
df = fn.neo4jToDataframe(query, conn_neo4j)

print(f"Total filas: {len(df)}")
print(f"Columnas: {df.columns.tolist()}")

if df.empty:
    raise RuntimeError("La consulta Neo4j no devolvió filas. Revisa conexión o datos.")

if "id_item" not in df.columns:
    raise KeyError(
        f"La columna 'id_item' no está en el DataFrame. "
        f"Columnas disponibles: {df.columns.tolist()}"
    )

print(f"Total id_items distintos: {df['id_item'].nunique()}")

df["specs_item"] = [
    f"{sc} | {sp}" if sp is not None and str(sp).strip() else sc
    for sc, sp in zip(df["specs_comprador"], df["specs_proveedor"])
]

TEST_MODE = False #True
TEST_ITEMS_LIMIT = 20

unique_ids = df["id_item"].drop_duplicates().tolist()
if TEST_MODE:
    unique_ids = unique_ids[:TEST_ITEMS_LIMIT]

df_subset = df[df["id_item"].isin(unique_ids)].reset_index(drop=True)
print(f"Procesando {len(unique_ids)} id_items ({len(df_subset)} filas item-proveedor)...")


# ─── ONE LLM CALL PER id_item FOR CANONICAL product_generico ────
GENERICO_SYSTEM_PROMPT = """Eres un experto en catalogación de productos de compras públicas en Chile (ChileCompra).
Dada la siguiente descripción (que concatena, separadas por " | ", la especificación del comprador y las especificaciones de TODOS los proveedores para un mismo ítem), extrae SOLAMENTE el nombre genérico normalizado del producto.

REGLAS:
- product_generico: nombre genérico normalizado del producto que identifica el SKU solicitado por el comprador. INCLUYE los atributos que definen la identidad/SKU del producto cuando aparecen en la especificación del COMPRADOR: número de vías, calibre/diámetro (FR, Ch), talla, longitud, material cuando es parte del nombre del producto. Ejemplos válidos: "sonda Foley silicona 3 vías FR 22", "canastillo Dormia 1.5 Fr 115 cm", "guante nitrilo talla L", "resma papel fotocopia carta". NO incluyas: marca/nombre comercial, cantidad, tipo de paquete, ni procedencia.
- La especificación del COMPRADOR (primer fragmento, antes del primer " | ") es la FUENTE AUTORITATIVA sobre la familia/categoría del producto Y sobre sus atributos definitorios (FR, talla, vías, etc.).
- Usa la información de los proveedores sólo para desambiguar o normalizar el nombre genérico, no para cambiar la categoría ni los atributos pedidos.
- El mismo id_item debe producir un único product_generico consistente para todos sus proveedores.

Responde SOLO con un JSON object, sin texto adicional. Ejemplo:
{"product_generico": "papel fotocopia"}"""


def build_generico_prompt(combined_spec):
    return f"{GENERICO_SYSTEM_PROMPT}\n\nInput:\n{combined_spec}\n\nOutput:"


def parse_generico_response(text):
    text = text.strip()
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        text = text[start:end]
    return json.loads(text)["product_generico"]


canonical_generico = {}
if os.path.exists(CANONICAL_CKPT):
    with open(CANONICAL_CKPT, "r", encoding="utf-8") as ckpt:
        for line in ckpt:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            canonical_generico[entry["id_item"]] = entry["product_generico"]
    print(f"Reanudando canonical: {len(canonical_generico)} ya procesados")

try:
    for i, id_item in enumerate(unique_ids):
        if id_item in canonical_generico:
            continue
        rows = df_subset[df_subset["id_item"] == id_item]
        sc = rows.iloc[0]["specs_comprador"]
        sps = [sp for sp in rows["specs_proveedor"].tolist()
               if sp is not None and str(sp).strip()]
        combined = " | ".join([sc] + sps)

        print(f"Canonical {i+1}/{len(unique_ids)} (id_item={id_item})...", end=" ")
        try:
            response_text = fa.invoke_bedrock(
                bedrock, build_generico_prompt(combined), model_id=MODEL_ID)
            generico = parse_generico_response(response_text)
            canonical_generico[id_item] = generico
            with open(CANONICAL_CKPT, "a", encoding="utf-8") as ckpt:
                ckpt.write(json.dumps(
                    {"id_item": id_item, "product_generico": generico},
                    ensure_ascii=False) + "\n")
            print(f"OK ({generico})")
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
        time.sleep(1)
except KeyboardInterrupt:
    print("\nInterrumpido. Progreso canonical guardado en checkpoint.")
    raise SystemExit(0)



# MATCH (prov:Proveedor {rut:'78.566.250-4'})-[]-(lic:Licitacion)-[]-(it:ItemLicitacion)-[]-(prod:ProductoCanonico)
# MATCH (competencia:Proveedor)-[]-(of:Oferta)-[]-(it)
# RETURN competencia.nombre_legal       AS nombre_prov,
#     prod.nombre_generico            AS producto,
#     lic.fecha_cierre                AS fecha_cierre,
#     lic.id_licitacion               AS id_licitacion,
#     it.id_item_licitacion           AS item_licitacion,
#     toInteger(of.precio_unitario)   AS precio_unitario,
#     toInteger(of.cantidad)          AS cantidad,
#     toInteger(of.precio_total)      AS precio_total,
#     of.adjudicada                   AS flag_adjudicacion,
#     it.especificacion_comprador     AS specs_comprador
# ORDER BY producto, fecha_cierre


# MATCH (prov:Proveedor {rut:'78.566.250-4'})-[]-(lic:Licitacion)-[]-(it:ItemLicitacion)-[]-(prod:ProductoCanonico)  MATCH (:Proveedor)-[]-(of:Oferta)-[]-(it)
# WITH DISTINCT prod.nombre_generico AS producto, lic, of 
# RETURN producto, 
#         toInteger(max(of.precio_unitario))                 AS precio_max,
#         toInteger(min(of.precio_unitario))                 AS precio_min,
#         toInteger(percentileCont(of.precio_unitario, 0.5)) AS precio_mediana,
#         toInteger(max(CASE WHEN of.adjudicada THEN of.precio_unitario END)) AS precio_adjudicado_max,
#         toInteger(min(CASE WHEN of.adjudicada THEN of.precio_unitario END)) AS precio_adjudicado_min,
#        count(DISTINCT of)                                 AS n_ofertas,
#        count(DISTINCT lic)                                AS n_licitaciones
# ORDER BY producto


# MATCH (prov:Proveedor)-[]-(of:Oferta)
#   WITH prov,
#        count(of)                                            AS n_ofertas, 
#        sum(CASE WHEN of.adjudicada THEN 1 ELSE 0 END)       AS n_adjudicadas 
#   WHERE n_adjudicadas >= 1000
#   RETURN prov.rut                                           AS rut,
#          prov.nombre_legal                                  AS nombre,
#          n_ofertas,
#          n_adjudicadas,
#          toFloat(n_adjudicadas) * 100.0 / n_ofertas         AS pct_adjudicadas
#   ORDER BY pct_adjudicadas DESC


# MATCH (org:Organismo)-[:TIENE_UNIDAD]-(:UnidadDeCompra)-[:PUBLICA]-(lic:Licitacion) OPTIONAL MATCH (lic)-[:REQUIERE_ITEM]-(:ItemLicitacion)-[:PARA_ITEM]-(of:Oferta)-[:OFRECE]-(prov:Proveedor)
#   WITH org,
#        count(DISTINCT lic)  AS n_licitaciones,
#        count(DISTINCT of)   AS n_ofertas,
#        count(DISTINCT prov) AS n_proveedores
#   RETURN org.rut            AS rut,
#          org.nombre         AS nombre,
#          n_licitaciones,
#          n_ofertas,
#          n_proveedores
#   ORDER BY n_licitaciones DESC


# MATCH (promedon:Proveedor {rut:'78.566.250-4'})-[:OFRECE]-(of:Oferta)-[:PARA_ITEM]-(:ItemLicitacion)-[:REQUIERE_ITEM]-(:Licitacion)-[:PUBLICA]-(:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
# WITH org,
# count(DISTINCT of) AS n_ofertas,
# sum(CASE WHEN of.adjudicada THEN 1 ELSE 0 END)               AS n_adjudicadas
# WHERE n_ofertas >= 10
# RETURN org.rut                                               AS rut,
#      org.nombre                                              AS nombre,
#      n_ofertas,
#      n_adjudicadas,
#      toFloat(n_adjudicadas) * 100.0 / n_ofertas              AS pct_adjudicacion
# ORDER BY pct_adjudicacion DESC

######################################################################

# Win rate y evolución en el tiempo. Sin esto todo lo demás es ruido.

# MATCH (p:Proveedor {rut:'78.566.250-4'})-[:OFRECE]-(of:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)-[:REQUIERE_ITEM]-(lic:Licitacion)
# WITH lic.year AS anio, lic.month AS mes,
#        count(of) AS n_ofertas,
#        sum(CASE WHEN of.adjudicada THEN 1 ELSE 0 END) AS n_ganadas,
#        sum(CASE WHEN of.adjudicada THEN of.precio_equiv_clp ELSE 0 END) AS revenue_clp
# RETURN anio, mes, n_ofertas, n_ganadas,
#        toFloat(n_ganadas)*100.0/n_ofertas AS win_rate,
#        revenue_clp
# ORDER BY anio, mes




