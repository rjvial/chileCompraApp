import pandas as pd
import boto3
import json
import time
import funcionesAws as fa

fileDir = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"
INPUT_FILE = "num_proveedores_por_producto.csv"
OUTPUT_FILE = "productos_estructurados.json"

BATCH_SIZE = 30
MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

# ─── READ INPUT ───────────────────────────────────────────────────
df = pd.read_csv(f"{fileDir}{INPUT_FILE}", sep="|")
print(f"Total productos: {len(df)}")

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
Para cada nombre de producto, extrae información estructurada en formato JSON.

Reglas:
- canonical_product: nombre genérico normalizado del producto (sin marca, sin variante, sin formato).
- brand: marca si se menciona, sino null.
- variant: variante o especificación (color, sabor, modelo, talla, tamaño, dimensiones, etc.), sino null.
- format: presentación (caja, bolsa, botella, rollo, etc.), sino null.
- quantity: cantidad numérica si se menciona, sino null.
- unit: unidad de medida (kg, litros, unidades, metros, etc.), sino null.
- location: si se menciona una región, ciudad o comuna de Chile, normaliza al nombre oficial de la región. Usa null si no se menciona. Las regiones válidas son:
""" + REGIONES_LIST + """
- extra_attributes: lista de atributos adicionales relevantes que no encajan en los campos anteriores. Lista vacía si no hay.

Responde SOLO con un JSON array, sin texto adicional. Ejemplo:

Input:
1. RESMA PAPEL FOTOCOPIA CARTA 500 HOJAS
2. TONER HP 85A NEGRO
3. GUANTE NITRILO TALLA L REGIÓN DE VALPARAÍSO

Output:
[
  {"canonical_product": "papel fotocopia", "brand": null, "variant": "carta", "format": "resma", "quantity": 500, "unit": "hojas", "location": null, "extra_attributes": []},
  {"canonical_product": "toner", "brand": "HP", "variant": "85A negro", "format": null, "quantity": null, "unit": null, "location": null, "extra_attributes": []},
  {"canonical_product": "guante nitrilo", "brand": null, "variant": "talla L", "format": null, "quantity": null, "unit": null, "location": "Región de Valparaíso", "extra_attributes": []}
]"""


def build_prompt(nombres):
    items = "\n".join(f"{i+1}. {n}" for i, n in enumerate(nombres))
    return f"{SYSTEM_PROMPT}\n\nInput:\n{items}\n\nOutput:"


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


SUMMARY_FIELDS = ["canonical_product", "brand", "variant", "format",
                   "quantity", "unit", "location", "extra_attributes"]

def build_summary(item):
    parts = []
    for field in SUMMARY_FIELDS:
        val = item.get(field)
        if val is not None and val != []:
            parts.append(f"{field}: {val}")
    return " | ".join(parts)


# ─── PROCESS BATCHES ──────────────────────────────────────────────
TEST_MODE = True
results = []
total_batches = 1 if TEST_MODE else (len(df) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_idx in range(total_batches):
    start = batch_idx * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(df))
    batch = df.iloc[start:end]
    nombres = batch["nombre_producto"].tolist()
    ids = batch["producto_detallado_id"].tolist()

    print(f"Batch {batch_idx+1}/{total_batches} ({start+1}-{end})...", end=" ")

    prompt = build_prompt(nombres)

    try:
        response_text = fa.invoke_bedrock(bedrock, prompt, model_id=MODEL_ID)
        parsed = parse_response(response_text, len(nombres))

        for i, item in enumerate(parsed):
            item["producto_detallado_id"] = ids[i]
            item["nombre_producto_original"] = nombres[i]
            item["summary"] = build_summary(item)

        results.extend(parsed)
        print("OK")

    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        for i, nombre in enumerate(nombres):
            results.append({
                "producto_detallado_id": ids[i],
                "nombre_producto_original": nombre,
                "canonical_product": None,
                "brand": None,
                "variant": None,
                "format": None,
                "quantity": None,
                "unit": None,
                "location": None,
                "extra_attributes": [],
                "error": str(e),
            })

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)

    time.sleep(1)

# # ─── GENERATE EMBEDDINGS ─────────────────────────────────────────
# print(f"\nGenerating embeddings for {len(results)} products...")
# for i, item in enumerate(results):
#     summary = item.get("summary", "")
#     if summary:
#         item["embedding"] = fa.invoke_titan_embedding(bedrock, summary)
#     else:
#         item["embedding"] = None
#     if (i + 1) % 50 == 0:
#         print(f"  {i+1}/{len(results)} embeddings done")

# print(f"  {len(results)}/{len(results)} embeddings done")

# ─── SAVE OUTPUT ──────────────────────────────────────────────────
with open(f"{fileDir}{OUTPUT_FILE}", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(results)} products to {fileDir}{OUTPUT_FILE}")
