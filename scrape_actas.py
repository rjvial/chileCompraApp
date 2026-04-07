from playwright.sync_api import sync_playwright
import os
import re
import json
import time
import pandas as pd
import boto3
from botocore.config import Config
from dotenv import load_dotenv
import funcionesAws as fa


def scrape_acta(url, page):
    """Scrape all adjudication data from a PreviewAwardAct page."""
    page.goto(url, timeout=60000)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)

    data = page.evaluate("""() => {
        const text = document.body.innerText;

        // Get ALL tables with their rows and cells
        const tables = [];
        document.querySelectorAll('table').forEach((table, idx) => {
            const rows = [];
            table.querySelectorAll('tr').forEach(tr => {
                const cells = [];
                tr.querySelectorAll('td, th').forEach(td => {
                    cells.push(td.innerText.trim());
                });
                if (cells.length > 0 && cells.some(c => c.length > 0)) {
                    rows.push(cells);
                }
            });
            if (rows.length > 0) {
                tables.push({index: idx, rowCount: rows.length, colCount: rows[0] ? rows[0].length : 0, rows});
            }
        });

        // Get all spans/labels with IDs (these hold specific field values)
        const fields = {};
        document.querySelectorAll('[id]').forEach(el => {
            const id = el.id;
            const val = el.innerText.trim();
            if (val && val.length < 500 && val.length > 0) {
                fields[id] = val;
            }
        });

        // Get attachment links
        const attachments = [];
        document.querySelectorAll('a').forEach(a => {
            const t = a.innerText.trim();
            const id = a.id || '';
            if (t && (t.includes('.pdf') || t.includes('.PDF') || t.includes('.doc') ||
                       t.includes('.xls') || t.includes('.zip') || id.toLowerCase().includes('dwnl'))) {
                attachments.push({text: t, id, href: a.href || ''});
            }
        });

        return {text, tables, fields, attachments};
    }""")

    return data


def parse_key_value_row(cells):
    """Parse a table row with alternating key-value cells into a dict."""
    result = {}
    i = 0
    while i < len(cells) - 1:
        key = cells[i].strip()
        val = cells[i + 1].strip()
        if key and val and not key.startswith("\t") and len(key) < 100:
            result[key] = val
            i += 2
        else:
            i += 1
    return result


def parse_acta(raw_data):
    """Parse raw data into a clean structured format."""
    text = raw_data["text"]
    tables = raw_data["tables"]

    # Find the main data table (largest one)
    main_table = None
    max_rows = 0
    for t in tables:
        if t["rowCount"] > max_rows:
            max_rows = t["rowCount"]
            main_table = t

    if not main_table:
        return {"texto_completo": text}

    rows = main_table["rows"]

    # --- 1. Organismo (row 0: key-value pairs) ---
    organismo = {}
    for row in rows:
        kv = parse_key_value_row(row[1:] if len(row) > 1 else row)
        if "Razón Social" in kv or "R.U.T." in kv:
            organismo = {
                "razon_social": kv.get("Razón Social", ""),
                "unidad_compra": kv.get("Unidad de Compra", ""),
                "rut": kv.get("R.U.T.", ""),
                "direccion": kv.get("Dirección", ""),
                "comuna": kv.get("Comuna", ""),
                "region": kv.get("Ciudad en que se genera la Adquisición", ""),
            }
            break

    # --- 2. Contacto (row 1) ---
    contacto = {}
    for row in rows:
        kv = parse_key_value_row(row[1:] if len(row) > 1 else row)
        if "Nombre Completo" in kv or "Cargo" in kv:
            contacto = {
                "nombre": kv.get("Nombre Completo", ""),
                "cargo": kv.get("Cargo", ""),
                "telefono": kv.get("Teléfono", ""),
                "email": kv.get("E-Mail", ""),
            }
            break

    # --- 3. Adquisicion details (row 2) ---
    adquisicion = {}
    for row in rows:
        kv = parse_key_value_row(row[1:] if len(row) > 1 else row)
        if "Número de Adquisición" in kv or "Nombre de Adquisición" in kv:
            adquisicion = {
                "numero": kv.get("Número de Adquisición", ""),
                "nombre": kv.get("Nombre de Adquisición", ""),
                "tipo": kv.get("Tipo de Adquisición", ""),
                "descripcion": kv.get("Descripción", ""),
                "tipo_convocatoria": kv.get("Tipo de Convocatoria", ""),
                "moneda": kv.get("Moneda", ""),
                "fecha_publicacion": kv.get("Fecha de Publicación", ""),
                "fecha_cierre": kv.get("Fecha de Cierre", ""),
                "tipo_adjudicacion": kv.get("Tipo de Adjudicación", ""),
                "monto_neto_adjudicado": kv.get("Monto Neto Adjudicado", ""),
                "monto_neto_estimado": kv.get("Monto Neto Estimado del Contrato", ""),
            }
            break

    # --- 4. Anexos (attachments rows) ---
    anexos = []
    for row in rows:
        cells = row[1:] if len(row) > 1 else row
        # Attachment rows have: filename.pdf, type, description, size, date
        if len(cells) >= 5:
            filename = cells[0].strip() if cells[0] else ""
            if filename and ("." in filename) and any(ext in filename.lower() for ext in ['.pdf', '.doc', '.xls', '.zip']):
                anexos.append({
                    "archivo": filename,
                    "tipo": cells[1].strip() if len(cells) > 1 else "",
                    "descripcion": cells[2].strip() if len(cells) > 2 else "",
                    "tamano": cells[3].strip() if len(cells) > 3 else "",
                    "fecha": cells[4].strip() if len(cells) > 4 else "",
                })

    # --- 5. Comision evaluadora ---
    comision = []
    for row in rows:
        kv = parse_key_value_row(row[1:] if len(row) > 1 else row)
        if "Rut" in kv and "Nombre" in kv:
            # This row has headers, parse the following cells as groups of 3
            cells = row[1:] if len(row) > 1 else row
            # Skip header cells (Rut, Nombre, Cargo)
            i = 0
            while i < len(cells):
                if cells[i] in ("Rut", "Nombre", "Cargo"):
                    i += 1
                    continue
                # Check if this looks like a RUT
                if re.match(r"[\d.]+-[\dkK]", cells[i]):
                    miembro = {"rut": cells[i]}
                    if i + 1 < len(cells):
                        miembro["nombre"] = cells[i + 1]
                    if i + 2 < len(cells):
                        miembro["cargo"] = cells[i + 2]
                    comision.append(miembro)
                    i += 3
                else:
                    i += 1
            break

    # --- 6. Items / Lines ---
    items = []
    for row in rows:
        cells = row
        # Item rows have 2000+ cells, start with line number
        if len(cells) > 20:
            # Find line items: look for sequential cell patterns
            # Cells structure: [merged], [empty], [merged], [merged], [empty],
            #   [line_num], [clasificacion], [producto_generico], [spec_label], [spec_value],
            #   [cantidad], [header_row...], [proveedor entries...]
            i = 5  # Start after header cells
            current_item = None
            while i < len(cells):
                cell = cells[i].strip()

                # Line number (1, 2, 3...)
                if cell.isdigit() and int(cell) <= 200 and i + 4 < len(cells):
                    next_cell = cells[i + 1].strip() if i + 1 < len(cells) else ""
                    if "Clasificación ONU" in next_cell or "Clasificaci" in next_cell:
                        if current_item:
                            items.append(current_item)
                        current_item = {
                            "linea": int(cell),
                            "clasificacion_onu": next_cell,
                            "producto_generico": cells[i + 2].strip() if i + 2 < len(cells) else "",
                            "especificaciones_comprador": "",
                            "cantidad": "",
                            "ofertas": [],
                        }
                        i += 3
                        continue

                # Buyer specs
                if current_item and ("Especificaciones del Comprador" in cell):
                    if i + 1 < len(cells):
                        current_item["especificaciones_comprador"] = cells[i + 1].strip()
                    i += 2
                    continue

                # Quantity
                if current_item and cell.startswith("Cantidad"):
                    m = re.search(r"(\d+)", cell)
                    if m:
                        current_item["cantidad"] = int(m.group(1))
                    i += 1
                    continue

                # Supplier offer row: RUT + name, then specs, price, qty, total, status
                if current_item and re.match(r"[\d.]+-[\dkK]", cell):
                    oferta = {
                        "rut_proveedor": "",
                        "nombre_proveedor": "",
                        "especificaciones_proveedor": "",
                        "monto_unitario": "",
                        "cantidad_adjudicada": "",
                        "total_neto": "",
                        "estado": "",
                    }
                    # RUT and name are in same cell
                    parts = cell.split(" ", 1)
                    if len(parts) >= 1:
                        oferta["rut_proveedor"] = parts[0]
                    if len(parts) >= 2:
                        oferta["nombre_proveedor"] = parts[1].strip()

                    if i + 1 < len(cells):
                        oferta["especificaciones_proveedor"] = cells[i + 1].strip()[:200]
                    if i + 2 < len(cells):
                        oferta["monto_unitario"] = cells[i + 2].strip()
                    if i + 3 < len(cells):
                        oferta["cantidad_adjudicada"] = cells[i + 3].strip()
                    if i + 4 < len(cells):
                        oferta["total_neto"] = cells[i + 4].strip()
                    if i + 5 < len(cells):
                        oferta["estado"] = cells[i + 5].strip()

                    current_item["ofertas"].append(oferta)
                    i += 6
                    continue

                i += 1

            if current_item:
                items.append(current_item)

    # --- 7. Decreto info from header ---
    decreto = {}
    m = re.search(r"Nro de Decreto\s+(\d+)", text)
    if m:
        decreto["numero"] = m.group(1)
    m = re.search(r"En\s+(.+?),\s+(\d{2}-\d{2}-\d{4})", text)
    if m:
        decreto["lugar"] = m.group(1)
        decreto["fecha"] = m.group(2)

    # --- 8. Fecha adjudicacion from header ---
    fecha_informada = ""
    m = re.search(r"Adjudicaci[oó]n Informada en portal el\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})", text)
    if m:
        fecha_informada = m.group(1)

    # --- 9. Resolution text ---
    texto_resolucion = ""
    m = re.search(r"((?:VISTOS|Vistos)[\s\S]*?(?:Arch[ií]vese|arch[ií]vese|An[oó]tese|an[oó]tese)[.\s]*)", text)
    if m:
        texto_resolucion = m.group(1).strip()

    # --- Build final structured result ---
    result = {
        "organismo": organismo,
        "contacto": contacto,
        "adquisicion": adquisicion,
        "decreto": decreto,
        "fecha_adjudicacion_informada": fecha_informada,
        "comision_evaluadora": comision,
        "anexos": anexos,
        "items": items,
        "texto_resolucion": texto_resolucion,
    }

    return result


print("=== MercadoPublico Acta Scraper ===\n")

# Load URLs from CSV
df = pd.read_csv("preview_award_urls.csv")
df = df[df["status"] == "ok"]
print(f"Loaded {len(df)} URLs from preview_award_urls.csv\n")

os.makedirs("data", exist_ok=True)
all_results = []

pw = sync_playwright().start()
browser = pw.chromium.launch(headless=False)
context = browser.new_context(accept_downloads=True)
page = context.new_page()

try:
    for i, row in df.iterrows():
        url = row["preview_award_url"]
        id_licitacion = row["IDLicitacion"]
        print(f"\n[{len(all_results)+1}/{len(df)}] {id_licitacion}: {url[:80]}...")
        try:
            raw = scrape_acta(url, page)
            parsed = parse_acta(raw)
            parsed["id_licitacion"] = id_licitacion
            parsed["url"] = url
            all_results.append(parsed)

            adq = parsed.get("adquisicion", {})
            org = parsed.get("organismo", {})
            dec = parsed.get("decreto", {})
            print(f"  Code: {adq.get('numero', 'N/A')}")
            print(f"  Name: {adq.get('nombre', 'N/A')}")
            print(f"  Org: {org.get('razon_social', 'N/A')} ({org.get('rut', '')})")
            print(f"  Decree: {dec.get('numero', 'N/A')} ({dec.get('fecha', '')})")
            print(f"  Type: {adq.get('tipo', 'N/A')}")
            print(f"  Amount: {adq.get('monto_neto_adjudicado', 'N/A')}")
            items_list = parsed.get("items", [])
            print(f"  Items: {len(items_list)} line items")
            comision = parsed.get("comision_evaluadora", [])
            print(f"  Evaluators: {len(comision)}")
            anexos = parsed.get("anexos", [])
            print(f"  Attachments: {len(anexos)}")
            for a in anexos:
                print(f"    - {a['archivo']} ({a['tamano']})")

        except Exception as e:
            print(f"  ERROR: {e}")
            all_results.append({"id_licitacion": id_licitacion, "url": url, "error": str(e)})

        time.sleep(1)
finally:
    browser.close()
    pw.stop()

# Save all results locally
output_file = os.path.join("data", "actas.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(all_results, f, ensure_ascii=False, indent=2)
print(f"\nSaved {len(all_results)} results to {output_file}")

# Upload to S3
load_dotenv(".env")
s3_config = Config(
    connect_timeout=10,
    read_timeout=60,
    retries={"max_attempts": 3},
    tcp_keepalive=True,
)
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=os.getenv("AWS_REGION"),
    config=s3_config,
)

S3_BUCKET = "tender-engines"
S3_KEY = "raw-data/actas/actas.json"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{S3_KEY} ===")
fa.upload_file(s3_client, output_file, S3_BUCKET, S3_KEY)

print(f"\n=== Done. {len(all_results)} actas scraped and uploaded ===")
