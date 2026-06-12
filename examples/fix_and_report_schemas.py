"""One-off: fix the name field of generated schemas and report illegal
extractions per category over fresh corpus samples (read-only)."""

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.categories.schema import SCHEMAS_DIR, load_schema
from chilecompra_er.graphdb import get_connection
from chilecompra_er.normalize import Normalizer
from chilecompra_er.resolve.extractor import extract
from generate_schemas import FAMILIES, fetch_samples

NAMES = {f["category_id"]: f["name"] for f in FAMILIES}

norm = Normalizer()
conn = get_connection()
try:
    for fam in FAMILIES:
        path = SCHEMAS_DIR / f"{fam['category_id']}.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        if doc["name"] != fam["name"]:
            doc["name"] = fam["name"]
            path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

        schema = load_schema(path)
        samples = [norm(t) for t in fetch_samples(conn, fam["corpus_regex"], 50)]
        illegal: Counter = Counter()
        for text in samples:
            for item in extract(text, schema).illegal:
                illegal[(item["attribute"], item["value"])] += 1
        if illegal:
            print(f"== {fam['category_id']}")
            for (attr, value), count in illegal.most_common(12):
                print(f"   {attr} = {value!r} x{count}")
finally:
    conn.close()
