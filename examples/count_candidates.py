"""Count corpus presence for next-10 category candidates (read-only)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection

CANDIDATES = {
    "canulas": "(?i).*canula.*",
    "mascarillas": "(?i).*(mascarilla|barbijo).*",
    "nariceras": "(?i).*naricera.*",
    "electrodos": "(?i).*electrodo.*",
    "cateteres_venosos": "(?i).*(branula|bránula|cateter venoso|catéter venoso|cateter iv).*",
    "tubos_endotraqueales": "(?i).*endotraqueal.*",
    "filtros": "(?i).*filtro.*",
    "bolsas_recolectoras": "(?i).*bolsa.*(recolect|orina|colostom|drenaje).*",
    "fresas_dentales": "(?i).*fresa.*",
    "limas_endodonticas": "(?i).*lima[s ].*",
    "guantes_quirurgicos": "(?i).*guante.*quirurgic.*",
    "esterilizacion": "(?i).*esterilizacion.*",
    "apositos_transparentes": "(?i).*tegaderm|aposito transparente.*",
    "drenajes": "(?i).*drenaje.*",
    "guias_puncion": "(?i).*(guia de puncion|trocar).*",
}

conn = get_connection()
try:
    for cid, rx in CANDIDATES.items():
        rec = conn.query(
            """
            MATCH (i:ItemLicitacion)
            WHERE i.descripcion_comprador IS NOT NULL
              AND i.descripcion_comprador =~ $rx
            RETURN count(DISTINCT i.descripcion_comprador) AS c
            """,
            parameters={"rx": rx},
        )
        print(f"{cid:<26}{rec[0]['c']:>6} distinct descriptions")
finally:
    conn.close()
