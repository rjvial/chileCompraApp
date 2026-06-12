"""Currency and inflation reference data (design note §6).

Immutable originals, derived conversion: keep original currency/amount;
derive unit_price_clp_norm at the tender-date official rate. Long horizons in
UF and CPI-deflated — nominal and real both first-class, never silently
mixed.

Source: mindicador.cl (no-auth JSON API over Banco Central / INE series),
cached per indicator-year under data/reference/ so conversion is a
deterministic, offline recompute. The official Banco Central SieteRestWS API
(requires credentials) can swap in behind the same functions.

Note: the "ipc" series is the MONTHLY VARIATION in percent, not an index —
cpi_index() builds a chained index from it for deflation.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from ..config import REFERENCE_DIR

INDICATORS = {"uf", "dolar", "euro", "ipc"}
_LOOKBACK_DAYS = 10  # daily series skip weekends/holidays


def _cache_path(indicator: str, year: int) -> Path:
    return REFERENCE_DIR / f"{indicator}_{year}.json"


def fetch_year(indicator: str, year: int, force: bool = False) -> dict[str, float]:
    """Return {iso_date: value} for one indicator-year, fetching and caching
    on first use."""
    if indicator not in INDICATORS:
        raise ValueError(f"unknown indicator {indicator!r}; expected one of {sorted(INDICATORS)}")
    path = _cache_path(indicator, year)
    if path.exists() and not force:
        return json.loads(path.read_text(encoding="utf-8"))

    import requests  # imported lazily so offline use of cached data needs no network stack

    resp = requests.get(f"https://mindicador.cl/api/{indicator}/{year}", timeout=30)
    resp.raise_for_status()
    serie = resp.json().get("serie", [])
    data = {item["fecha"][:10]: float(item["valor"]) for item in serie}
    if not data:
        raise RuntimeError(f"mindicador returned no data for {indicator}/{year}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=1), encoding="utf-8")
    return data


def get_value(indicator: str, d: date) -> float:
    """Value of a daily indicator on d, falling back to the most recent prior
    business day."""
    data = fetch_year(indicator, d.year)
    probe = d
    for _ in range(_LOOKBACK_DAYS):
        v = data.get(probe.isoformat())
        if v is not None:
            return v
        probe -= timedelta(days=1)
        if probe.year != d.year:
            data = fetch_year(indicator, probe.year)
    raise LookupError(f"no {indicator} value within {_LOOKBACK_DAYS} days before {d}")


def to_clp(amount: float, currency: str, d: date) -> float:
    """Convert an original amount to nominal CLP at the tender-date rate."""
    currency = currency.upper()
    if currency == "CLP":
        return amount
    if currency in ("UF", "CLF"):
        return amount * get_value("uf", d)
    if currency == "USD":
        return amount * get_value("dolar", d)
    if currency == "EUR":
        return amount * get_value("euro", d)
    raise ValueError(f"unsupported currency {currency!r}")


def cpi_index(year: int, month: int, base: tuple[int, int] = (2020, 1)) -> float:
    """Chained CPI index (base month = 100) from monthly variations."""
    if (year, month) < base:
        raise ValueError("month precedes the index base; lower `base`")
    index = 100.0
    y, m = base
    while (y, m) < (year, month):
        m += 1
        if m == 13:
            y, m = y + 1, 1
        variations = fetch_year("ipc", y)
        monthly = {k[:7]: v for k, v in variations.items()}
        var = monthly.get(f"{y:04d}-{m:02d}")
        if var is None:
            raise LookupError(f"no CPI variation for {y:04d}-{m:02d}")
        index *= 1 + var / 100.0
    return index


def deflate(amount_clp: float, from_month: tuple[int, int], to_month: tuple[int, int]) -> float:
    """Express a nominal CLP amount of from_month in to_month pesos."""
    return amount_clp * cpi_index(*to_month) / cpi_index(*from_month)
