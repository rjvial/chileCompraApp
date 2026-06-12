"""Paths and environment for the entity-resolution package."""

import os
from pathlib import Path

from dotenv import load_dotenv

PACKAGE_DIR = Path(__file__).parent
REPO_ROOT = PACKAGE_DIR.parent
DATA_DIR = REPO_ROOT / "data"
REFERENCE_DIR = DATA_DIR / "reference"  # FX/UF/CPI caches (refetchable, gitignored)

load_dotenv(REPO_ROOT / "secrets.env")


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)
