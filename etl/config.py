"""
etl/config.py
-------------
Centralized configuration loader for the IPEDS ETL project.

Why this exists:
- Keeps all knobs (DB URL, API base, timeouts, logging flags) in one place.
- Reads from `config/.env` so secrets never live in code or Git.
- Provides a tiny API (`settings`, `dump_settings()`) that the rest of the
  codebase can import without worrying where values come from.

This file intentionally does NOT print the database URL anywhere to avoid
leaking credentials in logs or notebooks.
"""

from __future__ import annotations

import os
import os.path
from dataclasses import dataclass
from typing import Callable, Optional

# `python-dotenv` lets us load environment variables from a file.
# Here we load `../config/.env` relative to THIS file's location so that the
# project works no matter where you launch Python from (VS Code, Jupyter, CLI).
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Locate and load the .env file
# -----------------------------------------------------------------------------
# Example tree:
#   ipeds_etl/
#     etl/           <- this file lives here
#     config/.env    <- we want to load this file
#
# We compute the path dynamically instead of assuming the working directory.
_ETL_DIR = os.path.dirname(__file__)                       # .../ipeds_etl/etl
_PROJECT_ROOT = os.path.abspath(os.path.join(_ETL_DIR, ".."))     # .../ipeds_etl
_ENV_PATH = os.path.join(_PROJECT_ROOT, "config", ".env")  # .../ipeds_etl/config/.env

# `load_dotenv` is a no-op if the file is missing, which is convenient for CI or
# environments where values are injected (e.g., GitHub Actions secrets).
load_dotenv(_ENV_PATH)


# -----------------------------------------------------------------------------
# Tiny helpers for reading and casting environment variables
# -----------------------------------------------------------------------------
def _cast_bool(value: str | None) -> bool:
    """
    Convert common truthy strings to a real bool.
    Accepts: "1", "true", "yes", "y", "on" (case-insensitive).
    Anything else -> False.
    """
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _get(
    name: str,
    default: Optional[str] = None,
    cast: Optional[Callable[[str], object]] = None,
    required: bool = False,
):
    """
    Fetch an environment variable by NAME with optional casting and requirements.

    Parameters
    ----------
    name : str
        The environment variable to read (e.g., "DATABASE_URL").
    default : Optional[str]
        Fallback value if the variable is missing or empty.
    cast : Optional[Callable[[str], object]]
        Function to convert the raw string into a type (int/float/bool/etc).
        Example: `_get("RATE_LIMIT_RPS", "4", float)` -> 4.0
    required : bool
        If True and the variable is empty/undefined and no default is provided,
        a RuntimeError is raised to fail fast.

    Returns
    -------
    Any
        The casted/env value, or the default, or None.
    """
    raw = os.getenv(name)

    # Treat empty strings as "not set" (common when someone types NAME= in .env)
    is_empty = raw is None or raw == ""

    if is_empty:
        if required and default is None:
            # Fail early with a clear error message so setup problems are obvious.
            raise RuntimeError(f"Missing required environment variable: {name}")
        raw = default

    # If no casting function is provided, just return the string (or None).
    if cast is None or raw is None:
        return raw

    # Allow cast to raise if the value is malformed (good signal during setup).
    return cast(raw)


# -----------------------------------------------------------------------------
# Frozen dataclass of all settings (immutable, importable anywhere)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    # --- Database ---
    # Use the ipeds_loader role for write-capable ETL connections.
    # Marked required=True because nothing works without a DB.
    DATABASE_URL: str = _get("DATABASE_URL", required=True)

    # --- Urban Institute API base URL ---
    # Default points to the public College/University v1 path.
    URBAN_BASE_URL: str = _get(
        "URBAN_BASE_URL",
        "https://educationdata.urban.org/api/v1/college-university",
    )

    # --- HTTP behavior ---
    REQUEST_TIMEOUT_SECONDS: int = _get("REQUEST_TIMEOUT_SECONDS", "30", int)
    RATE_LIMIT_RPS: float = _get("RATE_LIMIT_RPS", "4", float)
    MAX_RETRIES: int = _get("MAX_RETRIES", "3", int)
    USER_AGENT: str = _get("USER_AGENT", "ipeds-etl/0.1 (no-contact)")

    # --- Logging toggles ---
    # LOG_SQL controls SQLAlchemy's echo flag (very verbose if True).
    LOG_SQL: bool = _get("LOG_SQL", "false", _cast_bool)  # default False
    LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")            # "DEBUG", "INFO", etc.


# Instantiate a single, immutable settings object for the whole process.
settings = Settings()


def dump_settings() -> dict:
    """
    Return a *safe* snapshot of configuration for debugging/printing in notebooks.
    Intentionally hides DATABASE_URL to avoid leaking credentials into logs.
    """
    visible = {
        "URBAN_BASE_URL": settings.URBAN_BASE_URL,
        "REQUEST_TIMEOUT_SECONDS": settings.REQUEST_TIMEOUT_SECONDS,
        "RATE_LIMIT_RPS": settings.RATE_LIMIT_RPS,
        "MAX_RETRIES": settings.MAX_RETRIES,
        "USER_AGENT": settings.USER_AGENT,
        "LOG_SQL": settings.LOG_SQL,
        "LOG_LEVEL": settings.LOG_LEVEL,
        "ENV_PATH": _ENV_PATH,  # helpful to confirm which file was read
    }
    return visible