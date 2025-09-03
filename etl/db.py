"""
etl/db.py
---------
Thin wrapper around SQLAlchemy for:
- creating a connection engine using project settings,
- quick health checks (`ping()`),
- convenience queries (e.g., listing ipeds_* schemas).

Design goals:
- Keep this module tiny and dependency-free (besides SQLAlchemy).
- Centralize engine construction so pool settings are consistent.
- Make it trivial to import in notebooks: `from etl.db import ping, list_ipeds_schemas`
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl.config import settings  # pulls values from config/.env via etl/config.py


# -----------------------------------------------------------------------------
# Engine creation
# -----------------------------------------------------------------------------
# Notes:
# - pool_pre_ping=True: before reusing a pooled connection, SQLAlchemy issues a
#   lightweight "SELECT 1" to ensure the connection is still alive. This avoids
#   "My connection died overnight" surprises in long-running notebooks.
# - echo=settings.LOG_SQL: toggles extremely verbose SQL logging (debugging only).
_engine: Engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    echo=settings.LOG_SQL,
)

# Why module-level engine?
# - It's cheap to construct once and reuse.
# - If you ever need different roles (e.g., reader vs loader), you can add a
#   second factory function later without breaking callers.


# -----------------------------------------------------------------------------
# Simple health checks / utilities
# -----------------------------------------------------------------------------
def ping() -> tuple[str, str]:
    """
    Return (current_database, current_user) as a quick sanity check that:
    - the engine can connect,
    - the credentials are what you expect (usually 'ipeds_db' / 'ipeds_loader').
    """
    with _engine.connect() as cx:
        # `text()` wraps a literal SQL string for SQLAlchemy execution.
        row = cx.execute(text("select current_database(), current_user")).one()
        return row[0], row[1]


def list_ipeds_schemas() -> list[str]:
    """
    List the IPEDS-related schemas (plus 'public') in alphabetical order.
    Helpful for confirming that 00_schemas.sql and 15_meta.sql were applied.
    """
    sql = """
    select schema_name
    from information_schema.schemata
    where schema_name like 'ipeds_%' or schema_name = 'public'
    order by schema_name
    """
    with _engine.connect() as cx:
        # `all()` returns a list of Row objects; [r[0] for r in ...] pulls the string.
        return [r[0] for r in cx.execute(text(sql)).all()]


# -----------------------------------------------------------------------------
# Example usage (for your notebook 00_env_check.ipynb):
# -----------------------------------------------------------------------------
# from etl.db import ping, list_ipeds_schemas
# print("Ping:", ping())                 # -> ('ipeds_db', 'ipeds_loader')
# print("Schemas:", list_ipeds_schemas())# -> ['ipeds_core', 'ipeds_dim', ...]
#
# If you need to run arbitrary SQL in a notebook:
# with _engine.begin() as cx:
#     cx.execute(text("select 1"))
#
# Use `.begin()` for transactional blocks that autocommit/rollback on exit.