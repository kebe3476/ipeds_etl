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

def get_sqlalchemy_engine(echo: bool | None = None) -> Engine:
    """
    Return a SQLAlchemy Engine for database work.

    Why this helper exists
    ----------------------
    - Encapsulates access to the module-level `_engine` so other modules don't
      reach into private globals.
    - Allows callers (e.g., ad-hoc notebooks) to temporarily change the SQL
      echo/verbosity **without** mutating the shared engine used by the rest
      of the app.

    Parameters
    ----------
    echo : bool | None
        - None (default): return the shared module-level engine `_engine` as-is.
        - True/False: if this differs from `_engine.echo`, build and return a
          *separate, throwaway* Engine with that echo setting. This does NOT
          replace `_engine`; it’s just for the caller’s immediate use.

    Returns
    -------
    Engine
        Either the shared `_engine` (preferred path) or a short-lived Engine
        configured with the requested `echo` behavior.

    Behavior & Rationale
    --------------------
    - We avoid mutating `_engine.echo` at runtime so other threads/notebooks
      aren’t surprised by suddenly-verbose (or suddenly-quiet) logs.
    - Creating an Engine is relatively cheap, but it still allocates a pool.
      If you request a custom-echo engine, use it in a tight scope (e.g., with
      a context manager) and let it go out of scope so the pool can be GC’d.
    - Both engines use `pool_pre_ping=True` to reduce “stale connection” errors
      after idle periods.

    Example
    -------
    >>> # Normal usage (shared engine):
    >>> eng = get_sqlalchemy_engine()
    >>> with eng.begin() as cx:
    ...     cx.execute(text("SELECT 1"))

    >>> # Ad-hoc verbose SQL logging in a notebook cell:
    >>> eng_loud = get_sqlalchemy_engine(echo=True)
    >>> with eng_loud.begin() as cx:
    ...     cx.execute(text("SELECT 1"))
    """
    # If the caller explicitly asked for a specific echo setting AND it does not
    # match the shared engine's current setting, return a new, temporary Engine
    # with the requested verbosity. We don't overwrite the shared `_engine`.
    if echo is not None and echo != _engine.echo:
        return create_engine(
            settings.DATABASE_URL,   # same connection target
            pool_pre_ping=True,      # same resilience against stale conns
            echo=echo,               # caller-requested verbosity
        )

    # Otherwise: return the shared, long-lived Engine (preferred).
    return _engine


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