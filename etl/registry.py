"""
etl/registry.py
---------------
A tiny in-code *catalog* (single source of truth) describing each endpoint the
ETL knows about. The registry itself does not execute any SQL; instead:

- `core_io._ensure_core_table()` reads `schema` and `primary_key` here to build
  the `CREATE TABLE IF NOT EXISTS ipeds_core.{endpoint}` DDL (so yes—this is
  where column definitions ultimately come from).
- `core_io.load_core_from_raw()` looks up the endpoint’s `mapper` here to turn
  raw JSON records into typed dicts that match the schema.
- The optional `path` is the API path template (handy for reference/reuse).

Add new endpoints by inserting another entry alongside "directory" with:
  - path        : string template (usually ".../{year}/")
  - schema      : { column_name: "SQL TYPE" }
  - primary_key : list[str] used for UPSERT conflict target
  - mapper      : callable(raw_record) -> dict aligned to `schema`
"""

from __future__ import annotations

from typing import Dict, Any
from etl.mappers.directory import map_directory_row  # raw JSON -> typed row


# Type alias for clarity when reading code elsewhere. Each endpoint config is
# a small dict with "path", "schema", "primary_key", and "mapper" keys.
EndpointCfg = Dict[str, Any]


# -----------------------------------------------------------------------------
# Registry of endpoints
# -----------------------------------------------------------------------------
# Keys of REGISTRY are short endpoint names used throughout the project
# (e.g., "directory", "admissions", "completions"). Values are configs.
REGISTRY: Dict[str, EndpointCfg] = {
    "directory": {
        # API path template (year is a path segment for Urban’s IPEDS endpoints).
        # We keep this here even if the current loader pulls from ipeds_raw only;
        # it’s useful for documentation and future runners that call the API.
        "path": "ipeds/directory/{year}/",

        # Core table layout for ipeds_core.directory.
        # IMPORTANT:
        # - Column names here are the contract for the mapper output.
        # - SQL types are Postgres types (TEXT, INTEGER, DOUBLE PRECISION, ...).
        # - To add a new column later: add it here AND map it in the mapper.
        "schema": {
            # --- PRIMARY KEY (one row per institution-year) -------------------
            "unitid": "INTEGER NOT NULL",
            "year": "INTEGER NOT NULL",

            # --- Identity / contact info -------------------------------------
            "opeid": "TEXT",
            "inst_name": "TEXT",
            "inst_alias": "TEXT",
            "address": "TEXT",
            "city": "TEXT",
            "state_abbr": "TEXT",
            "zip": "TEXT",
            "phone_number": "TEXT",
            "url_school": "TEXT",
            "url_fin_aid": "TEXT",
            "url_application": "TEXT",
            "url_netprice": "TEXT",
            "url_veterans": "TEXT",
            "url_athletes": "TEXT",
            "url_disability_services": "TEXT",
            "ein": "TEXT",
            "duns": "TEXT",
            "ueis": "TEXT",
            "chief_admin_name": "TEXT",
            "chief_admin_title": "TEXT",
            "inst_system_name": "TEXT",

            # --- Geography ----------------------------------------------------
            "fips": "INTEGER",
            "county_name": "TEXT",
            "county_fips": "INTEGER",
            "region": "INTEGER",
            "urban_centric_locale": "INTEGER",
            "cbsa": "INTEGER",
            "cbsa_type": "INTEGER",
            "csa": "INTEGER",
            "necta": "INTEGER",
            "congress_district_id": "INTEGER",
            "latitude": "DOUBLE PRECISION",
            "longitude": "DOUBLE PRECISION",

            # --- Status / attributes -----------------------------------------
            "inst_status": "INTEGER",
            "sector": "INTEGER",
            "inst_control": "INTEGER",
            "institution_level": "INTEGER",
            "inst_category": "INTEGER",
            "inst_size": "INTEGER",
            "degree_granting": "INTEGER",
            "title_iv_indicator": "INTEGER",
            "hbcu": "INTEGER",
            "tribal_college": "INTEGER",
            "land_grant": "INTEGER",
            "hospital": "INTEGER",
            "medical_degree": "INTEGER",
            "open_public": "INTEGER",
            "currently_active_ipeds": "INTEGER",
            "postsec_public_active": "INTEGER",
            "postsec_public_active_title_iv": "INTEGER",
            "primarily_postsecondary": "INTEGER",
            "offering_highest_degree": "INTEGER",
            "offering_highest_level": "INTEGER",
            "offering_undergrad": "INTEGER",
            "offering_grad": "INTEGER",
            "reporting_method": "INTEGER",
            "inst_system_flag": "INTEGER",
            "comparison_group": "INTEGER",
            "comparison_group_custom": "INTEGER",

            # --- Mergers / deletions / dates ---------------------------------
            # `date_closed` often appears as text; we keep TEXT for simplicity.
            "newid": "INTEGER",
            "date_closed": "TEXT",
            "year_deleted": "INTEGER",

            # --- Carnegie classifications ------------------------------------
            "cc_basic_2000": "INTEGER",
            "cc_basic_2010": "INTEGER",
            "cc_basic_2015": "INTEGER",
            "cc_basic_2018": "INTEGER",
            "cc_basic_2021": "INTEGER",

            "cc_instruc_undergrad_2010": "INTEGER",
            "cc_instruc_undergrad_2015": "INTEGER",
            "cc_instruc_undergrad_2018": "INTEGER",
            "cc_instruc_undergrad_2021": "INTEGER",

            "cc_instruc_grad_2010": "INTEGER",
            "cc_instruc_grad_2015": "INTEGER",
            "cc_instruc_grad_2018": "INTEGER",
            "cc_instruc_grad_2021": "INTEGER",

            "cc_undergrad_2010": "INTEGER",
            "cc_undergrad_2015": "INTEGER",
            "cc_undergrad_2018": "INTEGER",
            "cc_undergrad_2021": "INTEGER",

            "cc_enroll_2010": "INTEGER",
            "cc_enroll_2015": "INTEGER",
            "cc_enroll_2018": "INTEGER",
            "cc_enroll_2021": "INTEGER",

            "cc_size_setting_2010": "INTEGER",
            "cc_size_setting_2015": "INTEGER",
            "cc_size_setting_2018": "INTEGER",
            "cc_size_setting_2021": "INTEGER",
        },

        # Composite PK used by `core_io` to build the ON CONFLICT target. This
        # ensures idempotent loads: re-running the same data updates the row
        # instead of inserting duplicates.
        "primary_key": ["unitid", "year"],

        # Function that converts a raw JSON record into a dict whose keys match
        # exactly the columns declared above under "schema".
        "mapper": map_directory_row,
    }
}


def get_endpoint_config(endpoint: str) -> EndpointCfg:
    """
    Fetch one endpoint's config from REGISTRY.

    Raises
    ------
    KeyError
        If the endpoint name isn't present in REGISTRY.
    """
    if endpoint not in REGISTRY:
        raise KeyError(f"Endpoint '{endpoint}' is not registered in etl/registry.py")
    return REGISTRY[endpoint]


def list_endpoints() -> list[str]:
    """
    Convenience helper for notebooks or diagnostics.
    Example:
        >>> from etl.registry import list_endpoints
        >>> list_endpoints()
        ['directory']
    """
    return sorted(REGISTRY.keys())