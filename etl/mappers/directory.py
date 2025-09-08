"""
etl/mappers/directory.py
------------------------
Mapper for the IPEDS *Directory* endpoint.

Goal
----
Turn ONE raw JSON record (a Python `dict`) into a clean, typed `dict` whose keys
exactly match the columns declared in `etl/registry.py` for the "directory" endpoint.

Key behaviors
-------------
- IPEDS special values (-1, -2, -3) and blank strings are treated as *missing* (-> None).
- Gentle casting:
    _to_int   -> returns int or None
    _to_float -> returns float or None
    _to_str   -> returns trimmed string or None
- Field-name resilience: `_pick()` can look up from a list of candidate keys to
  tolerate historical variations (e.g., "inst_name" vs "instnm").
"""

from __future__ import annotations
from typing import Any, Dict, Optional


# -----------------------------------------------------------------------------
# Helpers: missing-value detection, key selection, and safe casting
# -----------------------------------------------------------------------------

def _is_missing(v: Any) -> bool:
    """
    Return True if `v` should be considered "missing" in IPEDS land.

    Handles both numeric and string encodings of special codes:
      - -1 = Missing/not reported
      - -2 = Not applicable
      - -3 = Suppressed data
    Also treats None and empty/whitespace-only strings as missing.
    """
    # Explicit None => missing
    if v is None:
        return True

    # Strings: trim whitespace and check for empty or stringified special codes
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s in {"-1", "-2", "-3"}:
            return True
        return False

    # Non-strings: check the numeric special codes
    return v in (-1, -2, -3)


def _pick(row: Dict[str, Any], keys: list[str]) -> Optional[Any]:
    """
    Return the first non-missing value from `row` among the provided `keys`.

    Why:
    - Payloads sometimes vary in field names across years/feeds.
    - This lets us declare a preference order, e.g.:
        _pick(row, ["inst_name", "institution_name", "instnm"])
    """
    for k in keys:
        if k in row and not _is_missing(row[k]):
            return row[k]
    return None


def _to_int(v: Any) -> Optional[int]:
    """
    Cast to int, returning None if the value is missing or malformed.

    Accepts:
      - int (returned as-is),
      - strings like "42" (whitespace tolerated),
      - otherwise returns None.
    """
    if _is_missing(v):
        return None
    try:
        if isinstance(v, str):
            v = v.strip()
        return int(v)
    except (ValueError, TypeError):
        return None


def _to_float(v: Any) -> Optional[float]:
    """
    Cast to float, returning None if missing or malformed.

    Accepts:
      - float/int,
      - strings like "12.34" (whitespace tolerated),
      - otherwise returns None.
    """
    if _is_missing(v):
        return None
    try:
        if isinstance(v, str):
            v = v.strip()
        return float(v)
    except (ValueError, TypeError):
        return None


def _to_str(v: Any) -> Optional[str]:
    """
    Cast to a trimmed string, returning None if missing or empty after trim.

    Notes:
      - Preserves meaningful text (e.g., URLs, names).
      - Converts numbers to strings only when they aren't IPEDS-missing codes.
    """
    if _is_missing(v):
        return None
    s = str(v).strip()
    return s if s else None


# -----------------------------------------------------------------------------
# Main mapper: raw JSON record -> typed dict aligned to registry schema
# -----------------------------------------------------------------------------

def map_directory_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize one *Directory* record.
    Returns a dict whose keys exactly match the columns declared for
    `ipeds_core.directory` in `etl/registry.py`.
    """
    return {
        # --------------------------- PRIMARY KEY ----------------------------
        "unitid": _to_int(_pick(row, ["unitid"])),
        "year": _to_int(_pick(row, ["year"])),

        # ----------------------- Identity / contact -------------------------
        "opeid": _to_str(_pick(row, ["opeid"])),
        "inst_name": _to_str(_pick(row, ["inst_name", "institution_name", "instnm", "name"])),
        "inst_alias": _to_str(_pick(row, ["inst_alias"])),
        "address": _to_str(_pick(row, ["address"])),
        "city": _to_str(_pick(row, ["city"])),
        "state_abbr": _to_str(_pick(row, ["state_abbr", "stabbr", "state"])),
        "zip": _to_str(_pick(row, ["zip", "zip5", "zip_code"])),
        "phone_number": _to_str(_pick(row, ["phone_number", "phone"])),
        "url_school": _to_str(_pick(row, ["url_school", "website", "web_address"])),
        "url_fin_aid": _to_str(_pick(row, ["url_fin_aid"])),
        "url_application": _to_str(_pick(row, ["url_application"])),
        "url_netprice": _to_str(_pick(row, ["url_netprice"])),
        "url_veterans": _to_str(_pick(row, ["url_veterans"])),
        "url_athletes": _to_str(_pick(row, ["url_athletes"])),
        "url_disability_services": _to_str(_pick(row, ["url_disability_services"])),
        "ein": _to_str(_pick(row, ["ein"])),
        "duns": _to_str(_pick(row, ["duns"])),
        "ueis": _to_str(_pick(row, ["ueis"])),
        "chief_admin_name": _to_str(_pick(row, ["chief_admin_name"])),
        "chief_admin_title": _to_str(_pick(row, ["chief_admin_title"])),
        "inst_system_name": _to_str(_pick(row, ["inst_system_name"])),

        # ---------------------------- Geography -----------------------------
        "fips": _to_int(_pick(row, ["fips"])),
        "county_name": _to_str(_pick(row, ["county_name"])),
        "county_fips": _to_int(_pick(row, ["county_fips"])),
        "region": _to_int(_pick(row, ["region"])),
        "urban_centric_locale": _to_int(_pick(row, ["urban_centric_locale", "locale"])),
        "cbsa": _to_int(_pick(row, ["cbsa"])),
        "cbsa_type": _to_int(_pick(row, ["cbsa_type"])),
        "csa": _to_int(_pick(row, ["csa"])),
        "necta": _to_int(_pick(row, ["necta"])),
        "congress_district_id": _to_int(_pick(row, ["congress_district_id"])),
        "latitude": _to_float(_pick(row, ["latitude", "lat"])),
        "longitude": _to_float(_pick(row, ["longitude", "lon", "lng"])),

        # ------------------------ Status / attributes -----------------------
        "inst_status": _to_int(_pick(row, ["inst_status"])),
        "sector": _to_int(_pick(row, ["sector", "sector_cd"])),
        "inst_control": _to_int(_pick(row, ["inst_control", "control"])),
        "institution_level": _to_int(_pick(row, ["institution_level", "level", "iclevel"])),
        "inst_category": _to_int(_pick(row, ["inst_category"])),
        "inst_size": _to_int(_pick(row, ["inst_size"])),
        "degree_granting": _to_int(_pick(row, ["degree_granting"])),
        "title_iv_indicator": _to_int(_pick(row, ["title_iv_indicator"])),
        "hbcu": _to_int(_pick(row, ["hbcu"])),
        "tribal_college": _to_int(_pick(row, ["tribal_college"])),
        "land_grant": _to_int(_pick(row, ["land_grant"])),
        "hospital": _to_int(_pick(row, ["hospital"])),
        "medical_degree": _to_int(_pick(row, ["medical_degree"])),
        "open_public": _to_int(_pick(row, ["open_public"])),
        "currently_active_ipeds": _to_int(_pick(row, ["currently_active_ipeds"])),
        "postsec_public_active": _to_int(_pick(row, ["postsec_public_active"])),
        "postsec_public_active_title_iv": _to_int(_pick(row, ["postsec_public_active_title_iv"])),
        "primarily_postsecondary": _to_int(_pick(row, ["primarily_postsecondary"])),
        "offering_highest_degree": _to_int(_pick(row, ["offering_highest_degree"])),
        "offering_highest_level": _to_int(_pick(row, ["offering_highest_level"])),
        "offering_undergrad": _to_int(_pick(row, ["offering_undergrad"])),
        "offering_grad": _to_int(_pick(row, ["offering_grad"])),
        "reporting_method": _to_int(_pick(row, ["reporting_method"])),
        "inst_system_flag": _to_int(_pick(row, ["inst_system_flag"])),
        "comparison_group": _to_int(_pick(row, ["comparison_group"])),
        "comparison_group_custom": _to_int(_pick(row, ["comparison_group_custom"])),

        # --------------------- Mergers / deletions / dates -------------------
        "newid": _to_int(_pick(row, ["newid"])),
        "date_closed": _to_str(_pick(row, ["date_closed"])),
        "year_deleted": _to_int(_pick(row, ["year_deleted"])),

        # ---------------------- Carnegie classifications ---------------------
        "cc_basic_2000": _to_int(_pick(row, ["cc_basic_2000"])),
        "cc_basic_2010": _to_int(_pick(row, ["cc_basic_2010"])),
        "cc_basic_2015": _to_int(_pick(row, ["cc_basic_2015"])),
        "cc_basic_2018": _to_int(_pick(row, ["cc_basic_2018"])),
        "cc_basic_2021": _to_int(_pick(row, ["cc_basic_2021"])),

        "cc_instruc_undergrad_2010": _to_int(_pick(row, ["cc_instruc_undergrad_2010"])),
        "cc_instruc_undergrad_2015": _to_int(_pick(row, ["cc_instruc_undergrad_2015"])),
        "cc_instruc_undergrad_2018": _to_int(_pick(row, ["cc_instruc_undergrad_2018"])),
        "cc_instruc_undergrad_2021": _to_int(_pick(row, ["cc_instruc_undergrad_2021"])),

        "cc_instruc_grad_2010": _to_int(_pick(row, ["cc_instruc_grad_2010"])),
        "cc_instruc_grad_2015": _to_int(_pick(row, ["cc_instruc_grad_2015"])),
        "cc_instruc_grad_2018": _to_int(_pick(row, ["cc_instruc_grad_2018"])),
        "cc_instruc_grad_2021": _to_int(_pick(row, ["cc_instruc_grad_2021"])),

        "cc_undergrad_2010": _to_int(_pick(row, ["cc_undergrad_2010"])),
        "cc_undergrad_2015": _to_int(_pick(row, ["cc_undergrad_2015"])),
        "cc_undergrad_2018": _to_int(_pick(row, ["cc_undergrad_2018"])),
        "cc_undergrad_2021": _to_int(_pick(row, ["cc_undergrad_2021"])),

        "cc_enroll_2010": _to_int(_pick(row, ["cc_enroll_2010"])),
        "cc_enroll_2015": _to_int(_pick(row, ["cc_enroll_2015"])),
        "cc_enroll_2018": _to_int(_pick(row, ["cc_enroll_2018"])),
        "cc_enroll_2021": _to_int(_pick(row, ["cc_enroll_2021"])),

        "cc_size_setting_2010": _to_int(_pick(row, ["cc_size_setting_2010"])),
        "cc_size_setting_2015": _to_int(_pick(row, ["cc_size_setting_2015"])),
        "cc_size_setting_2018": _to_int(_pick(row, ["cc_size_setting_2018"])),
        "cc_size_setting_2021": _to_int(_pick(row, ["cc_size_setting_2021"])),
    }