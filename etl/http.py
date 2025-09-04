"""
etl/http.py
-----------
This module handles all HTTP requests to the Urban Institute IPEDS API.

It uses:
- A persistent `requests.Session` for connection reuse.
- Exponential backoff for retryable errors.
- Respect for API rate limits (RPS delay).
- Automatic pagination handling (using `next` from response).
- Centralized config from `etl/config.py`.

This is the ONLY place that should call the API directly.
Other modules (e.g., raw_io.py) call `fetch_endpoint_data(...)`.
"""

from __future__ import annotations

import time
import requests
from urllib.parse import urljoin
from typing import Any

from etl.config import settings

# -----------------------------------------------------------------------------
# Global session object to reuse TCP connections
# -----------------------------------------------------------------------------
# This avoids the overhead of reconnecting for every request.
session = requests.Session()


# -----------------------------------------------------------------------------
# Retry logic with exponential backoff
# -----------------------------------------------------------------------------
def get_with_retries(
    url: str,
    params: dict[str, Any] | None = None,
    max_retries: int = settings.MAX_RETRIES,
) -> requests.Response:
    """
    Makes a GET request with retry logic and exponential backoff.
    Used by the main data fetcher below.

    Parameters
    ----------
    url : str
        Full API URL (e.g., https://.../directory/).
    params : dict, optional
        Query string parameters (e.g., {"year": 2022, "page": 3}).
    max_retries : int
        Max number of retry attempts before raising an error.

    Returns
    -------
    response : requests.Response
        The successful response (status 200).

    Raises
    ------
    Exception
        If all retries fail.
    """
    for attempt in range(max_retries):
        try:
            response = session.get(
                url,
                params=params,
                headers={"User-Agent": settings.USER_AGENT},
                timeout=settings.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response

        except requests.RequestException as e:
            print(f"[WARN] Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s...

    raise Exception(f"[FAIL] Giving up after {max_retries} failed attempts to fetch {url}")


# -----------------------------------------------------------------------------
# Main function: fetch all paginated results for one endpoint + year
# -----------------------------------------------------------------------------
def fetch_endpoint_data(endpoint_path: str, year: int) -> list[dict[str, Any]]:
    """
    Downloads all data from a paginated Urban IPEDS API endpoint for a given year.

    `endpoint_path` can be either:
      - "ipeds/directory/"            (we'll append "{year}/")
      - "ipeds/directory/{year}/"     (we'll format it)
    """
    all_results: list[dict[str, Any]] = []

    # Normalize base and path
    base = settings.URBAN_BASE_URL.rstrip("/") + "/"
    # Allow both "ipeds/directory/" and "ipeds/directory/{year}/"
    if "{year}" in endpoint_path:
        path = endpoint_path.format(year=year).strip("/") + "/"
    else:
        path = f"{endpoint_path.strip('/')}/{year}/"

    # First page URL
    url = urljoin(base, path)

    while True:
        # GET (no year in query; year is in path)
        response = get_with_retries(url, params=None)
        data = response.json()

        # Collect records from this page
        all_results.extend(data.get("results", []))

        # Follow 'next' if present (it can be absolute or relative)
        next_url = data.get("next")
        if not next_url:
            break
        url = next_url if next_url.startswith("http") else urljoin(base, next_url.lstrip("/"))

        # gentle rate limit
        time.sleep(1.0 / settings.RATE_LIMIT_RPS)

    print(f"[OK] Fetched {len(all_results):,} records from {path} (year={year})")
    return all_results