# IPEDS ETL

ETL pipeline for loading **IPEDS data** from the [Urban Institute Education Data API](https://educationdata.urban.org/) into **PostgreSQL**.  
Includes raw + clean schemas, dimension lookups, BI-friendly views, role-based access, and lineage tracking for reproducible, analysis-ready higher ed datasets.

---

## Overview

A reproducible pipeline to ingest, clean, and structure IPEDS into a warehouse analysts and BI tools can query directly.

- **Extract** → Pull JSON with retries, pagination, and rate limiting.  
- **Transform** → Map/cast fields, normalize IPEDS special codes (-1/-2/-3 → NULL).  
- **Load** → Persist to layered Postgres schemas with audit logs and lineage.

---

## Quickstart

### Prereqs
- Python 3.10+  
- PostgreSQL 13+  
- `psycopg2-binary`, `SQLAlchemy`, `requests`, `jupyterlab` (see `requirements.txt`)

### Setup
```bash
git clone https://github.com/<your-username>/ipeds_etl.git
cd ipeds_etl

python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt
cp config/.env.example .env
Edit .env:


DATABASE_URL=postgresql://user:pass@localhost:5432/ipeds_db
Run a sample load (Jupyter)

jupyter lab
Open notebooks/10_load_endpoint.ipynb and load e.g. Admissions 2015–2020.

Project Structure (high level)
graphql
Copy code
ipeds-etl/
├─ notebooks/        # Jupyter entry points
├─ etl/              # Python ETL engine (http/db/raw_io/core_io/etc.)
├─ sql/              # Schemas, indexes, example views
├─ config/           # .env + optional endpoints.yaml
├─ tests/            # Unit + smoke tests
├─ docs/             # Detailed docs
└─ README.md
Database layout, roles, maintenance, and lineage → see docs/architecture.md.

Citation / Acknowledgements
This project makes use of the Urban Institute’s Education Data API.
Full acknowledgements and data source details:
Urban Institute Education Data API – Acknowledgements

Disclaimer: This project is not affiliated with the Urban Institute. Transformations and any errors are the author’s own.