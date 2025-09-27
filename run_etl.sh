#!/usr/bin/env bash
# Ejecutar ETL DW (Linux/macOS)
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=TU_PASS
export PGDATABASE=dw_salud

python3 etl_dw_postgres.py
