@echo off
REM Ejecutar ETL DW (Windows)
set PGHOST=localhost
set PGPORT=5432
set PGUSER=postgres
set PGPASSWORD=TU_PASS
set PGDATABASE=dw_salud

python etl_dw_postgres.py
pause
