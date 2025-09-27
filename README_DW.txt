INSTRUCCIONES RÁPIDAS
1) Ejecuta el SQL en PostgreSQL:
   psql "host=localhost port=5432 dbname=dw_salud user=postgres password=TU_PASS" -f dw_postgres_ddl.sql

2) Instala dependencias (en un venv opcional):
   pip install psycopg2-binary pandas unidecode

3) Exporta variables de conexión o edita DB_CONFIG dentro del script:
   Windows PowerShell:
     $env:PGHOST="localhost"; $env:PGPORT="5432"; $env:PGUSER="postgres"; $env:PGPASSWORD="TU_PASS"; $env:PGDATABASE="dw_salud"
   Linux/macOS:
     export PGHOST="localhost"; export PGPORT="5432"; export PGUSER="postgres"; export PGPASSWORD="TU_PASS"; export PGDATABASE="dw_salud"

4) Pon los CSV junto al script y ejecuta:
   python etl_dw_postgres.py
