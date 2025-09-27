# etl_02_staging.py
import os, re, io, csv
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PGConn

# Ajusta nombres de archivos si es necesario
CSV_PATHS = {
    "stg_equipos_entregados":       "Equipos Entregados.csv",
    "stg_equipos_por_entregar":     "Equipos por entregar.csv",
    "stg_equipos_retirados":        "Equipos Retirados.csv",
    "stg_reporte_equipos":          "Reporte Equipos.csv",
    "stg_maestro_equipos":          "Maestro Equipos.csv",
    "stg_insumos_solicitados":      "Insumos Solicitados.csv",
    "stg_insumos_solicitados_hist": "Insumos Solicitados Histórico.csv",
    "stg_insumos_solicitados_hist_act": "Insumos Solicitados Histórico Actualizado.csv",
    "stg_insumos_por_programar":    "Insumos por Programar.csv",
    "stg_pedidos_parciales":        "Pedidos Parciales.csv",
    "stg_insumos_al_proveedor":     "Insumos Solicitados al Proveedor.csv",
    "stg_maestro_medicamentos":     "Maestro Medicamentos.csv",
    "stg_maestro_insumos_medicos":  "Maestro Insumos Medicos.csv",
    # si tienes "Inventario [PEPS].csv", agrégalo aquí si lo vas a usar
}

DB = dict(
    host=os.getenv("PGHOST","localhost"),
    port=int(os.getenv("PGPORT","5432")),
    user=os.getenv("PGUSER","postgres"),
    password=os.getenv("PGPASSWORD","postgres"),
    dbname=os.getenv("PGDATABASE","dw_salud"),
)

def slug(s):
    import unidecode
    s = unidecode.unidecode(str(s)).strip().lower()
    s = re.sub(r"[^a-z0-9]+","_",s)
    return re.sub(r"_+","_",s).strip("_")

def read_csv_flexible(path: str) -> pd.DataFrame:
    encs = ["utf-8-sig","latin1"]
    seps = [None,";",","]
    last = None
    for e in encs:
        for s in seps:
            try:
                if s is None:
                    return pd.read_csv(path, sep=None, engine="python", encoding=e, on_bad_lines="skip")
                else:
                    return pd.read_csv(path, sep=s, encoding=e, on_bad_lines="skip")
            except Exception as ex:
                last = ex
    raise last or RuntimeError(f"No se pudo leer {path}")

def ensure_staging(conn: PGConn, table: str, df: pd.DataFrame):
    cols = [slug(c) for c in df.columns]
    cols_sql = ", ".join([f'"{c}" TEXT NULL' for c in cols] + ['"load_ts" TIMESTAMP NULL'])
    sql = f'CREATE TABLE IF NOT EXISTS stg."{table}" ({cols_sql});'
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

def copy_df(conn: PGConn, df: pd.DataFrame, table: str):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)
    cols = [slug(c) for c in df.columns] + ["load_ts"]
    cols_sql = ", ".join([f'"{c}"' for c in cols])
    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY stg."{table}" ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ",");',
            buf
        )
    conn.commit()

def main():
    import datetime
    with psycopg2.connect(**DB) as conn:
        total = 0
        for table, path in CSV_PATHS.items():
            if not path or not os.path.exists(path):
                print(f"[SKIP] {table}: no existe archivo -> {path}")
                continue
            df = read_csv_flexible(path)
            df.columns = [slug(c) for c in df.columns]
            df["load_ts"] = datetime.datetime.now().isoformat(timespec="seconds")
            ensure_staging(conn, table, df)
            copy_df(conn, df, table)
            print(f"[STAGING] {table}: {len(df)} filas")
            total += len(df)
        print(f"[OK] Total filas staging: {total}")

if __name__ == "__main__":
    main()

