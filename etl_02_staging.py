# etl_02_staging.py  — versión corregida (sin duplicar load_ts)
import os, re, io, csv, glob
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PGConn

# 1) PON AQUÍ LA CARPETA DONDE ESTÁN TUS CSV:
CSV_DIR = r"C:\Users\moran\OneDrive\Escritorio\Univalle\TERCERO\ETL Tesis\dw_pack"  # <- ajusta la ruta

# 2) Nombres base (con o sin .csv). El script los resolverá dentro de CSV_DIR
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
    # NO incluir 'load_ts' aquí; la agregaremos al DF antes del COPY.
    cols = [slug(c) for c in df.columns if slug(c) != "load_ts"]
    cols_sql = ", ".join([f'"{c}" TEXT NULL' for c in cols] + ['"load_ts" TIMESTAMP NULL'])
    sql = f'CREATE TABLE IF NOT EXISTS stg."{table}" ({cols_sql});'
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

def copy_df(conn: PGConn, df: pd.DataFrame, table: str):
    # df YA debe traer 'load_ts'
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)
    cols = [slug(c) for c in df.columns]  # incluye load_ts
    cols_sql = ", ".join([f'"{c}"' for c in cols])
    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY stg."{table}" ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ",");',
            buf
        )
    conn.commit()

def resolve_file(base_name_or_with_ext: str) -> str | None:
    """
    Busca dentro de CSV_DIR:
      - Si trae extensión y existe: la usa.
      - Si no trae extensión: intenta .csv/.CSV y búsqueda case-insensitive.
    """
    if os.path.isabs(base_name_or_with_ext) and os.path.exists(base_name_or_with_ext):
        return base_name_or_with_ext

    candidate = base_name_or_with_ext
    root, ext = os.path.splitext(candidate)
    if not ext:
        candidate_csv = os.path.join(CSV_DIR, root + ".csv")
        candidate_csv2 = os.path.join(CSV_DIR, root + ".CSV")
        if os.path.exists(candidate_csv):  return candidate_csv
        if os.path.exists(candidate_csv2): return candidate_csv2
        pat = os.path.join(CSV_DIR, "*")
        for p in glob.glob(pat):
            if os.path.isfile(p):
                stem = os.path.splitext(os.path.basename(p))[0]
                if stem.lower() == root.lower() and os.path.splitext(p)[1].lower() == ".csv":
                    return p
        return None
    else:
        path = candidate if os.path.isabs(candidate) else os.path.join(CSV_DIR, candidate)
        return path if os.path.exists(path) else None

def main():
    import datetime
    print(f"[INFO] CSV_DIR: {CSV_DIR}")
    print(f"[INFO] Archivos detectados en CSV_DIR:")
    for p in sorted(glob.glob(os.path.join(CSV_DIR, "*"))):
        print("   -", os.path.basename(p))

    with psycopg2.connect(**DB) as conn:
        total = 0
        for table, name in CSV_PATHS.items():
            path = resolve_file(name)
            if not path or not os.path.exists(path):
                print(f"[SKIP] {table}: no existe archivo -> {name}")
                continue

            df = read_csv_flexible(path)
            df.columns = [slug(c) for c in df.columns]

            # 1) Crear tabla staging (sin load_ts duplicada)
            ensure_staging(conn, table, df)

            # 2) Añadir load_ts y copiar
            df["load_ts"] = datetime.datetime.now().isoformat(timespec="seconds")
            copy_df(conn, df, table)

            print(f"[STAGING] {table}: {len(df)} filas (desde {os.path.basename(path)})")
            total += len(df)

        print(f"[OK] Total filas staging: {total}")

if __name__ == "__main__":
    main()
