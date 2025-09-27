# etl_04_hechos.py — corregido: get_id con SCD2, normalización de fechas
import os, re
import pandas as pd
from unidecode import unidecode
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB = dict(
    host=os.getenv("PGHOST","localhost"),
    port=int(os.getenv("PGPORT","5432")),
    user=os.getenv("PGUSER","postgres"),
    password=os.getenv("PGPASSWORD","postgres"),
    dbname=os.getenv("PGDATABASE","dw_salud"),
)

SCHEMA_STG="stg"; SCHEMA_DW="dw"

# --- util: patrones y fechas ---
def pick(df, pats):
    for p in pats:
        rx = re.compile(p)
        for c in df.columns:
            if rx.search(c): return c
    return None

SPANISH_MONTHS = {
    "ene":"jan","feb":"feb","mar":"mar","abr":"apr","may":"may","jun":"jun",
    "jul":"jul","ago":"aug","sep":"sep","set":"sep","oct":"oct","nov":"nov","dic":"dec",
    "enero":"january","febrero":"february","marzo":"march","abril":"april","mayo":"may","junio":"june",
    "julio":"july","agosto":"august","septiembre":"september","setiembre":"september",
    "octubre":"october","noviembre":"november","diciembre":"diciembre"
}
def normalize_spanish_months(s: str) -> str:
    if not s: return s
    s0 = unidecode(str(s))
    keys = sorted(SPANISH_MONTHS.keys(), key=len, reverse=True)
    pat = r"\b(" + "|".join(re.escape(k) for k in keys) + r")\b"
    def repl(m):
        key = m.group(0).lower()
        return SPANISH_MONTHS.get(key, m.group(0))
    return re.sub(pat, repl, s0, flags=re.IGNORECASE)

def normalize_date_any(value) -> pd.Timestamp | None:
    if pd.isna(value): return None
    s = str(value).strip()
    if not s: return None
    s_norm = normalize_spanish_months(s)
    ts = pd.to_datetime(s_norm, errors="coerce", dayfirst=True, infer_datetime_format=True)
    if pd.isna(ts):
        for fmt in ("%d-%b-%Y %I:%M%p", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y %H:%M", "%d-%m-%Y"):
            try:
                ts = pd.to_datetime(datetime.strptime(s_norm, fmt))
                break
            except Exception:
                continue
    return None if pd.isna(ts) else ts

# --- util: lookup IDs ---
SCD2_TABLES = {"dim_paciente", "dim_equipo"}  # solo estas tienen es_actual

def get_id(conn, table, id_col, nk_col, nk_val):
    sql = f"SELECT {id_col} FROM {SCHEMA_DW}.{table} WHERE {nk_col}=%s"
    params = [nk_val]
    if table in SCD2_TABLES:
        sql += " AND es_actual=TRUE"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        r = cur.fetchone()
        return r[0] if r else None

# =========================
# HECHO: Equipos
# =========================
def load_hecho_equipos():
    inserted=0
    with psycopg2.connect(**DB) as conn:
        for t in ["stg_equipos_entregados","stg_equipos_retirados","stg_equipos_por_entregar","stg_reporte_equipos"]:
            try:
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
            except Exception:
                continue

            col_equipo = pick(df,[r"(serial|serie|codigo|equipo)"])
            col_pac    = pick(df,[r"(docu|id).*pac|paciente|historia|nombre"])
            col_aseg   = pick(df,[r"asegur|eps|ars"])
            col_fecha  = pick(df,[r"fecha"])
            if not (col_equipo and col_pac and col_aseg and col_fecha):
                continue

            df["_equipo_nk"]   = df[col_equipo].astype(str).str.strip()
            df["_paciente_nk"] = df[col_pac].astype(str).str.strip()
            df["_aseg_nk"]     = df[col_aseg].astype(str).str.strip().str.upper().map(unidecode)
            df["_fecha_ts"]    = df[col_fecha].map(normalize_date_any)
            df["_fecha"]       = df["_fecha_ts"].dropna().map(lambda x: x.date() if x is not None else None)

            batch=[]
            for _,r in df.dropna(subset=["_equipo_nk","_paciente_nk","_aseg_nk","_fecha"]).iterrows():
                eid = get_id(conn,"dim_equipo","equipo_id","equipo_nk",r["_equipo_nk"])
                pid = get_id(conn,"dim_paciente","paciente_id","paciente_nk",r["_paciente_nk"])
                aid = get_id(conn,"dim_aseguradora","aseguradora_id","aseguradora_nk",r["_aseg_nk"])
                if not all([eid,pid,aid]): 
                    continue
                batch.append((eid,pid,aid,r["_fecha"]))

            if batch:
                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO dw.hecho_equipos(equipo_id,paciente_id,aseguradora_id,fecha_solicitud_id)
                        VALUES %s
                    """, batch)
                conn.commit()
                inserted += len(batch)

    print(f"[HECHO_EQUIPOS] insertados {inserted}")

# =========================
# HECHO: Solicitud Servicios
# =========================
def load_hecho_solicitud_servicios():
    inserted=0
    with psycopg2.connect(**DB) as conn:
        for t in ["stg_insumos_solicitados","stg_pedidos_parciales","stg_insumos_solicitados_hist","stg_insumos_solicitados_hist_act"]:
            try:
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
            except Exception:
                continue

            col_np   = pick(df,[r"(numero|num|nro).*pedido", r"pedido.*(num|numero|id)"])
            col_pac  = pick(df,[r"(docu|id).*pac|paciente|historia|nombre"])
            col_aseg = pick(df,[r"asegur|eps|ars"])
            col_fecha= pick(df,[r"fecha"])
            col_med  = pick(df,[r"(codigo|sku|referen).*", r"medicamento.*(id|codigo)"])
            if not (col_np and col_pac and col_aseg and col_fecha and col_med):
                continue

            df["_np_nk"]   = df[col_np].astype(str).str.strip()
            df["_pac_nk"]  = df[col_pac].astype(str).str.strip()
            df["_aseg_nk"] = df[col_aseg].astype(str).str.strip().str.upper().map(unidecode)
            df["_fecha_ts"]= df[col_fecha].map(normalize_date_any)
            df["_fecha"]   = df["_fecha_ts"].dropna().map(lambda x: x.date() if x is not None else None)
            df["_med_nk"]  = df[col_med].astype(str).str.strip()

            batch=[]
            for _,r in df.dropna(subset=["_np_nk","_pac_nk","_aseg_nk","_fecha","_med_nk"]).iterrows():
                # dim_pedido (crea si falta)
                with conn.cursor() as cur:
                    cur.execute("SELECT numero_pedido_id FROM dw.dim_pedido WHERE numero_pedido_nk=%s",(r["_np_nk"],))
                    rp = cur.fetchone()
                    if not rp:
                        cur.execute("INSERT INTO dw.dim_pedido(numero_pedido_nk) VALUES (%s) ON CONFLICT DO NOTHING",(r["_np_nk"],))
                        cur.execute("SELECT numero_pedido_id FROM dw.dim_pedido WHERE numero_pedido_nk=%s",(r["_np_nk"],))
                        rp = cur.fetchone()
                np_id = rp[0] if rp else None

                pac_id = get_id(conn,"dim_paciente","paciente_id","paciente_nk",r["_pac_nk"])
                aseg_id= get_id(conn,"dim_aseguradora","aseguradora_id","aseguradora_nk",r["_aseg_nk"])

                # dim_medicamento (crea si falta)
                with conn.cursor() as cur:
                    cur.execute("SELECT codigo_medicamento FROM dw.dim_medicamento WHERE medicamento_nk=%s",(r["_med_nk"],))
                    rm = cur.fetchone()
                    if not rm:
                        cur.execute("INSERT INTO dw.dim_medicamento(medicamento_nk) VALUES (%s) ON CONFLICT DO NOTHING",(r["_med_nk"],))
                        cur.execute("SELECT codigo_medicamento FROM dw.dim_medicamento WHERE medicamento_nk=%s",(r["_med_nk"],))
                        rm = cur.fetchone()
                med_id = rm[0] if rm else None

                if not all([np_id,pac_id,aseg_id,med_id]): 
                    continue
                batch.append((np_id,pac_id,aseg_id,r["_fecha"],med_id))

            if batch:
                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO dw.hecho_solicitud_servicios(numero_pedido_id,paciente_id,aseguradora_id,fecha_solicitud_id,codigo_medicamento)
                        VALUES %s
                    """, batch)
                conn.commit()
                inserted += len(batch)

    print(f"[HECHO_SOLICITUD_SERVICIOS] insertados {inserted}")

# --------------------------
if __name__ == "__main__":
    load_hecho_equipos()
    load_hecho_solicitud_servicios()
