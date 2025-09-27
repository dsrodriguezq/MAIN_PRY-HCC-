# -*- coding: utf-8 -*-
# ETL DW para PostgreSQL - ChatGPT
# Requisitos:
#   pip install psycopg2-binary pandas unidecode

import os, re, io, csv
from datetime import date, datetime, timedelta
from typing import Optional, List, Tuple
import pandas as pd
from unidecode import unidecode
import psycopg2
from psycopg2.extensions import connection as PGConn
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "dbname": os.getenv("PGDATABASE", "dw_salud")
}

SCHEMA_STG = "stg"
SCHEMA_DW  = "dw"

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
    "stg_inventario_peps":          "Inventario [PEPS].csv"
}

def get_conn() -> PGConn:
    return psycopg2.connect(**DB_CONFIG)

def run_sql(sql: str, params: Optional[Tuple]=None) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)

def fetch_all(sql: str, params: Optional[Tuple]=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def log_fin(proceso: str, detalle: str, filas: int, estado: str="OK") -> None:
    run_sql("SELECT dw.log_fin(%s,%s,%s,%s);", (proceso, detalle, filas, estado))

def slugify_col(name: str) -> str:
    name = unidecode(str(name)).strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name

def read_csv_flexible(path: str) -> pd.DataFrame:
    encs = ["utf-8-sig","latin1"]
    seps = [None,";",","]
    last_err = None
    for enc in encs:
        for sep in seps:
            try:
                if sep is None:
                    df = pd.read_csv(path, sep=None, engine="python", encoding=enc, on_bad_lines="skip")
                else:
                    df = pd.read_csv(path, sep=sep, encoding=enc, on_bad_lines="skip")
                return df
            except Exception as e:
                last_err = e
    raise last_err if last_err else Exception(f"No se pudo leer {path}")

def ensure_table_staging(table: str, df: pd.DataFrame) -> None:
    cols = [slugify_col(c) for c in df.columns]
    cols_sql = ", ".join([f'"{c}" TEXT NULL' for c in cols] + ['"load_ts" TIMESTAMP NULL'])
    sql = f'CREATE TABLE IF NOT EXISTS {SCHEMA_STG}."{table}" ({cols_sql});'
    run_sql(sql)

def copy_df(conn: PGConn, df: pd.DataFrame, table: str) -> int:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)
    with conn.cursor() as cur:
        cols = [slugify_col(c) for c in df.columns] + ["load_ts"]
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        copy_sql = f'COPY {SCHEMA_STG}."{table}" ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ",");'
        cur.copy_expert(copy_sql, buffer)
    conn.commit()
    return len(df)

def load_to_staging() -> None:
    total = 0
    for table, path in CSV_PATHS.items():
        if not path or not os.path.exists(path):
            continue
        try:
            df = read_csv_flexible(path)
            df.columns = [slugify_col(c) for c in df.columns]
            df["load_ts"] = datetime.now().isoformat(timespec="seconds")
            ensure_table_staging(table, df)
            with get_conn() as conn:
                inserted = copy_df(conn, df, table)
            log_fin("staging", f"Cargado {table}", inserted, "OK")
            total += inserted
            print(f"[STG] {table}: {inserted} filas")
        except Exception as e:
            log_fin("staging", f"Error {table}: {e}", 0, "ERROR")
            print(f"[STG][ERROR] {table}: {e}")
    print(f"[STG] Total filas: {total}")

def populate_dim_fecha(start: date=date(2019,1,1), end: date=date(2030,12,31)) -> None:
    rows = []
    d = start
    while d <= end:
        rows.append((d, d.year, d.month, d.day))
        d += timedelta(days=1)
    sql = """
    INSERT INTO dw.dim_fecha (fecha_solicitud_id, ano, mes, dia)
    VALUES %s
    ON CONFLICT (fecha_solicitud_id) DO NOTHING;
    """
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        conn.commit()
    log_fin("dim_fecha", f"Calendario {start}..{end}", len(rows), "OK")
    print(f"[DIM_FECHA] Rango {start}..{end} insertado (upsert).")

def pick_first_col(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    for p in patterns:
        regex = re.compile(p)
        for c in df.columns:
            if regex.search(c):
                return c
    return None

def upsert_dim_aseguradora() -> None:
    inserted = 0
    candidate_tables = [t for t in CSV_PATHS.keys() if t.startswith("stg_")]
    aseg_values = set()
    for t in candidate_tables:
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
            col = pick_first_col(df, [r"asegur", r"eps", r"ars"])
            if col:
                vals = df[col].dropna().astype(str).str.strip().str.upper().apply(unidecode).unique()
                aseg_values.update(vals)
        except Exception:
            continue
    rows = [(a, a) for a in sorted(aseg_values) if a and a != "NAN"]
    sql = """
    INSERT INTO dw.dim_aseguradora(aseguradora_nk, aseguradora)
    VALUES %s
    ON CONFLICT (aseguradora_nk) DO UPDATE SET aseguradora = EXCLUDED.aseguradora;
    """
    if rows:
        with get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
            conn.commit()
    log_fin("dim_aseguradora", "upsert", len(rows), "OK")
    print(f"[DIM_ASEGURADORA] upsert {len(rows)} valores.")

def scd2_upsert_dim_paciente(df_src: pd.DataFrame) -> int:
    inserted = 0
    with get_conn() as conn, conn.cursor() as cur:
        for _, r in df_src.iterrows():
            nk = str(r.get("paciente_nk", "")).strip()
            if not nk:
                continue
            cur.execute("SELECT paciente_id, nombre, municipio, estado, aseguradora, zona, fecha_ingreso "
                        "FROM dw.dim_paciente WHERE paciente_nk=%s AND es_actual=TRUE", (nk,))
            row = cur.fetchone()
            changed = False
            if row:
                (pid, nombre, municipio, estado, aseguradora, zona, fecha_ingreso) = row
                for col in ["nombre","municipio","estado","aseguradora","zona"]:
                    if str(r.get(col) or "").strip() != str(locals()[col] or "").strip():
                        changed = True; break
                if not changed and str(r.get("fecha_ingreso") or "") != str(fecha_ingreso or ""):
                    changed = True
                if changed:
                    cur.execute("UPDATE dw.dim_paciente SET vigente_hasta=now(), es_actual=FALSE "
                                "WHERE paciente_id=%s AND es_actual=TRUE", (pid,))
            if (not row) or changed:
                cur.execute("""
                    INSERT INTO dw.dim_paciente(paciente_nk, nombre, municipio, estado, aseguradora, zona, fecha_ingreso)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (nk, r.get("nombre"), r.get("municipio"), r.get("estado"),
                      r.get("aseguradora"), r.get("zona"), r.get("fecha_ingreso")))
                inserted += 1
        conn.commit()
    print(f"[DIM_PACIENTE] upsert/ scd2 insertados {inserted}.")
    log_fin("dim_paciente", "upsert_scd2", inserted, "OK")
    return inserted

def scd2_upsert_dim_equipo(df_src: pd.DataFrame) -> int:
    inserted = 0
    with get_conn() as conn, conn.cursor() as cur:
        for _, r in df_src.iterrows():
            nk = str(r.get("equipo_nk", "")).strip()
            if not nk:
                continue
            cur.execute("SELECT equipo_id, equipo, estado_equipo FROM dw.dim_equipo "
                        "WHERE equipo_nk=%s AND es_actual=TRUE", (nk,))
            row = cur.fetchone()
            changed = False
            if row:
                (eid, equipo, estado_equipo) = row
                for col in ["equipo","estado_equipo"]:
                    if str(r.get(col) or "").strip() != str(locals()[col] or "").strip():
                        changed = True; break
                if changed:
                    cur.execute("UPDATE dw.dim_equipo SET vigente_hasta=now(), es_actual=FALSE "
                                "WHERE equipo_id=%s AND es_actual=TRUE", (eid,))
            if (not row) or changed:
                cur.execute("""
                    INSERT INTO dw.dim_equipo(equipo_nk, equipo, estado_equipo)
                    VALUES (%s,%s,%s)
                """, (nk, r.get("equipo"), r.get("estado_equipo")))
                inserted += 1
        conn.commit()
    print(f"[DIM_EQUIPO] upsert/ scd2 insertados {inserted}.")
    log_fin("dim_equipo", "upsert_scd2", inserted, "OK")
    return inserted

def upsert_dim_medicamento(df_src: pd.DataFrame) -> int:
    rows = []
    for _, r in df_src.iterrows():
        nk = str(r.get("medicamento_nk","")).strip()
        if not nk: continue
        rows.append((nk, r.get("nombre"), r.get("forma_farmaceutica"), r.get("via_administracion")))
    sql = """
    INSERT INTO dw.dim_medicamento(medicamento_nk, nombre, forma_farmaceutica, via_administracion)
    VALUES %s
    ON CONFLICT (medicamento_nk)
    DO UPDATE SET nombre=EXCLUDED.nombre,
                  forma_farmaceutica=EXCLUDED.forma_farmaceutica,
                  via_administracion=EXCLUDED.via_administracion;
    """
    if rows:
        with get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
            conn.commit()
    print(f"[DIM_MEDICAMENTO] upsert {len(rows)}.")
    log_fin("dim_medicamento", "upsert", len(rows), "OK")
    return len(rows)

def upsert_dim_pedido(df_src: pd.DataFrame) -> int:
    rows = []
    for _, r in df_src.iterrows():
        nk = str(r.get("numero_pedido_nk","")).strip()
        if not nk: continue
        rows.append((nk, r.get("insumo_solicitado"), r.get("cantidad")))
    sql = """
    INSERT INTO dw.dim_pedido(numero_pedido_nk, insumo_solicitado, cantidad)
    VALUES %s
    ON CONFLICT (numero_pedido_nk)
    DO UPDATE SET insumo_solicitado=EXCLUDED.insumo_solicitado,
                  cantidad=EXCLUDED.cantidad;
    """
    if rows:
        with get_conn() as conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
            conn.commit()
    print(f"[DIM_PEDIDO] upsert {len(rows)}.")
    log_fin("dim_pedido", "upsert", len(rows), "OK")
    return len(rows)

def get_id(table: str, id_col: str, nk_col: str, nk_val: str):
    sql = f"SELECT {id_col} FROM {SCHEMA_DW}.{table} WHERE {nk_col}=%s AND (es_actual IS NULL OR es_actual=TRUE)"
    rows = fetch_all(sql, (nk_val,))
    return rows[0][0] if rows else None

def build_df_aseguradoras(): upsert_dim_aseguradora()

def build_df_pacientes():
    candidate_tables = [t for t in CSV_PATHS.keys() if t.startswith("stg_")]
    rows = {}
    for t in candidate_tables:
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
        except Exception:
            continue
        col_id = pick_first_col(df, [r"(docu|id).*pac", r"paciente.*(id|doc)", r"identificacion", r"historia"]) or pick_first_col(df,[r"nombre"])
        col_nombre = pick_first_col(df, [r"nombre", r"paciente"])
        col_mun = pick_first_col(df, [r"munic", r"ciudad"])
        col_est = pick_first_col(df, [r"depto|depart|estado"])
        col_zona = pick_first_col(df, [r"zona|barrio"])
        col_aseg = pick_first_col(df, [r"asegur|eps|ars"])
        col_fing = pick_first_col(df, [r"fecha.*ingreso", r"ingreso"])

        for _, r in df.iterrows():
            nk = str(r.get(col_id) or "").strip()
            if not nk: continue
            rows[nk] = {
                "paciente_nk": nk,
                "nombre": str(r.get(col_nombre) or "").strip(),
                "municipio": str(r.get(col_mun) or "").strip(),
                "estado": str(r.get(col_est) or "").strip(),
                "aseguradora": str(r.get(col_aseg) or "").strip(),
                "zona": str(r.get(col_zona) or "").strip(),
                "fecha_ingreso": r.get(col_fing)
            }
    if rows:
        df_out = pd.DataFrame(list(rows.values()))
        scd2_upsert_dim_paciente(df_out)

def build_df_equipos():
    frames = []
    for t in ["stg_maestro_equipos", "stg_reporte_equipos", "stg_equipos_entregados", "stg_equipos_retirados", "stg_equipos_por_entregar"]:
        path = CSV_PATHS.get(t)
        if not path: continue
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
            col_nk = pick_first_col(df, [r"(serial|serie|codigo|equipo).*"])
            col_nombre = pick_first_col(df, [r"equipo|descripcion|nombre"])
            col_estado = pick_first_col(df, [r"estado"])
            tmp = pd.DataFrame({
                "equipo_nk": df[col_nk] if col_nk in df.columns else None,
                "equipo": df[col_nombre] if col_nombre in df.columns else None,
                "estado_equipo": df[col_estado] if col_estado in df.columns else None
            })
            frames.append(tmp)
        except Exception:
            continue
    if frames:
        df_out = pd.concat(frames, ignore_index=True).dropna(subset=["equipo_nk"]).drop_duplicates("equipo_nk")
        scd2_upsert_dim_equipo(df_out)

def build_df_medicamentos():
    frames = []
    for t in ["stg_maestro_medicamentos", "stg_maestro_insumos_medicos"]:
        path = CSV_PATHS.get(t)
        if not path: continue
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
            col_nk = pick_first_col(df, [r"(codigo|sku|referen).*", r"medicamento.*(id|codigo)"])
            col_nombre = pick_first_col(df, [r"(nombre|descripcion)"])
            col_forma = pick_first_col(df, [r"forma"])
            col_via = pick_first_col(df, [r"via"])
            tmp = pd.DataFrame({
                "medicamento_nk": df[col_nk] if col_nk in df.columns else None,
                "nombre": df[col_nombre] if col_nombre in df.columns else None,
                "forma_farmaceutica": df[col_forma] if col_forma in df.columns else None,
                "via_administracion": df[col_via] if col_via in df.columns else None
            })
            frames.append(tmp)
        except Exception:
            continue
    if frames:
        df_out = pd.concat(frames, ignore_index=True).dropna(subset=["medicamento_nk"]).drop_duplicates("medicamento_nk")
        upsert_dim_medicamento(df_out)

def build_df_pedido():
    frames = []
    for t in ["stg_insumos_solicitados", "stg_pedidos_parciales", "stg_insumos_solicitados_hist", "stg_insumos_solicitados_hist_act"]:
        path = CSV_PATHS.get(t)
        if not path: continue
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
            col_np = pick_first_col(df, [r"(numero|num|nro).*pedido", r"pedido.*(num|numero|id)"])
            col_ins = pick_first_col(df, [r"(insumo|producto|medicamento|articulo|descripcion)"])
            col_can = pick_first_col(df, [r"(cant|unid)"])
            tmp = pd.DataFrame({
                "numero_pedido_nk": df[col_np] if col_np in df.columns else None,
                "insumo_solicitado": df[col_ins] if col_ins in df.columns else None,
                "cantidad": pd.to_numeric(df[col_can], errors="coerce") if col_can in df.columns else None
            })
            frames.append(tmp)
        except Exception:
            continue
    if frames:
        df_out = pd.concat(frames, ignore_index=True).dropna(subset=["numero_pedido_nk"]).drop_duplicates("numero_pedido_nk")
        upsert_dim_pedido(df_out)

def load_hecho_equipos():
    inserted = 0
    for t in ["stg_equipos_entregados", "stg_equipos_retirados", "stg_equipos_por_entregar", "stg_reporte_equipos"]:
        path = CSV_PATHS.get(t)
        if not path: continue
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
        except Exception:
            continue
        col_equipo = pick_first_col(df, [r"(serial|serie|codigo|equipo)"])
        col_pac = pick_first_col(df, [r"(docu|id).*pac|paciente|historia|nombre"])
        col_aseg = pick_first_col(df, [r"asegur|eps|ars"])
        col_fecha = pick_first_col(df, [r"fecha"])
        if not (col_equipo and col_pac and col_aseg and col_fecha):
            continue
        df["_equipo_nk"] = df[col_equipo].astype(str).str.strip()
        df["_paciente_nk"] = df[col_pac].astype(str).str.strip()
        df["_aseg_nk"] = df[col_aseg].astype(str).str.strip().str.upper().apply(unidecode)
        df["_fecha"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.date

        batch = []
        for _, r in df.dropna(subset=["_equipo_nk","_paciente_nk","_aseg_nk","_fecha"]).iterrows():
            equipo_id = get_id("dim_equipo", "equipo_id", "equipo_nk", r["_equipo_nk"])
            paciente_id = get_id("dim_paciente", "paciente_id", "paciente_nk", r["_paciente_nk"])
            aseg_id    = get_id("dim_aseguradora", "aseguradora_id", "aseguradora_nk", r["_aseg_nk"])
            if not all([equipo_id, paciente_id, aseg_id]):
                continue
            batch.append((equipo_id, paciente_id, aseg_id, r["_fecha"]))
        if batch:
            sql = """
            INSERT INTO dw.hecho_equipos(equipo_id, paciente_id, aseguradora_id, fecha_solicitud_id)
            VALUES %s;
            """
            with get_conn() as conn, conn.cursor() as cur:
                execute_values(cur, sql, batch)
                conn.commit()
            inserted += len(batch)
    log_fin("hecho_equipos", "load", inserted, "OK")
    print(f"[HECHO_EQUIPOS] insertados {inserted}.")

def load_hecho_solicitud_servicios():
    inserted = 0
    for t in ["stg_insumos_solicitados", "stg_pedidos_parciales",
              "stg_insumos_solicitados_hist", "stg_insumos_solicitados_hist_act"]:
        path = CSV_PATHS.get(t)
        if not path: continue
        try:
            df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', get_conn())
        except Exception:
            continue
        col_np = pick_first_col(df, [r"(numero|num|nro).*pedido", r"pedido.*(num|numero|id)"])
        col_pac = pick_first_col(df, [r"(docu|id).*pac|paciente|historia|nombre"])
        col_aseg = pick_first_col(df, [r"asegur|eps|ars"])
        col_fecha = pick_first_col(df, [r"fecha"])
        col_codmed = pick_first_col(df, [r"(codigo|sku|referen).*", r"medicamento.*(id|codigo)"])

        if not (col_np and col_pac and col_aseg and col_fecha and col_codmed):
            continue
        df["_np_nk"] = df[col_np].astype(str).str.strip()
        df["_paciente_nk"] = df[col_pac].astype(str).str.strip()
        df["_aseg_nk"] = df[col_aseg].astype(str).str.strip().str.upper().apply(unidecode)
        df["_fecha"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.date
        df["_med_nk"] = df[col_codmed].astype(str).str.strip()

        batch = []
        for _, r in df.dropna(subset=["_np_nk","_paciente_nk","_aseg_nk","_fecha","_med_nk"]).iterrows():
            rows = fetch_all("SELECT numero_pedido_id FROM dw.dim_pedido WHERE numero_pedido_nk=%s", (r["_np_nk"],))
            if not rows:
                run_sql("INSERT INTO dw.dim_pedido(numero_pedido_nk) VALUES (%s) ON CONFLICT DO NOTHING", (r["_np_nk"],))
                rows = fetch_all("SELECT numero_pedido_id FROM dw.dim_pedido WHERE numero_pedido_nk=%s", (r["_np_nk"],))
            numero_pedido_id = rows[0][0] if rows else None

            paciente_id = get_id("dim_paciente", "paciente_id", "paciente_nk", r["_paciente_nk"])
            aseg_id     = get_id("dim_aseguradora", "aseguradora_id", "aseguradora_nk", r["_aseg_nk"])

            rows = fetch_all("SELECT codigo_medicamento FROM dw.dim_medicamento WHERE medicamento_nk=%s", (r["_med_nk"],))
            if not rows:
                run_sql("INSERT INTO dw.dim_medicamento(medicamento_nk) VALUES (%s) ON CONFLICT DO NOTHING", (r["_med_nk"],))
                rows = fetch_all("SELECT codigo_medicamento FROM dw.dim_medicamento WHERE medicamento_nk=%s", (r["_med_nk"],))
            codigo_medicamento = rows[0][0] if rows else None

            if not all([numero_pedido_id, paciente_id, aseg_id, codigo_medicamento]):
                continue

            batch.append((numero_pedido_id, paciente_id, aseg_id, r["_fecha"], codigo_medicamento))

        if batch:
            sql = """
            INSERT INTO dw.hecho_solicitud_servicios(numero_pedido_id, paciente_id, aseguradora_id, fecha_solicitud_id, codigo_medicamento)
            VALUES %s;
            """
            with get_conn() as conn, conn.cursor() as cur:
                execute_values(cur, sql, batch)
                conn.commit()
            inserted += len(batch)
    log_fin("hecho_solicitud_servicios", "load", inserted, "OK")
    print(f"[HECHO_SOLICITUD_SERVICIOS] insertados {inserted}.")

def main():
    print("== ETL DW - Inicio ==")
    load_to_staging()
    populate_dim_fecha()
    build_df_aseguradoras()
    build_df_pacientes()
    build_df_equipos()
    build_df_medicamentos()
    build_df_pedido()
    load_hecho_equipos()
    load_hecho_solicitud_servicios()
    print("== ETL DW - FIN ==")

if __name__ == "__main__":
    main()
    