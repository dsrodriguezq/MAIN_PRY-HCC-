# etl_03_dimensiones.py
import os, re
import pandas as pd
from unidecode import unidecode
import psycopg2
from psycopg2.extras import execute_values

DB = dict(
    host=os.getenv("PGHOST","localhost"),
    port=int(os.getenv("PGPORT","5432")),
    user=os.getenv("PGUSER","postgres"),
    password=os.getenv("PGPASSWORD","postgres"),
    dbname=os.getenv("PGDATABASE","dw_salud"),
)

SCHEMA_STG="stg"; SCHEMA_DW="dw"

def pick(df: pd.DataFrame, pats):
    for p in pats:
        rx = re.compile(p)
        for c in df.columns:
            if rx.search(c): return c
    return None

def upsert_aseguradora():
    inserted=0
    with psycopg2.connect(**DB) as conn:
        aseg=set()
        with conn.cursor() as cur:
            cur.execute("""SELECT table_name FROM information_schema.tables 
                           WHERE table_schema=%s AND table_name LIKE 'stg_%%'""",(SCHEMA_STG,))
            for (t,) in cur.fetchall():
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
                col = pick(df,[r"asegur", r"eps", r"ars"])
                if col:
                    vals = df[col].dropna().astype(str).str.strip().str.upper().map(unidecode).unique()
                    aseg.update(vals)
        rows=[(a,a) for a in sorted(aseg) if a and a!="NAN"]
        if rows:
            sql="""INSERT INTO dw.dim_aseguradora(aseguradora_nk,aseguradora)
                   VALUES %s
                   ON CONFLICT (aseguradora_nk) DO UPDATE SET aseguradora=EXCLUDED.aseguradora"""
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
        conn.commit()
        inserted=len(rows)
    print(f"[DIM_ASEGURADORA] upsert {inserted}")

def scd2_upsert_paciente(df_src: pd.DataFrame):
    ins=0
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        for _,r in df_src.iterrows():
            nk = str(r.get("paciente_nk","")).strip()
            if not nk: continue
            cur.execute("""SELECT paciente_id,nombre,municipio,estado,aseguradora,zona,fecha_ingreso
                           FROM dw.dim_paciente WHERE paciente_nk=%s AND es_actual=TRUE""",(nk,))
            row = cur.fetchone()
            changed=False
            if row:
                (pid,nombre,municipio,estado,aseguradora,zona,fecha_ingreso)=row
                for col in ["nombre","municipio","estado","aseguradora","zona"]:
                    if str(r.get(col) or "").strip()!=str(locals()[col] or "").strip():
                        changed=True; break
                if not changed and str(r.get("fecha_ingreso") or "")!=str(fecha_ingreso or ""):
                    changed=True
                if changed:
                    cur.execute("""UPDATE dw.dim_paciente SET vigente_hasta=now(), es_actual=FALSE
                                   WHERE paciente_id=%s AND es_actual=TRUE""",(pid,))
            if (not row) or changed:
                cur.execute("""INSERT INTO dw.dim_paciente(paciente_nk,nombre,municipio,estado,aseguradora,zona,fecha_ingreso)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            (nk,r.get("nombre"),r.get("municipio"),r.get("estado"),
                             r.get("aseguradora"),r.get("zona"),r.get("fecha_ingreso")))
                ins+=1
        conn.commit()
    print(f"[DIM_PACIENTE] upsert/SCD2 insertados {ins}")

def scd2_upsert_equipo(df_src: pd.DataFrame):
    ins=0
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        for _,r in df_src.iterrows():
            nk=str(r.get("equipo_nk","")).strip()
            if not nk: continue
            cur.execute("""SELECT equipo_id,equipo,estado_equipo FROM dw.dim_equipo
                           WHERE equipo_nk=%s AND es_actual=TRUE""",(nk,))
            row=cur.fetchone()
            changed=False
            if row:
                (eid,equipo,estado_equipo)=row
                for col in ["equipo","estado_equipo"]:
                    if str(r.get(col) or "").strip()!=str(locals()[col] or "").strip():
                        changed=True; break
                if changed:
                    cur.execute("""UPDATE dw.dim_equipo SET vigente_hasta=now(), es_actual=FALSE
                                   WHERE equipo_id=%s AND es_actual=TRUE""",(eid,))
            if (not row) or changed:
                cur.execute("""INSERT INTO dw.dim_equipo(equipo_nk,equipo,estado_equipo)
                               VALUES (%s,%s,%s)""",(nk,r.get("equipo"),r.get("estado_equipo")))
                ins+=1
        conn.commit()
    print(f"[DIM_EQUIPO] upsert/SCD2 insertados {ins}")

def upsert_medicamento(df_src: pd.DataFrame):
    rows=[]
    for _,r in df_src.iterrows():
        nk=str(r.get("medicamento_nk","")).strip()
        if not nk: continue
        rows.append((nk,r.get("nombre"),r.get("forma_farmaceutica"),r.get("via_administracion")))
    if not rows:
        print("[DIM_MEDICAMENTO] no hay filas")
        return
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dw.dim_medicamento(medicamento_nk,nombre,forma_farmaceutica,via_administracion)
            VALUES %s
            ON CONFLICT (medicamento_nk) DO UPDATE
            SET nombre=EXCLUDED.nombre, forma_farmaceutica=EXCLUDED.forma_farmaceutica, via_administracion=EXCLUDED.via_administracion
        """, rows)
        conn.commit()
    print(f"[DIM_MEDICAMENTO] upsert {len(rows)}")

def upsert_pedido(df_src: pd.DataFrame):
    rows=[]
    for _,r in df_src.iterrows():
        nk=str(r.get("numero_pedido_nk","")).strip()
        if not nk: continue
        rows.append((nk,r.get("insumo_solicitado"),r.get("cantidad")))
    if not rows:
        print("[DIM_PEDIDO] no hay filas")
        return
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dw.dim_pedido(numero_pedido_nk,insumo_solicitado,cantidad)
            VALUES %s
            ON CONFLICT (numero_pedido_nk) DO UPDATE
            SET insumo_solicitado=EXCLUDED.insumo_solicitado, cantidad=EXCLUDED.cantidad
        """, rows)
        conn.commit()
    print(f"[DIM_PEDIDO] upsert {len(rows)}")

def main():
    with psycopg2.connect(**DB) as conn:
        # 1) Aseguradoras (de cualquier stg_*)
        upsert_aseguradora()

        # 2) Pacientes (SCD2)
        rows={}
        with conn.cursor() as cur:
            cur.execute("""SELECT table_name FROM information_schema.tables 
                           WHERE table_schema=%s AND table_name LIKE 'stg_%%'""",(SCHEMA_STG,))
            for (t,) in cur.fetchall():
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
                col_id = pick(df,[r"(docu|id).*pac", r"paciente.*(id|doc)", r"identificacion", r"historia"]) or pick(df,[r"nombre"])
                col_nombre = pick(df,[r"nombre", r"paciente"])
                col_mun = pick(df,[r"munic", r"ciudad"])
                col_est = pick(df,[r"depto|depart|estado"])
                col_zona = pick(df,[r"zona|barrio"])
                col_aseg = pick(df,[r"asegur|eps|ars"])
                col_fing = pick(df,[r"fecha.*ingreso", r"ingreso"])
                for _,r in df.iterrows():
                    nk = str(r.get(col_id) or "").strip()
                    if not nk: continue
                    rows[nk] = dict(
                        paciente_nk = nk,
                        nombre      = str(r.get(col_nombre) or "").strip(),
                        municipio   = str(r.get(col_mun) or "").strip(),
                        estado      = str(r.get(col_est) or "").strip(),
                        aseguradora = str(r.get(col_aseg) or "").strip(),
                        zona        = str(r.get(col_zona) or "").strip(),
                        fecha_ingreso = r.get(col_fing)
                    )
        if rows:
            scd2_upsert_paciente(pd.DataFrame(list(rows.values())))

        # 3) Equipos (SCD2)
        frames=[]
        for t in ["stg_maestro_equipos","stg_reporte_equipos","stg_equipos_entregados","stg_equipos_retirados","stg_equipos_por_entregar"]:
            try:
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
            except Exception:
                continue
            col_nk = pick(df,[r"(serial|serie|codigo|equipo).*"])
            col_nom= pick(df,[r"equipo|descripcion|nombre"])
            col_est= pick(df,[r"estado"])
            if col_nk:
                frames.append(pd.DataFrame(dict(
                    equipo_nk = df[col_nk],
                    equipo    = df[col_nom] if col_nom in df.columns else None,
                    estado_equipo = df[col_est] if col_est in df.columns else None
                )))
        if frames:
            dfe = pd.concat(frames, ignore_index=True).dropna(subset=["equipo_nk"]).drop_duplicates("equipo_nk")
            scd2_upsert_equipo(dfe)

        # 4) Medicamentos / Insumos (maestros)
        frames=[]
        for t in ["stg_maestro_medicamentos","stg_maestro_insumos_medicos"]:
            try:
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
            except Exception:
                continue
            col_nk = pick(df,[r"(codigo|sku|referen).*", r"medicamento.*(id|codigo)"])
            col_nom= pick(df,[r"(nombre|descripcion)"])
            col_for= pick(df,[r"forma"])
            col_via= pick(df,[r"via"])
            if col_nk:
                frames.append(pd.DataFrame(dict(
                    medicamento_nk = df[col_nk],
                    nombre = df[col_nom] if col_nom in df.columns else None,
                    forma_farmaceutica = df[col_for] if col_for in df.columns else None,
                    via_administracion = df[col_via] if col_via in df.columns else None
                )))
        if frames:
            dfm = pd.concat(frames, ignore_index=True).dropna(subset=["medicamento_nk"]).drop_duplicates("medicamento_nk")
            upsert_medicamento(dfm)

        # 5) Pedido (desde solicitudes)
        frames=[]
        for t in ["stg_insumos_solicitados","stg_pedidos_parciales","stg_insumos_solicitados_hist","stg_insumos_solicitados_hist_act"]:
            try:
                df = pd.read_sql(f'SELECT * FROM {SCHEMA_STG}."{t}"', conn)
            except Exception:
                continue
            col_np = pick(df,[r"(numero|num|nro).*pedido", r"pedido.*(num|numero|id)"])
            col_ins= pick(df,[r"(insumo|producto|medicamento|articulo|descripcion)"])
            col_can= pick(df,[r"(cant|unid)"])
            if col_np:
                frames.append(pd.DataFrame(dict(
                    numero_pedido_nk = df[col_np],
                    insumo_solicitado = df[col_ins] if col_ins in df.columns else None,
                    cantidad = pd.to_numeric(df[col_can], errors="coerce") if col_can in df.columns else None
                )))
        if frames:
            dfp = pd.concat(frames, ignore_index=True).dropna(subset=["numero_pedido_nk"]).drop_duplicates("numero_pedido_nk")
            upsert_pedido(dfp)

if __name__ == "__main__":
    main()
