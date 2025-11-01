"""
ETL para Carga de Dimensiones
Autor: Data Team
Descripción: Carga inicial y actualización de todas las dimensiones del DW
"""
import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

# Agregar el directorio config al path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.database_config import get_engine, SCHEMA_DW

# ==============================
# RUTAS DE ARCHIVOS
# ==============================
# IMPORTANTE: Ajustar estas rutas según tu estructura de carpetas
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

RUTAS = {
    'aseguradoras': os.path.join(DATA_DIR, 'Aseguradora y Capita.xlsx'),
    'pacientes': os.path.join(DATA_DIR, 'Pacientes.xlsx'),
    'equipos': os.path.join(DATA_DIR, 'Maestro Equipos.csv'),
    'medicamentos': os.path.join(DATA_DIR, 'Maestro Medicamentos.csv'),
    'insumos': os.path.join(DATA_DIR, 'Maestro Insumos Medicos.csv'),
    'pedidos': os.path.join(DATA_DIR, 'Pedidos Solicitados.csv'),
    'reporte': os.path.join(DATA_DIR, 'Reporte Equipos.csv')
}

# ==============================
# FUNCIONES DE LECTURA
# ==============================
def leer_archivos():
    """Lee todos los archivos fuente"""
    print("📂 Leyendo archivos fuente...")
    
    archivos = {}
    
    try:
        # Excel files
        archivos['aseguradoras'] = pd.read_excel(
            RUTAS['aseguradoras'], 
            usecols=['Aseguradora', 'Codigo Sistema'], 
            engine='openpyxl'
        )
        
        archivos['pacientes'] = pd.read_excel(
            RUTAS['pacientes'],
            usecols=['Identificacion', 'Nombre', 'Municipio', 
                    'Nombre Estado', 'Aseguradora', 'Zona', 'Fecha Ingreso'],
            engine='openpyxl'
        )
        
        # CSV files
        archivos['equipos'] = pd.read_csv(
            RUTAS['equipos'], 
            sep=';', 
            engine='python', 
            encoding='latin1'
        )
        
        archivos['medicamentos'] = pd.read_csv(
            RUTAS['medicamentos'], 
            sep=None, 
            engine='python', 
            encoding='latin1'
        )
        
        archivos['insumos'] = pd.read_csv(
            RUTAS['insumos'], 
            sep=None, 
            engine='python', 
            encoding='latin1'
        )
        
        archivos['pedidos'] = pd.read_csv(
            RUTAS['pedidos'], 
            sep=None, 
            engine='python', 
            encoding='latin1'
        )
        
        archivos['reporte'] = pd.read_csv(
            RUTAS['reporte'], 
            sep=',', 
            encoding='utf-8-sig'
        )
        
        # Limpiar nombres de columnas
        for key in archivos:
            archivos[key].columns = [c.strip() for c in archivos[key].columns]
        
        print("✅ Archivos leídos correctamente")
        return archivos
        
    except FileNotFoundError as e:
        print(f"❌ Error: No se encontró el archivo {e.filename}")
        print(f"   Verifica que los archivos estén en: {DATA_DIR}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error leyendo archivos: {str(e)}")
        sys.exit(1)

# ==============================
# CARGA DE STAGING
# ==============================
def cargar_staging(archivos, engine):
    """Carga datos en tablas de staging"""
    print("\n📥 Cargando staging...")
    
    try:
        archivos['equipos'].to_sql(
            'stg_maestro_equipos', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        archivos['pacientes'].to_sql(
            'stg_maestro_pacientes', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        archivos['aseguradoras'].to_sql(
            'stg_maestro_aseguradoras', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        archivos['reporte'].to_sql(
            'stg_reporte_equipos', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        archivos['medicamentos'].to_sql(
            'stg_maestro_medicamentos', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        archivos['insumos'].to_sql(
            'stg_maestro_insumos', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        archivos['pedidos'].to_sql(
            'stg_pedidos', 
            engine, 
            schema='stg', 
            if_exists='replace', 
            index=False
        )
        
        print("✅ Staging cargado correctamente")
        
    except Exception as e:
        print(f"❌ Error cargando staging: {str(e)}")
        raise

# ==============================
# POBLACIÓN DE DIMENSIONES
# ==============================
def poblar_dim_aseguradora(conn):
    """Carga dim_aseguradora"""
    print("\n🔄 Poblando dim_aseguradora...")
    
    df_asg = pd.read_sql_table('stg_maestro_aseguradoras', schema='stg', con=conn)
    contador = 0
    
    for _, r in df_asg.iterrows():
        params = {
            'aseguradora_nk': str(r.get('Codigo Sistema', '')).strip(),
            'aseguradora': str(r.get('Aseguradora', '')).strip()
        }
        
        # Verificar si existe
        res = conn.execute(
            text("SELECT aseguradora_id FROM dw_HHCC.dim_aseguradora WHERE aseguradora_nk = :aseguradora_nk"), 
            params
        ).fetchone()
        
        if res:
            # Actualizar
            conn.execute(
                text("UPDATE dw_HHCC.dim_aseguradora SET aseguradora=:aseguradora WHERE aseguradora_nk=:aseguradora_nk"), 
                params
            )
        else:
            # Insertar
            conn.execute(
                text("""
                    INSERT INTO dw_HHCC.dim_aseguradora 
                    (aseguradora_nk, aseguradora, vigente_desde, es_actual) 
                    VALUES (:aseguradora_nk, :aseguradora, CURRENT_DATE, TRUE)
                """), 
                params
            )
            contador += 1
    
    print(f"✅ dim_aseguradora: {contador} registros nuevos")

def poblar_dim_paciente(conn):
    """Carga dim_paciente"""
    print("\n🔄 Poblando dim_paciente...")
    
    df_pac = pd.read_sql_table('stg_maestro_pacientes', schema='stg', con=conn)
    contador = 0
    
    for _, r in df_pac.iterrows():
        params = {
            'documento_paciente': str(r['Identificacion']).strip(),
            'nombre': r.get('Nombre'),
            'municipio': r.get('Municipio'),
            'estado': r.get('Nombre Estado'),
            'aseguradora': r.get('Aseguradora'),
            'zona': r.get('Zona'),
            'fecha_ingreso': r.get('Fecha Ingreso')
        }
        
        res = conn.execute(
            text("SELECT paciente_id FROM dw_HHCC.dim_paciente WHERE documento_paciente = :documento_paciente"), 
            {'documento_paciente': params['documento_paciente']}
        ).fetchone()
        
        if res:
            conn.execute(text("""
                UPDATE dw_HHCC.dim_paciente
                SET nombre=:nombre, municipio=:municipio, estado=:estado, 
                    aseguradora=:aseguradora, zona=:zona, fecha_ingreso=:fecha_ingreso
                WHERE documento_paciente=:documento_paciente
            """), params)
        else:
            conn.execute(text("""
                INSERT INTO dw_HHCC.dim_paciente 
                (documento_paciente, nombre, municipio, estado, aseguradora, zona, 
                 fecha_ingreso, vigente_desde, es_actual)
                VALUES (:documento_paciente, :nombre, :municipio, :estado, 
                        :aseguradora, :zona, :fecha_ingreso, CURRENT_DATE, TRUE)
            """), params)
            contador += 1
    
    print(f"✅ dim_paciente: {contador} registros nuevos")

def poblar_dim_equipo_scd2(conn):
    """Carga dim_equipo con SCD Tipo 2"""
    print("\n🔄 Poblando dim_equipo (SCD Tipo 2)...")
    
    df_equipo = pd.read_sql_table('stg_maestro_equipos', schema='stg', con=conn)
    
    # Normalizar
    df_equipo['equipo_nk'] = df_equipo['Código Interno'].astype(str).str.strip()
    df_equipo['equipo'] = df_equipo['Nombre Equipo'].astype(str).str.strip()
    df_equipo['estado_equipo'] = df_equipo['EQUIPO ACTIVO'].astype(str).str.strip()
    
    contador = 0
    
    for _, row in df_equipo[['equipo_nk', 'equipo', 'estado_equipo']].drop_duplicates().iterrows():
        params = {
            'equipo_nk': row['equipo_nk'], 
            'equipo': row['equipo'], 
            'estado_equipo': row['estado_equipo']
        }
        
        # Cerrar versión anterior si cambió
        conn.execute(text("""
            UPDATE dw_HHCC.dim_equipo
            SET es_actual = FALSE, vigente_hasta = CURRENT_DATE
            WHERE equipo_nk = :equipo_nk AND es_actual = TRUE
            AND (TRIM(UPPER(equipo)) <> TRIM(UPPER(:equipo)) 
                 OR TRIM(UPPER(estado_equipo)) <> TRIM(UPPER(:estado_equipo)))
        """), params)
        
        # Insertar nueva versión si no existe
        conn.execute(text("""
            INSERT INTO dw_HHCC.dim_equipo (equipo_nk, equipo, estado_equipo, vigente_desde, es_actual)
            SELECT :equipo_nk, :equipo, :estado_equipo, CURRENT_DATE, TRUE
            WHERE NOT EXISTS (
                SELECT 1 FROM dw_HHCC.dim_equipo
                WHERE equipo_nk = :equipo_nk AND es_actual = TRUE
                AND TRIM(UPPER(equipo)) = TRIM(UPPER(:equipo))
                AND TRIM(UPPER(estado_equipo)) = TRIM(UPPER(:estado_equipo))
            )
        """), params)
        contador += 1
    
    print(f"✅ dim_equipo: {contador} registros procesados")

def poblar_dim_pedido(conn):
    """Carga dim_pedido"""
    print("\n🔄 Poblando dim_pedido...")
    
    df_pedido = pd.read_sql_table('stg_pedidos', schema='stg', con=conn)
    
    # Normalizar
    df_pedido.rename(columns={
        'Numero Pedido': 'numero_pedido',
        'Insumo Solicitado': 'insumo_solicitado',
        'Cantidad': 'cantidad'
    }, inplace=True)
    
    df_pedido['insumo_solicitado'] = df_pedido['insumo_solicitado'].astype(str).str.strip().str[:255]
    df_pedido['numero_pedido'] = df_pedido['numero_pedido'].astype(str).str.strip()
    df_pedido['cantidad'] = pd.to_numeric(df_pedido['cantidad'], errors='coerce').fillna(0)
    
    contador = 0
    
    for _, row in df_pedido.iterrows():
        insert_sql = """
        INSERT INTO dw_hhcc.dim_pedido (numero_pedido, insumo_solicitado, cantidad)
        VALUES (:numero_pedido, :insumo_solicitado, :cantidad)
        ON CONFLICT (numero_pedido) DO UPDATE
        SET insumo_solicitado = EXCLUDED.insumo_solicitado,
            cantidad = EXCLUDED.cantidad
        """
        conn.execute(text(insert_sql), row.to_dict())
        contador += 1
    
    print(f"✅ dim_pedido: {contador} registros procesados")

def poblar_dim_medicamento(conn):
    """Carga dim_medicamento"""
    print("\n🔄 Poblando dim_medicamento...")
    
    df_med = pd.read_sql_table('stg_maestro_medicamentos', schema='stg', con=conn)
    
    # Normalizar
    df_med['nombre'] = df_med['nombre'].astype(str).str.strip().str[:255]
    df_med['forma_farmaceutica'] = df_med['forma_farmaceutica'].astype(str).str.strip().str[:100]
    df_med['via_administracion'] = df_med['via_administracion'].astype(str).str.strip().str[:100]
    
    contador = 0
    
    for _, row in df_med.iterrows():
        insert_sql = """
        INSERT INTO dw_hhcc.dim_medicamento (codigo, nombre, forma_farmaceutica, via_administracion)
        VALUES (:codigo, :nombre, :forma_farmaceutica, :via_administracion)
        ON CONFLICT (codigo) DO UPDATE
        SET nombre = EXCLUDED.nombre,
            forma_farmaceutica = EXCLUDED.forma_farmaceutica,
            via_administracion = EXCLUDED.via_administracion
        """
        conn.execute(text(insert_sql), row.to_dict())
        contador += 1
    
    print(f"✅ dim_medicamento: {contador} registros procesados")

# ==============================
# MAIN
# ==============================
def main():
    """Ejecuta el ETL completo de dimensiones"""
    print("="*60)
    print("ETL DE DIMENSIONES")
    print("="*60)
    
    try:
        # Obtener engine
        engine = get_engine()
        
        # Leer archivos
        archivos = leer_archivos()
        
        # Cargar staging
        cargar_staging(archivos, engine)
        
        # Poblar dimensiones
        with engine.begin() as conn:
            poblar_dim_aseguradora(conn)
            poblar_dim_paciente(conn)
            poblar_dim_equipo_scd2(conn)
            poblar_dim_pedido(conn)
            poblar_dim_medicamento(conn)
        
        print("\n" + "="*60)
        print("✅ ETL DE DIMENSIONES COMPLETADO")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
