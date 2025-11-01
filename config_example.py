"""
Configuración de conexión a la base de datos
IMPORTANTE: Copiar este archivo a database_config.py y actualizar con tus credenciales
           database_config.py está en .gitignore por seguridad
"""
import os
from sqlalchemy import create_engine

# ==============================
# CONFIGURACIÓN DE BASE DE DATOS
# ==============================
# Opción 1: Variables de entorno (recomendado para producción)
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'your_password_here')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'dw_HHCC')

# Opción 2: Hardcodeado (solo para desarrollo local)
# DESCOMENTAR Y EDITAR ESTAS LÍNEAS:
# DB_USER = 'postgres'
# DB_PASS = 'tu_contraseña_aquí'
# DB_HOST = 'localhost'
# DB_PORT = '5432'
# DB_NAME = 'dw_HHCC'

# ==============================
# STRING DE CONEXIÓN
# ==============================
CONN_STR = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# ==============================
# ESQUEMAS
# ==============================
SCHEMA_STG = 'stg'
SCHEMA_DW = 'dw_hhcc'

# ==============================
# FUNCIONES DE CONEXIÓN
# ==============================
def get_engine():
    """
    Retorna una instancia de SQLAlchemy engine
    
    Returns:
        Engine: SQLAlchemy engine configurado
    """
    return create_engine(
        CONN_STR, 
        client_encoding='utf8', 
        echo=False,  # Cambiar a True para ver SQL generado
        pool_pre_ping=True  # Verifica conexión antes de usar
    )

def test_connection():
    """
    Prueba la conexión a la base de datos
    
    Returns:
        bool: True si la conexión es exitosa
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute("SELECT version()")
            version = result.fetchone()[0]
            print("✅ Conexión exitosa a PostgreSQL")
            print(f"   Versión: {version[:50]}...")
            return True
    except Exception as e:
        print(f"❌ Error de conexión: {str(e)}")
        print("\nVerifica:")
        print("  1. PostgreSQL está corriendo")
        print("  2. Credenciales son correctas")
        print("  3. Base de datos existe")
        print("  4. Usuario tiene permisos")
        return False

# ==============================
# PRUEBA DE CONEXIÓN
# ==============================
if __name__ == "__main__":
    print("Probando conexión a la base de datos...")
    print(f"Host: {DB_HOST}:{DB_PORT}")
    print(f"Database: {DB_NAME}")
    print(f"User: {DB_USER}")
    print("-" * 50)
    test_connection()
