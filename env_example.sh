# =====================================================
# CONFIGURACIÓN DE BASE DE DATOS
# =====================================================
# IMPORTANTE: Copiar este archivo a .env y actualizar con tus credenciales
#             .env está en .gitignore por seguridad

# Credenciales PostgreSQL
DB_USER=postgres
DB_PASS=your_secure_password_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dw_HHCC

# =====================================================
# CONFIGURACIÓN DE ENTORNO
# =====================================================
# Opciones: development, production, testing
ENVIRONMENT=development

# Habilitar logs detallados (True/False)
DEBUG_MODE=False

# =====================================================
# RUTAS DE DATOS (opcional)
# =====================================================
# Si tus archivos están en una ubicación diferente
# DATA_DIR=/ruta/a/tus/datos

# =====================================================
# CONFIGURACIÓN ETL (opcional)
# =====================================================
# Tamaño de lote para inserciones masivas
BATCH_SIZE=1000

# Número de reintentos en caso de error
MAX_RETRIES=3

# Timeout de conexión (segundos)
CONNECTION_TIMEOUT=30
