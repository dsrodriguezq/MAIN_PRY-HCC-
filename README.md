📦 Estructura Completa del Proyecto
Árbol de Directorios
dw-medical-equipment/
│
├── README.md                          # Documentación principal
├── LICENSE                            # Licencia MIT
├── CONTRIBUTING.md                    # Guía de contribución
├── .gitignore                         # Archivos ignorados por Git
├── .env.example                       # Plantilla de variables de entorno
├── requirements.txt                   # Dependencias Python
├── setup.py                           # Script de configuración inicial
│
├── config/                            # Configuración
│   ├── database_config.py.example    # Plantilla de configuración DB
│   └── database_config.py            # Configuración real (no versionado)
│
├── sql/                               # Scripts SQL
│   ├── 01_create_schemas.sql         # Creación de esquemas
│   ├── 02_create_staging_tables.sql  # Tablas de staging
│   ├── 03_create_dimension_tables.sql # Tablas dimensionales
│   ├── 04_create_fact_tables.sql     # Tablas de hechos
│   └── 05_queries_validation.sql     # Consultas de validación
│
├── etl/                               # Scripts ETL Python
│   ├── __init__.py                   # Módulo Python
│   ├── etl_dimensions.py             # ETL de dimensiones
│   ├── etl_fact_equipos.py          # ETL de hechos equipos
│   └── etl_fact_servicios.py        # ETL de hechos servicios
│
├── docs/                              # Documentación técnica
│   ├── ARCHITECTURE.md               # Arquitectura del sistema
│   ├── DATA_MODEL.md                 # Modelo de datos detallado
│   └── ETL_PROCESS.md                # Procesos ETL
│
├── data/                              # Datos (no versionado)
│   ├── README.md                     # Instrucciones de datos
│   └── sample/                       # Datos de ejemplo
│       └── .gitkeep
│
├── logs/                              # Logs de ejecución (no versionado)
│   └── .gitkeep
│
└── tests/                             # Tests (futuro)
    ├── __init__.py
    ├── unit/
    └── integration/
📄 Descripción de Archivos Clave
Raíz del Proyecto
Archivo	Descripción	Versionado
README.md	Documentación principal del proyecto	✅
LICENSE	Licencia MIT del proyecto	✅
CONTRIBUTING.md	Guía para contribuidores	✅
.gitignore	Archivos/directorios ignorados por Git	✅
.env.example	Plantilla de variables de entorno	✅
.env	Variables de entorno reales	❌
requirements.txt	Dependencias Python	✅
setup.py	Script de configuración automática	✅
Directorio config/
Archivo	Descripción	Versionado
database_config.py.example	Plantilla de configuración DB	✅
database_config.py	Credenciales reales de BD	❌
Directorio sql/
Archivo	Orden	Descripción
01_create_schemas.sql	1	Crea esquemas stg y dw_HHCC
02_create_staging_tables.sql	2	Crea tablas de staging
03_create_dimension_tables.sql	3	Crea dimensiones del modelo
04_create_fact_tables.sql	4	Crea tablas de hechos
05_queries_validation.sql	5	Consultas de validación y análisis
Directorio etl/
Archivo	Propósito	Orden Ejecución
etl_dimensions.py	Carga de todas las dimensiones	1
etl_fact_equipos.py	Carga de hechos de equipos	2
etl_fact_servicios.py	Carga de hechos de servicios	3
Directorio docs/
Archivo	Contenido
ARCHITECTURE.md	Diseño del DW, capas, patrones
DATA_MODEL.md	Diccionario de datos, relaciones
ETL_PROCESS.md	Flujo ETL detallado, troubleshooting
Directorio data/
NO versionado (contiene datos sensibles)
Estructura documentada en data/README.md
Archivos esperados:
Excel: Aseguradoras, Pacientes
CSV: Equipos, Medicamentos, Reportes, etc.
🔐 Archivos Sensibles (.gitignore)
# Configuración con credenciales
config/database_config.py
.env

# Datos
data/*.csv
data/*.xlsx
data/*.xls

# Logs
logs/
*.log

# Python
__pycache__/
*.pyc
.venv/
📋 Checklist de Configuración Inicial
1. Clonar Repositorio
git clone https://github.com/tu-usuario/dw-medical-equipment.git
cd dw-medical-equipment
2. Configurar Entorno Python
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate     # Windows

pip install -r requirements.txt
3. Configurar Base de Datos
# Copiar plantillas
cp .env.example .env
cp config/database_config.py.example config/database_config.py

# Editar con tus credenciales
nano .env
nano config/database_config.py
4. Crear Estructura de BD
psql -U postgres -d dw_HHCC -f sql/01_create_schemas.sql
psql -U postgres -d dw_HHCC -f sql/02_create_staging_tables.sql
psql -U postgres -d dw_HHCC -f sql/03_create_dimension_tables.sql
psql -U postgres -d dw_HHCC -f sql/04_create_fact_tables.sql
5. Colocar Datos
# Copiar archivos CSV/Excel a data/
cp /path/to/files/*.csv data/
cp /path/to/files/*.xlsx data/
6. Ejecutar ETL
python etl/etl_dimensions.py
python etl/etl_fact_equipos.py
python etl/etl_fact_servicios.py
7. Validar
psql -U postgres -d dw_HHCC -f sql/05_queries_validation.sql
🚀 Script de Setup Automático
# Ejecutar configuración automática
python setup.py
Este script:

✅ Verifica Python 3.8+
✅ Verifica PostgreSQL
✅ Instala dependencias
✅ Crea estructura de directorios
✅ Genera archivos de configuración
✅ Prueba conexión a BD
✅ Muestra próximos pasos
📊 Tamaño Estimado del Proyecto
Código SQL:         ~2.5 KB
Código Python:      ~15 KB
Documentación:      ~45 KB
Archivos config:    ~3 KB
Total (sin datos):  ~65 KB

Con datos:          Variable (100 MB - 1 GB típico)
🔄 Flujo de Trabajo
Desarrollo Local
# 1. Crear rama
git checkout -b feature/nueva-feature

# 2. Hacer cambios
# ... editar archivos ...

# 3. Probar
python etl/etl_dimensions.py

# 4. Commit
git add .
git commit -m "feat(etl): agregar nueva feature"

# 5. Push
git push origin feature/nueva-feature

# 6. Crear Pull Request en GitHub
Actualización de Datos
# 1. Actualizar archivos en data/
# 2. Ejecutar ETL correspondiente
python etl/etl_fact_equipos.py

# 3. Validar resultados
psql -U postgres -d dw_HHCC -f sql/05_queries_validation.sql
🛠️ Mantenimiento
Backups
# Backup completo
pg_dump -U postgres dw_HHCC > backup_$(date +%Y%m%d).sql

# Backup solo datos
pg_dump -U postgres --data-only dw_HHCC > data_backup_$(date +%Y%m%d).sql
Limpieza
# Limpiar staging
psql -U postgres -d dw_HHCC -c "TRUNCATE TABLE stg.stg_reporte_equipos;"

# Limpiar logs antiguos
find logs/ -name "*.log" -mtime +30 -delete
Actualización de Dependencias
pip list --outdated
pip install --upgrade pandas sqlalchemy
pip freeze > requirements.txt
📚 Recursos Adicionales
Documentación PostgreSQL: https://www.postgresql.org/docs/
Pandas Docs: https://pandas.pydata.org/docs/
SQLAlchemy Docs: https://docs.sqlalchemy.org/
PEP 8 Style Guide: https://pep8.org/
🤝 Contribuir
Ver CONTRIBUTING.md para guías detalladas.

📧 Soporte
Issues: https://github.com/tu-usuario/dw-medical-equipment/issues
Email: tu-email@ejemplo.com
Documentación: docs/
Última actualización: 2024
