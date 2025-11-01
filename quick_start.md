# ⚡ Quick Start - Inicio Rápido

Guía de 5 minutos para poner en marcha el Data Warehouse.

## 📋 Prerrequisitos

- [x] PostgreSQL 12+ instalado
- [x] Python 3.8+ instalado
- [x] Git instalado

## 🚀 Pasos Rápidos

### 1️⃣ Clonar e Instalar (2 min)

```bash
# Clonar repositorio
git clone https://github.com/tu-usuario/dw-medical-equipment.git
cd dw-medical-equipment

# Instalar dependencias
pip install -r requirements.txt
```

### 2️⃣ Configurar Base de Datos (1 min)

```bash
# Crear base de datos
createdb dw_HHCC

# Copiar y editar configuración
cp config/database_config.py.example config/database_config.py

# Editar con tus credenciales (nano, vim, o tu editor)
nano config/database_config.py
```

**Mínimo a configurar en `database_config.py`**:
```python
DB_USER = 'postgres'
DB_PASS = 'tu_contraseña'  # ⚠️ CAMBIAR ESTO
DB_HOST = 'localhost'
DB_NAME = 'dw_HHCC'
```

### 3️⃣ Crear Estructura (1 min)

```bash
# Ejecutar scripts SQL en orden
psql -U postgres -d dw_HHCC << EOF
\i sql/01_create_schemas.sql
\i sql/02_create_staging_tables.sql
\i sql/03_create_dimension_tables.sql
\i sql/04_create_fact_tables.sql
EOF
```

**O uno por uno**:
```bash
psql -U postgres -d dw_HHCC -f sql/01_create_schemas.sql
psql -U postgres -d dw_HHCC -f sql/02_create_staging_tables.sql
psql -U postgres -d dw_HHCC -f sql/03_create_dimension_tables.sql
psql -U postgres -d dw_HHCC -f sql/04_create_fact_tables.sql
```

### 4️⃣ Preparar Datos (< 1 min)

```bash
# Copiar tus archivos CSV/Excel a data/
cp /ruta/a/tus/archivos/*.csv data/
cp /ruta/a/tus/archivos/*.xlsx data/

# Verificar que estén todos
ls -lh data/
```

**Archivos necesarios**:
- ✅ Aseguradora y Capita.xlsx
- ✅ Pacientes.xlsx
- ✅ Maestro Equipos.csv
- ✅ Maestro Medicamentos.csv
- ✅ Maestro Insumos Medicos.csv
- ✅ Pedidos Solicitados.csv
- ✅ Reporte Equipos.csv
- ✅ Insumos Solicitados Histórico Actualizado.csv

### 5️⃣ Ejecutar ETL (2-10 min según volumen)

```bash
# Cargar dimensiones
python etl/etl_dimensions.py

# Cargar hechos de equipos
python etl/etl_fact_equipos.py

# Cargar hechos de servicios
python etl/etl_fact_servicios.py
```

### 6️⃣ Validar (< 1 min)

```bash
# Ejecutar consultas de validación
psql -U postgres -d dw_HHCC -f sql/05_queries_validation.sql

# O conectarse y explorar
psql -U postgres -d dw_HHCC
```

```sql
-- Verificar conteos
SELECT 'dim_equipo', COUNT(*) FROM dw_HHCC.dim_equipo
UNION ALL
SELECT 'dim_paciente', COUNT(*) FROM dw_HHCC.dim_paciente
UNION ALL
SELECT 'hecho_equipos', COUNT(*) FROM dw_HHCC.hecho_equipos;
```

## ✅ ¡Listo!

Tu Data Warehouse está funcionando. Ahora puedes:

### Consultas de Ejemplo

```sql
-- Equipos por mes
SELECT 
    f.anio, 
    f.mes,
    e.equipo, 
    SUM(h.cantidad_equipos) AS total
FROM dw_HHCC.hecho_equipos h
JOIN dw_HHCC.dim_equipo e ON h.equipo_id = e.equipo_id
JOIN dw_HHCC.dim_fecha f ON h.fecha_solicitud_id = f.fecha_id
WHERE e.es_actual = TRUE
GROUP BY f.anio, f.mes, e.equipo
ORDER BY f.anio DESC, f.mes DESC
LIMIT 20;
```

### Conectar BI Tools

**Power BI / Tableau / Metabase**:
```
Host: localhost
Port: 5432
Database: dw_HHCC
Schema: dw_hhcc
User: postgres
Password: [tu_contraseña]
```

## 🔧 Troubleshooting Rápido

### Problema: Error de conexión

```bash
# Verificar que PostgreSQL esté corriendo
sudo systemctl status postgresql  # Linux
brew services list                # Mac
# Buscar "Services" en Windows

# Probar conexión
psql -U postgres -c "SELECT 1"
```

### Problema: Archivo no encontrado

```bash
# Verificar rutas en etl_dimensions.py
# Línea ~20: DATA_DIR = ...

# O copiar archivos a la ubicación correcta
mkdir -p data
cp /ruta/origen/*.csv data/
```

### Problema: Error en ETL

```bash
# Ver último error
tail -n 50 logs/etl.log  # si existe

# O ejecutar con verbose
python etl/etl_dimensions.py 2>&1 | tee output.log
```

### Problema: Registros sin cargar

```sql
-- Verificar staging
SELECT COUNT(*) FROM stg.stg_reporte_equipos;

-- Verificar dimensiones
SELECT COUNT(*) FROM dw_HHCC.dim_equipo WHERE es_actual = TRUE;

-- Ver registros huérfanos
SELECT * FROM dw_HHCC.hecho_equipos 
WHERE equipo_id IS NULL 
   OR paciente_id IS NULL
LIMIT 10;
```

## 📚 Siguiente Nivel

Una vez funcionando, explorar:

1. **Documentación completa**: [README.md](README.md)
2. **Arquitectura**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
3. **Modelo de datos**: [docs/DATA_MODEL.md](docs/DATA_MODEL.md)
4. **Procesos ETL**: [docs/ETL_PROCESS.md](docs/ETL_PROCESS.md)

## 🆘 Ayuda

- 📖 Documentación: [docs/](docs/)
- 🐛 Reportar bug: [Issues](../../issues)
- 💬 Preguntas: [Discussions](../../discussions)
- 📧 Email: tu-email@ejemplo.com

## 🎯 Comandos Útiles

```bash
# Re-ejecutar todo desde cero
psql -U postgres -c "DROP DATABASE IF EXISTS dw_HHCC"
psql -U postgres -c "CREATE DATABASE dw_HHCC"
# ... repetir pasos 3-5

# Backup rápido
pg_dump -U postgres dw_HHCC > backup.sql

# Restaurar backup
psql -U postgres -d dw_HHCC < backup.sql

# Ver logs de PostgreSQL
tail -f /var/log/postgresql/postgresql-*.log  # Linux
```

---

⏱️ **Tiempo total**: ~7-15 minutos

🎉 **¡Felicitaciones!** Tu DW está listo para análisis.
