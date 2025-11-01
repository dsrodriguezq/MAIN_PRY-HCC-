# 🔄 Proceso ETL

## Visión General

El proceso ETL (Extract, Transform, Load) está dividido en tres scripts principales que deben ejecutarse en orden:

1. **etl_dimensions.py** - Carga de dimensiones
2. **etl_fact_equipos.py** - Carga de hechos de equipos
3. **etl_fact_servicios.py** - Carga de hechos de servicios

## Flujo Detallado

### 1. ETL de Dimensiones

**Script**: `etl/etl_dimensions.py`

**Propósito**: Poblar todas las tablas dimensionales del DW.

#### Fases

##### A. Extracción (Extract)
```python
# Lectura de archivos fuente
- Maestro Equipos.csv
- Pacientes.xlsx
- Aseguradora y Capita.xlsx
- Maestro Medicamentos.csv
- Maestro Insumos Medicos.csv
- Pedidos Solicitados.csv
```

**Formatos soportados**:
- CSV (delimitadores: `;`, `,`)
- Excel (.xlsx)

##### B. Transformación (Transform)
- Normalización de nombres de columnas (strip)
- Conversión de tipos de datos
- Limpieza de valores nulos
- Generación de natural keys

##### C. Carga (Load)

1. **Carga a Staging**:
```python
df.to_sql('stg_tabla', engine, schema='stg', if_exists='replace')
```

2. **Población de Dimensiones**:

**dim_aseguradora**:
```python
- Verificar si código existe
- Si existe: UPDATE
- Si no existe: INSERT
```

**dim_paciente**:
```python
- Verificar si documento existe
- Si existe: UPDATE atributos
- Si no existe: INSERT nuevo
```

**dim_equipo (SCD Tipo 2)**:
```python
1. Comparar equipo_nk actual vs nuevo
2. Si cambió:
   - UPDATE registro anterior: es_actual=FALSE, vigente_hasta=HOY
   - INSERT nueva versión: es_actual=TRUE, vigente_desde=HOY
3. Si no cambió: no hacer nada
```

**dim_pedido**:
```python
- UPSERT usando ON CONFLICT
- Actualiza si existe, inserta si no
```

**dim_medicamento**:
```python
- UPSERT por código
```

#### Ejecución

```bash
cd etl
python etl_dimensions.py
```

**Salida esperada**:
```
📂 Leyendo archivos fuente...
✅ Archivos leídos correctamente

📥 Cargando staging...
✅ Staging cargado correctamente

🔄 Poblando dim_aseguradora...
✅ dim_aseguradora: 45 registros nuevos

🔄 Poblando dim_paciente...
✅ dim_paciente: 1523 registros nuevos

🔄 Poblando dim_equipo (SCD Tipo 2)...
✅ dim_equipo: 87 registros procesados

...

✅ ETL DE DIMENSIONES COMPLETADO
```

**Tiempo estimado**: 2-5 minutos (depende del volumen)

---

### 2. ETL de Hechos - Equipos

**Script**: `etl/etl_fact_equipos.py`

**Propósito**: Cargar transacciones de equipos médicos.

#### Fases

##### A. Preparación
1. Leer staging: `stg_reporte_equipos`
2. Limpiar y normalizar datos
3. Convertir fechas al formato correcto

##### B. Auto-Población de Dimensiones Faltantes

**dim_fecha**:
```python
- Extraer fechas únicas de transacciones
- Insertar en dim_fecha si no existen
```

**dim_paciente**:
```python
- Identificar pacientes no existentes
- Insertar con datos básicos (documento, nombre)
```

**dim_aseguradora**:
```python
- Identificar códigos faltantes
- Insertar con nombre genérico
```

##### C. Joins con Dimensiones
```python
# Normalizar keys para join
df['codigo_tipo_equipo_norm'] = df['Código Interno'].str.upper()
df['documento_norm'] = df['Documento Paciente'].str.upper()

# Merge con dimensiones
df.merge(dim_equipo, ...)
df.merge(dim_paciente, ...)
df.merge(dim_aseguradora, ...)
df.merge(dim_fecha, ...)
```

##### D. Validación
```python
# Verificar registros sin FK
sin_equipo = df[df['equipo_id'].isna()]
sin_paciente = df[df['paciente_id'].isna()]
sin_aseguradora = df[df['aseguradora_id'].isna()]
sin_fecha = df[df['fecha_id'].isna()]
```

##### E. Carga
```python
# Generar PK compuesta
solicitud_equipos_id = f"{Codigo}-{NumeroSerie}-{Fecha}"

# Insertar o actualizar
FOR cada registro:
    IF existe:
        UPDATE
    ELSE:
        INSERT
```

#### Ejecución

```bash
cd etl
python etl_fact_equipos.py
```

**Salida esperada**:
```
============================================================
CARGANDO HECHO_EQUIPOS
============================================================
Registros en staging: 4532

Poblando dim_fecha...
dim_fecha actualizada: 234 fechas nuevas insertadas ✅

Dimensiones cargadas:
  - dim_equipo: 87 registros
  - dim_paciente: 1523 registros
  - dim_aseguradora: 45 registros
  - dim_fecha: 876 registros

📊 Registros válidos: 4498/4532

✅ HECHO_EQUIPOS CARGADO:
   - Insertados: 4321
   - Actualizados: 177
   - Errores: 34

✅ PROCESO COMPLETADO
```

**Tiempo estimado**: 5-15 minutos (depende del volumen)

---

### 3. ETL de Hechos - Servicios

**Script**: `etl/etl_fact_servicios.py`

**Propósito**: Cargar solicitudes de servicios e insumos.

#### Características Especiales

##### UPSERT Masivo
Este script utiliza una técnica optimizada para grandes volúmenes:

```python
1. Crear tabla temporal
2. Cargar datos a tabla temporal (bulk insert)
3. UPSERT desde tabla temporal a tabla final
4. Eliminar tabla temporal
```

**Ventajas**:
- 10-50x más rápido que inserts individuales
- Manejo transaccional
- Menos locks en la tabla final

##### Manejo de Duplicados
```python
# Eliminar duplicados en memoria antes de cargar
df.drop_duplicates(subset=['solicitud_servicio_id'], keep='last')
```

#### Fases

##### A. Lectura de CSV
```python
df = pd.read_csv(
    'Insumos Solicitados Histórico Actualizado.csv',
    sep=None,  # Auto-detectar separador
    encoding='utf-8-sig'
)
```

##### B. Transformación
```python
# Normalizar columnas
- Servicio
- Numero de pedido
- Identificacion Paciente
- Fecha envio a logistica
- Aseguradora
- Estado del pedido

# Asignar cantidad default
df['cantidad'] = 1
```

##### C. Población de Dimensiones Auxiliares
```python
poblar_dim_fecha()
poblar_dim_pedido()  # Auto-crear pedidos faltantes
```

##### D. Carga Optimizada
```sql
-- Crear tabla temporal
CREATE TEMP TABLE temp_servicios (...)

-- Cargar con pandas (rápido)
df.to_sql('temp_servicios', ...)

-- UPSERT masivo
INSERT INTO hecho_solicitud_servicios
SELECT * FROM temp_servicios
ON CONFLICT (solicitud_servicio_id) 
DO UPDATE SET ...

-- Limpiar
DROP TABLE temp_servicios
```

#### Ejecución

```bash
cd etl
python etl_fact_servicios.py
```

**Salida esperada**:
```
============================================================
CARGANDO HECHO_SOLICITUD_SERVICIOS
============================================================

[1/6] Leyendo archivo CSV...
✓ Archivo cargado: 50450 registros

[2/6] Extrayendo de staging...
[3/6] Limpiando datos...

[4/6] Poblando dimensiones...
dim_fecha actualizada: 187 fechas nuevas insertadas ✅
dim_pedido actualizada: 4321 pedidos nuevos insertados ✅

[5/6] Haciendo joins con dimensiones...
✓ Dimensiones cargadas:
  - dim_pedido: 4321
  - dim_paciente: 1523
  - dim_aseguradora: 45
  - dim_fecha: 1063

📊 Registros válidos: 50123/50450

[6/6] Cargando datos (usando UPSERT masivo)...
  → Registros antes de eliminar duplicados: 50123
  → Registros después de eliminar duplicados: 49876
  → Creando tabla temporal...
  → Insertando 49876 registros a tabla temporal...
  → Ejecutando UPSERT masivo...

✅ HECHO_SOLICITUD_SERVICIOS CARGADO:
   - Total procesado: 49876 registros
   - Operación: UPSERT masivo exitoso

⏱️  Tiempo total: 23.45 segundos

✅ PROCESO COMPLETADO
```

**Tiempo estimado**: 20-60 segundos (con UPSERT masivo)

---

## Orden de Ejecución

### Carga Inicial (Primera vez)

```bash
# 1. Crear estructura de BD
psql -U postgres -d dw_HHCC -f sql/01_create_schemas.sql
psql -U postgres -d dw_HHCC -f sql/02_create_staging_tables.sql
psql -U postgres -d dw_HHCC -f sql/03_create_dimension_tables.sql
psql -U postgres -d dw_HHCC -f sql/04_create_fact_tables.sql

# 2. Ejecutar ETL
python etl/etl_dimensions.py
python etl/etl_fact_equipos.py
python etl/etl_fact_servicios.py

# 3. Validar
psql -U postgres -d dw_HHCC -f sql/05_queries_validation.sql
```

### Carga Incremental (Actualización)

```bash
# Solo ejecutar ETLs con archivos actualizados
python etl/etl_dimensions.py       # Si hay cambios en maestros
python etl/etl_fact_equipos.py     # Si hay nuevos equipos
python etl/etl_fact_servicios.py   # Si hay nuevos servicios
```

---

## Manejo de Errores

### Errores Comunes

#### 1. Archivo No Encontrado
```
❌ Error: No se encontró el archivo /path/to/file.csv
```
**Solución**: Verificar rutas en `DATA_DIR`

#### 2. Error de Conexión
```
❌ Error de conexión: FATAL:  password authentication failed
```
**Solución**: Verificar credenciales en `config/database_config.py`

#### 3. Registros Sin FK
```
⚠️  234 registros sin equipo_id
```
**Solución**: 
- Revisar códigos en staging vs dimensiones
- Ejecutar primero `etl_dimensions.py`
- Verificar normalización de keys

#### 4. Duplicados en PK
```
ERROR: duplicate key value violates unique constraint
```
**Solución**: 
- Ya manejado automáticamente con UPSERT
- Si persiste: eliminar duplicados manualmente

### Logs y Debugging

#### Activar modo verbose
```python
engine = create_engine(CONN_STR, echo=True)  # Muestra SQL
```

#### Ver registros problemáticos
```sql
-- En hecho_equipos
SELECT * FROM dw_HHCC.hecho_equipos 
WHERE equipo_id IS NULL OR paciente_id IS NULL;

-- En staging
SELECT * FROM stg.stg_reporte_equipos
WHERE "Código Interno" NOT IN (
    SELECT equipo_nk FROM dw_HHCC.dim_equipo
);
```

---

## Optimizaciones

### Mejoras de Performance

1. **Índices**: Creados automáticamente en FKs
2. **Bulk Inserts**: Uso de `method='multi'`
3. **Tablas Temporales**: Para UPSERT masivo
4. **Transacciones**: Un commit al final

### Configuración PostgreSQL

```ini
# postgresql.conf
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB
effective_cache_size = 1GB
```

### Paralelización (Futuro)

```python
# Cargar hechos en paralelo
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=2) as executor:
    f1 = executor.submit(cargar_hecho_equipos)
    f2 = executor.submit(cargar_hecho_servicios)
```

---

## Monitoreo

### Métricas a Revisar

```sql
-- Tiempo de última carga
SELECT 
    'hecho_equipos' as tabla,
    MAX(fecha_carga) as ultima_carga,
    COUNT(*) as total_registros
FROM dw_HHCC.hecho_equipos
UNION ALL
SELECT 
    'hecho_solicitud_servicios',
    MAX(fecha_carga),
    COUNT(*)
FROM dw_HHCC.hecho_solicitud_servicios;
```

### Alertas Recomendadas

- Carga fallida (exit code != 0)
- Más de 5% de registros rechazados
- Tiempo de ejecución > 2x promedio
- Espacio en disco < 20%

---

## Checklist de Validación

### Post-ETL

- [ ] Conteo de registros staging vs DW
- [ ] No hay FKs nulas en hechos
- [ ] Fechas válidas en dim_fecha
- [ ] SCD2 funcionando (múltiples versiones)
- [ ] Métricas suman correctamente
- [ ] Consultas de validación sin errores

```sql
-- Ejecutar
\i sql/05_queries_validation.sql
```
