# 🏗️ Arquitectura del Data Warehouse

## Visión General

El Data Warehouse está diseñado con una arquitectura de dos capas que facilita la gestión, transformación y análisis de datos médicos.

## Capas de la Arquitectura

### 1. Capa de Staging (stg)

**Propósito**: Área de aterrizaje temporal para datos crudos procedentes de sistemas fuente.

**Características**:
- Sin transformaciones complejas
- Estructura idéntica a archivos fuente
- Datos volátiles (se sobrescriben en cada carga)
- Facilita debugging y trazabilidad

**Tablas**:
```
stg.stg_maestro_equipos
stg.stg_maestro_pacientes
stg.stg_maestro_aseguradoras
stg.stg_reporte_equipos
stg.stg_maestro_medicamentos
stg.stg_maestro_insumos
stg.stg_insumos_solicitados
```

### 2. Capa de Data Warehouse (dw_HHCC)

**Propósito**: Almacenamiento optimizado para consultas analíticas.

**Modelo**: Estrella (Star Schema)
- Dimensiones normalizadas
- Tablas de hechos desnormalizadas
- Relaciones mediante foreign keys

## Modelo Dimensional

### Esquema Estrella

```
                    dim_fecha
                        |
                        |
    dim_equipo ----→ hecho_equipos ←---- dim_paciente
                        |
                        ↓
                  dim_aseguradora


                    dim_fecha
                        |
                        |
    dim_pedido ----→ hecho_solicitud_servicios ←---- dim_paciente
                        |
                        ↓
                  dim_aseguradora
```

## Slowly Changing Dimensions (SCD)

### SCD Tipo 2 - Implementación

Las siguientes dimensiones mantienen historial completo de cambios:

1. **dim_equipo**
2. **dim_paciente**
3. **dim_aseguradora**

**Campos de control**:
- `vigente_desde`: Fecha inicio de vigencia
- `vigente_hasta`: Fecha fin de vigencia (9999-12-31 para registro actual)
- `es_actual`: Boolean indicando versión activa

**Ejemplo de versionamiento**:
```sql
equipo_id | equipo_nk | equipo        | estado_equipo | vigente_desde | vigente_hasta | es_actual
----------|-----------|---------------|---------------|---------------|---------------|----------
1         | 510       | Ventilador X  | ACTIVO        | 2023-01-01    | 2024-06-15    | FALSE
2         | 510       | Ventilador X+ | ACTIVO        | 2024-06-16    | 9999-12-31    | TRUE
```

## Flujo de Datos

### Proceso ETL

```
┌─────────────────┐
│  Archivos CSV   │
│     Excel       │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│   Python ETL    │
│  (pandas + SQL) │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  Staging (stg)  │
│  Carga Bruta    │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  Validación &   │
│  Transformación │
└────────┬────────┘
         │
         ├──────────────┐
         ↓              ↓
┌──────────────┐  ┌─────────────┐
│  Dimensiones │  │   Hechos    │
│   (dw_HHCC)  │  │  (dw_HHCC)  │
└──────────────┘  └─────────────┘
         │              │
         └──────┬───────┘
                ↓
         ┌─────────────┐
         │   BI Tools  │
         │  Reporting  │
         └─────────────┘
```

## Componentes Técnicos

### Base de Datos
- **SGBD**: PostgreSQL 12+
- **Encoding**: UTF-8
- **Índices**: Optimizados para consultas analíticas

### Lenguajes y Librerías
- **Python 3.8+**
  - SQLAlchemy: ORM y gestión de conexiones
  - Pandas: Transformación de datos
  - psycopg2: Driver nativo PostgreSQL

### Scripts ETL
1. `etl_dimensions.py`: Carga inicial de dimensiones
2. `etl_fact_equipos.py`: Carga de hechos de equipos
3. `etl_fact_servicios.py`: Carga de hechos de servicios

## Patrones de Diseño

### 1. Staging Pattern
Separación clara entre datos crudos y datos procesados.

### 2. Upsert Pattern
Inserción o actualización condicional usando `ON CONFLICT`.

### 3. Incremental Load
- Detección de registros nuevos vs existentes
- Actualización solo de cambios

### 4. Surrogate Keys
- IDs autoincrementales (SERIAL)
- Natural Keys preservadas para joins

### 5. Audit Trail
- Campos de fecha de carga
- Flags de vigencia
- Historial completo en SCD2

## Consideraciones de Rendimiento

### Índices Estratégicos
```sql
-- Foreign keys en tablas de hechos
CREATE INDEX idx_hecho_equipos_equipo ON hecho_equipos(equipo_id);
CREATE INDEX idx_hecho_equipos_fecha ON hecho_equipos(fecha_solicitud_id);

-- Natural keys en dimensiones
CREATE INDEX idx_dim_equipo_nk ON dim_equipo(equipo_nk);
CREATE INDEX idx_dim_paciente_doc ON dim_paciente(documento_paciente);
```

### Optimizaciones de Carga
- **Bulk inserts**: Uso de `method='multi'` en pandas
- **Transacciones**: Commit único al final del proceso
- **Tablas temporales**: Para UPSERT masivo

### Particionamiento (Futuro)
Considerar particionamiento por fecha en tablas de hechos cuando el volumen crezca:
```sql
-- Ejemplo futuro
PARTITION BY RANGE (fecha_solicitud_id)
```

## Seguridad

### Acceso a Datos
- Esquemas separados (stg vs dw_HHCC)
- Roles diferenciados:
  - ETL: escritura en stg y dw_HHCC
  - Analistas: solo lectura en dw_HHCC
  - Reportes: vistas materializadas

### Datos Sensibles
- Información de pacientes protegida
- Configuración de BD fuera de Git (.gitignore)
- Variables de entorno para credenciales

## Escalabilidad

### Horizontal
- Réplicas de lectura para reporting
- Separación de cargas ETL vs consultas analíticas

### Vertical
- Optimización de índices
- Compresión de datos históricos
- Archivado de registros antiguos

## Monitoreo y Mantenimiento

### Logs
- Registro detallado de cada ejecución ETL
- Alertas en caso de errores
- Métricas de tiempo de ejecución

### Validaciones Automáticas
- Conteo de registros pre/post carga
- Verificación de integridad referencial
- Detección de valores nulos inesperados

### Mantenimiento Periódico
- `VACUUM ANALYZE` semanal
- Reindexación mensual
- Revisión de espacio en disco

## Evolución Futura

### Mejoras Planificadas
1. Automatización con Airflow/Luigi
2. Vistas materializadas para reportes frecuentes
3. Data quality framework (Great Expectations)
4. Dashboards en tiempo real (Superset/Metabase)
5. Machine Learning para predicciones

### Extensiones del Modelo
- Nuevas dimensiones (proveedor, ubicación)
- Métricas calculadas (costos, tiempos de entrega)
- Agregaciones pre-calculadas
