# 🏥 Data Warehouse - Sistema de Gestión de Equipos y Servicios Médicos

## 📋 Descripción del Proyecto

Data Warehouse diseñado para la gestión y análisis de equipos médicos, servicios hospitalarios y seguimiento de pacientes. Implementa un modelo dimensional con arquitectura de staging + data warehouse.

## 🎯 Objetivos

- Centralizar información de equipos médicos, pacientes y aseguradoras
- Facilitar análisis de solicitudes de equipos y servicios
- Implementar SCD (Slowly Changing Dimensions) para rastrear cambios históricos
- Generar reportes analíticos para toma de decisiones

## 🏗️ Arquitectura

### Capas del Data Warehouse

1. **Capa de Staging (stg)**: Almacenamiento temporal de datos crudos
2. **Capa Dimensional (dw_HHCC)**: Modelo estrella con dimensiones y hechos

### Modelo Dimensional

#### 📊 Dimensiones

- **dim_equipo**: Catálogo de equipos médicos (SCD Tipo 2)
- **dim_paciente**: Información de pacientes (SCD Tipo 2)
- **dim_aseguradora**: Entidades aseguradoras (SCD Tipo 2)
- **dim_pedido**: Pedidos de insumos y servicios
- **dim_medicamento**: Catálogo de medicamentos
- **dim_fecha**: Dimensión de tiempo

#### 📈 Tablas de Hechos

- **hecho_equipos**: Solicitudes y entregas de equipos
- **hecho_solicitud_servicios**: Solicitudes de servicios e insumos

## 🚀 Instalación

### Prerrequisitos

- PostgreSQL 12+
- Python 3.8+
- pip

### Pasos de Instalación

1. **Clonar el repositorio**
```bash
git clone https://github.com/tu-usuario/dw-medical-equipment.git
cd dw-medical-equipment
```

2. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

3. **Configurar base de datos**

Editar `config/database_config.py` con tus credenciales:

```python
DB_USER = 'tu_usuario'
DB_PASS = 'tu_contraseña'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'dw_HHCC'
```

4. **Crear estructura de base de datos**

Ejecutar scripts SQL en orden:

```bash
psql -U postgres -d dw_HHCC -f sql/01_create_schemas.sql
psql -U postgres -d dw_HHCC -f sql/02_create_staging_tables.sql
psql -U postgres -d dw_HHCC -f sql/03_create_dimension_tables.sql
psql -U postgres -d dw_HHCC -f sql/04_create_fact_tables.sql
```

## 📂 Estructura de Archivos de Entrada

El sistema espera los siguientes archivos CSV/Excel:

```
data/
├── Maestro Equipos.csv
├── Pacientes.xlsx
├── Aseguradora y Capita.xlsx
├── Maestro Medicamentos.csv
├── Maestro Insumos Medicos.csv
├── Reporte Equipos.csv
└── Insumos Solicitados Histórico Actualizado.csv
```

### Formato de Archivos

#### Maestro Equipos.csv
- Código Interno
- Nombre Equipo
- EQUIPO ACTIVO

#### Pacientes.xlsx
- Identificacion
- Nombre
- Municipio
- Nombre Estado
- Aseguradora
- Zona
- Fecha Ingreso

#### Reporte Equipos.csv
- Codigo
- Equipo
- Documento Paciente
- Aseguradora
- Fecha Entregado
- Cantidad Equipos
- Estado Equipo

## 🔄 Proceso ETL

### 1. Cargar Dimensiones

```bash
python etl/etl_dimensions.py
```

Este script:
- Carga datos a staging
- Pobla todas las dimensiones
- Implementa SCD Tipo 2 para dim_equipo
- Maneja actualizaciones incrementales

### 2. Cargar Hechos de Equipos

```bash
python etl/etl_fact_equipos.py
```

Características:
- Validación de integridad referencial
- Auto-población de dimensiones faltantes
- Manejo de duplicados
- Generación de PKs compuestas

### 3. Cargar Hechos de Servicios

```bash
python etl/etl_fact_servicios.py
```

Características:
- Carga optimizada con UPSERT masivo
- Eliminación de duplicados
- Validación de datos
- Logging detallado

## 📊 Consultas de Ejemplo

### Equipos por Mes y Tipo
```sql
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
ORDER BY f.anio, f.mes;
```

### Servicios por Aseguradora
```sql
SELECT 
    a.aseguradora,
    COUNT(*) as total_solicitudes,
    SUM(h.cantidad) as total_insumos
FROM dw_HHCC.hecho_solicitud_servicios h
JOIN dw_HHCC.dim_aseguradora a ON h.aseguradora_id = a.aseguradora_id
WHERE a.es_actual = TRUE
GROUP BY a.aseguradora
ORDER BY total_solicitudes DESC;
```

## 🧪 Validación de Datos

### Verificar Carga de Dimensiones
```sql
SELECT 'dim_equipo' as tabla, COUNT(*) as registros FROM dw_HHCC.dim_equipo WHERE es_actual = TRUE
UNION ALL
SELECT 'dim_paciente', COUNT(*) FROM dw_HHCC.dim_paciente WHERE es_actual = TRUE
UNION ALL
SELECT 'dim_aseguradora', COUNT(*) FROM dw_HHCC.dim_aseguradora WHERE es_actual = TRUE
UNION ALL
SELECT 'dim_pedido', COUNT(*) FROM dw_HHCC.dim_pedido;
```

### Detectar Registros Huérfanos
```sql
SELECT 
    COUNT(*) as registros_sin_dimension
FROM dw_HHCC.hecho_equipos h
WHERE h.equipo_id IS NULL 
   OR h.paciente_id IS NULL 
   OR h.aseguradora_id IS NULL;
```

## 📚 Documentación Adicional

- [Arquitectura del Sistema](docs/ARCHITECTURE.md)
- [Modelo de Datos Detallado](docs/DATA_MODEL.md)
- [Proceso ETL](docs/ETL_PROCESS.md)

## 🛠️ Tecnologías Utilizadas

- **Base de Datos**: PostgreSQL
- **Lenguaje**: Python 3.8+
- **Librerías**:
  - SQLAlchemy: ORM y gestión de conexiones
  - Pandas: Manipulación de datos
  - psycopg2: Driver PostgreSQL

## 📋 Características Técnicas

✅ Slowly Changing Dimensions (SCD Tipo 2)  
✅ Validación de integridad referencial  
✅ Manejo de duplicados  
✅ Logging detallado  
✅ Carga incremental  
✅ Optimización con UPSERT masivo  
✅ Auto-corrección de dimensiones faltantes  

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📝 Licencia

Este proyecto está bajo la Licencia MIT. Ver archivo `LICENSE` para más detalles.

## 👥 Autores

- **Tu Nombre** - *Trabajo Inicial* - [Tu GitHub](https://github.com/tu-usuario)

## 📧 Contacto

Para preguntas o sugerencias, contactar a: tu-email@ejemplo.com

---

⭐ Si este proyecto te fue útil, considera darle una estrella en GitHub
