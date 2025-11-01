# 🤝 Guía de Contribución

¡Gracias por tu interés en contribuir a este proyecto! Este documento proporciona pautas para hacer contribuciones efectivas.

## 📋 Tabla de Contenidos

- [Código de Conducta](#código-de-conducta)
- [Cómo Contribuir](#cómo-contribuir)
- [Proceso de Pull Request](#proceso-de-pull-request)
- [Estándares de Código](#estándares-de-código)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Testing](#testing)
- [Documentación](#documentación)

## 📜 Código de Conducta

Este proyecto sigue un código de conducta de comunidad abierta. Al participar, te comprometes a:

- Ser respetuoso y constructivo
- Aceptar críticas constructivas
- Enfocarte en lo mejor para la comunidad
- Mostrar empatía hacia otros miembros

## 🚀 Cómo Contribuir

### Reportar Bugs

Si encuentras un bug:

1. **Verifica** que no exista un issue similar
2. **Crea** un nuevo issue con:
   - Descripción clara del problema
   - Pasos para reproducirlo
   - Comportamiento esperado vs actual
   - Versión de Python, PostgreSQL
   - Logs relevantes

**Template de Bug Report**:
```markdown
## Descripción
[Descripción breve del bug]

## Pasos para Reproducir
1. Ejecutar script X
2. Con datos Y
3. Error Z ocurre

## Comportamiento Esperado
[Qué debería ocurrir]

## Comportamiento Actual
[Qué ocurre realmente]

## Entorno
- Python: 3.x
- PostgreSQL: 12.x
- OS: Windows/Linux/Mac
```

### Sugerir Mejoras

Para sugerir nuevas características:

1. **Abre** un issue con la etiqueta `enhancement`
2. **Describe** el problema que resuelve
3. **Propón** una solución
4. **Discute** alternativas

### Contribuir Código

1. **Fork** el repositorio
2. **Crea** una rama desde `main`:
   ```bash
   git checkout -b feature/amazing-feature
   ```
3. **Haz** tus cambios
4. **Commit** con mensajes descriptivos
5. **Push** a tu fork
6. **Abre** un Pull Request

## 🔄 Proceso de Pull Request

### Antes de Enviar

- [ ] Código sigue los estándares de estilo
- [ ] Tests pasan exitosamente
- [ ] Documentación actualizada
- [ ] Changelog actualizado
- [ ] Sin conflictos con `main`

### Estructura del PR

```markdown
## Descripción
[Qué hace este PR]

## Tipo de Cambio
- [ ] Bug fix
- [ ] Nueva característica
- [ ] Breaking change
- [ ] Documentación

## Testing
[Cómo probar los cambios]

## Checklist
- [ ] Tests pasan
- [ ] Documentación actualizada
- [ ] Sin conflictos
```

### Revisión

- Responde a comentarios constructivamente
- Haz cambios solicitados
- Mantén el PR actualizado con `main`

## 💻 Estándares de Código

### Python (PEP 8)

```python
# ✅ Correcto
def cargar_dimension(conn, tabla_staging):
    """
    Carga datos desde staging a dimensión
    
    Args:
        conn: Conexión a BD
        tabla_staging: Nombre de tabla staging
        
    Returns:
        int: Número de registros cargados
    """
    contador = 0
    # ... código
    return contador

# ❌ Incorrecto
def cargarDimension(c,t):
    cont=0
    # sin documentación
    return cont
```

### SQL

```sql
-- ✅ Correcto
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
ORDER BY f.anio DESC;

-- ❌ Incorrecto
select * from hecho_equipos h, dim_equipo e where h.equipo_id=e.equipo_id;
```

### Convenciones

#### Nombres
- **Tablas**: `snake_case`
- **Funciones**: `snake_case`
- **Variables**: `snake_case`
- **Constantes**: `UPPER_CASE`
- **Clases**: `PascalCase`

#### Comentarios
```python
# ✅ Comentarios útiles
# Cerrar versión anterior antes de insertar nueva (SCD2)
conn.execute(text("""UPDATE ... SET es_actual = FALSE"""))

# ❌ Comentarios obvios
# Ejecutar query
conn.execute(query)
```

## 🏗️ Estructura del Proyecto

```
dw-medical-equipment/
├── config/          # Configuración
├── sql/             # Scripts SQL
├── etl/             # Scripts ETL Python
├── docs/            # Documentación
├── tests/           # Tests (futuro)
└── data/            # Datos (no versionado)
```

### Agregar Nuevos Scripts

1. **ETL**: Colocar en `etl/`
2. **SQL**: Colocar en `sql/`
3. **Docs**: Colocar en `docs/`

### Nombrar Archivos

- ETL: `etl_<proposito>.py`
- SQL: `##_<proposito>.sql` (numerado)
- Docs: `<PROPOSITO>.md` (mayúsculas)

## 🧪 Testing

### Ejecutar Tests (futuro)

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Coverage
pytest --cov=etl tests/
```

### Escribir Tests

```python
# tests/test_etl_dimensions.py
def test_poblar_dim_equipo():
    """Verifica que dim_equipo se carga correctamente"""
    # Setup
    conn = get_test_connection()
    
    # Execute
    resultado = poblar_dim_equipo(conn)
    
    # Assert
    assert resultado > 0
    assert verificar_integridad(conn)
```

## 📝 Documentación

### Docstrings

```python
def funcion(param1, param2):
    """
    Breve descripción de una línea.
    
    Descripción más detallada si es necesario.
    Puede ocupar múltiples líneas.
    
    Args:
        param1 (tipo): Descripción del parámetro
        param2 (tipo): Descripción del parámetro
        
    Returns:
        tipo: Descripción del retorno
        
    Raises:
        ExceptionType: Cuándo ocurre
        
    Example:
        >>> funcion('valor1', 'valor2')
        'resultado'
    """
```

### README

Al agregar features, actualizar:
- Descripción de funcionalidad
- Instrucciones de uso
- Ejemplos

### Documentación Técnica

En `docs/`, actualizar:
- `ARCHITECTURE.md`: Cambios arquitectónicos
- `DATA_MODEL.md`: Nuevas tablas/campos
- `ETL_PROCESS.md`: Nuevos procesos ETL

## 🎨 Estilo de Commits

### Formato

```
<tipo>(<alcance>): <descripción>

[cuerpo opcional]

[footer opcional]
```

### Tipos

- `feat`: Nueva característica
- `fix`: Corrección de bug
- `docs`: Cambios en documentación
- `style`: Formato (no cambia código)
- `refactor`: Refactorización
- `test`: Agregar/modificar tests
- `chore`: Mantenimiento

### Ejemplos

```bash
# ✅ Buenos commits
feat(etl): agregar soporte para nuevos archivos Excel
fix(dim_equipo): corregir duplicados en SCD2
docs(readme): actualizar instrucciones de instalación
refactor(etl_servicios): optimizar carga con UPSERT masivo

# ❌ Malos commits
update stuff
fix
changes
```

## 🏷️ Versionamiento

Seguimos [Semantic Versioning](https://semver.org/):

- **MAJOR**: Cambios incompatibles
- **MINOR**: Nueva funcionalidad compatible
- **PATCH**: Bug fixes compatibles

Ejemplo: `v1.2.3`

## 📞 Contacto

Si tienes dudas:

1. Revisa la [documentación](docs/)
2. Busca en [issues existentes](../../issues)
3. Abre un [nuevo issue](../../issues/new)
4. Contacta a [tu-email@ejemplo.com]

## 🙏 Reconocimientos

Los contribuidores son reconocidos en:
- README principal
- Release notes
- Archivo CONTRIBUTORS.md

¡Gracias por contribuir! 🎉
