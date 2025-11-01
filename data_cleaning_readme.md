# 🧹 Scripts de Limpieza de Datos

Este directorio contiene scripts para limpiar y preparar los archivos CSV antes de cargarlos al Data Warehouse.

## 📋 Scripts Disponibles

### 1. clean_reporte_equipos.py
**Propósito**: Limpia el reporte de equipos y normaliza códigos de aseguradoras.

**Entrada**:
- `data/Reporte Equipos.csv`
- `data/Aseguradora y Capita.csv`

**Salida**:
- `data/Reporte_Equipos_Limpio.csv`

**Acciones**:
- ✅ Detecta separador y encoding automáticamente
- ✅ Elimina registros con "DEMO" en documento
- ✅ Normaliza espacios en texto
- ✅ Reemplaza nombres de aseguradoras por códigos
- ✅ Marca aseguradoras sin código como "No Aplica"

**Ejecución**:
```bash
python data_cleaning/clean_reporte_equipos.py
```

---

### 2. clean_insumos_solicitados.py
**Propósito**: Limpia el histórico de insumos solicitados.

**Entrada**:
- `data/Insumos Solicitados Histórico Actualizado.csv`

**Salida**:
- `data/Insumos_Solicitados_Limpio.csv`

**Acciones**:
- ✅ Detecta delimitador automáticamente
- ✅ Elimina BOM y caracteres invisibles
- ✅ Normaliza espacios en columnas de texto
- ✅ Elimina registros con "DEMO" en identificación
- ✅ Limpia nombres de columnas

**Ejecución**:
```bash
python data_cleaning/clean_insumos_solicitados.py
```

---

### 3. clean_pedidos_codificacion.py
**Propósito**: Limpia pedidos y codifica insumos usando matching inteligente.

**Entrada**:
- `data/Pedidos Solicitados.csv`
- `data/Maestro Insumos Medicos.csv`
- `data/Maestro Medicamentos.csv`

**Salida**:
- `data/Pedidos_Limpio_YYYYMMDD_HHMM.csv`

**Acciones**:
- ✅ Elimina registros DEMO
- ✅ Consolida catálogo de insumos y medicamentos
- ✅ Normaliza nombres (elimina acentos, unidades, stopwords)
- ✅ **Fase 1**: Matching exacto
- ✅ **Fase 2**: Matching parcial (primeras 4 palabras)
- ✅ **Fase 3**: Fuzzy matching (requiere rapidfuzz)
- ✅ Reemplaza nombres por códigos
- ✅ Filtra solo registros codificados

**Dependencias opcionales**:
```bash
pip install rapidfuzz  # Para fuzzy matching
pip install tqdm       # Para barra de progreso
```

**Ejecución**:
```bash
python data_cleaning/clean_pedidos_codificacion.py
```

**Configuración**:
- `UMBRAL_FUZZY = 85`: Ajustar umbral de similitud (0-100)

---

## 🚀 Ejecución Rápida

### Ejecutar todos los scripts

**Linux/Mac**:
```bash
./run_cleaning.sh
```

**Windows**:
```batch
run_cleaning.bat
```

### Ejecutar individualmente

```bash
# Reporte Equipos
python data_cleaning/clean_reporte_equipos.py

# Insumos Solicitados
python data_cleaning/clean_insumos_solicitados.py

# Pedidos con Codificación
python data_cleaning/clean_pedidos_codificacion.py
```

---

## 📊 Flujo de Trabajo

```
┌─────────────────────┐
│  Archivos Crudos    │
│  (data/*.csv)       │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Scripts Limpieza   │
│  (data_cleaning/)   │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Archivos Limpios   │
│  (*_Limpio.csv)     │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│   ETL Scripts       │
│   (etl/)            │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│   Data Warehouse    │
│   (PostgreSQL)      │
└─────────────────────┘
```

---

## 🔧 Configuración

### Rutas de Archivos

Los scripts buscan archivos en `../data/` por defecto. Para cambiar:

```python
# En cada script, modificar:
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# O especificar ruta absoluta:
DATA_DIR = r"C:\ruta\a\tus\datos"
```

### Variables de Entorno

Opcionalmente usar variables de entorno:

```bash
export DATA_DIR=/ruta/a/datos
python data_cleaning/clean_reporte_equipos.py
```

---

## 🧪 Testing

### Verificar archivos de entrada

```bash
# Linux/Mac
ls -lh ../data/*.csv

# Windows
dir ..\data\*.csv
```

### Verificar archivos de salida

```bash
# Linux/Mac
ls -lh ../data/*_Limpio*.csv

# Windows
dir ..\data\*_Limpio*.csv
```

### Validar contenido

```python
import pandas as pd

# Verificar encoding
df = pd.read_csv('../data/Reporte_Equipos_Limpio.csv')
print(df.head())
print(df.info())

# Verificar no hay DEMO
assert not df['Documento Paciente'].str.contains('DEMO', case=False, na=False).any()
```

---

## 🐛 Troubleshooting

### Error: Archivo no encontrado

```
❌ Error: No se encontró el archivo Reporte Equipos
   Ubicación esperada: /path/to/data/Reporte Equipos.csv
```

**Solución**: Verificar que los archivos estén en `data/` con nombres exactos.

### Error: Encoding

```
UnicodeDecodeError: 'utf-8' codec can't decode...
```

**Solución**: Los scripts detectan encoding automáticamente. Si persiste, editar manualmente el encoding en el script.

### Warning: rapidfuzz no disponible

```
⚠️  rapidfuzz no disponible - solo matching exacto
```

**Solución**: 
```bash
pip install rapidfuzz
```

### Pocos registros codificados

Si `clean_pedidos_codificacion.py` codifica < 80% de registros:

1. Verificar archivos maestros están completos
2. Ajustar `UMBRAL_FUZZY` (reducir a 75-80)
3. Revisar normalización en función `normalizar_texto()`

---

## 📈 Métricas Esperadas

### clean_reporte_equipos.py
- Registros eliminados (DEMO): 0-5%
- Aseguradoras sin código: 0-2%

### clean_insumos_solicitados.py
- Registros eliminados (DEMO): 0-5%

### clean_pedidos_codificacion.py
- **Matching exacto**: 60-75%
- **Matching parcial**: 15-25%
- **Fuzzy matching**: 5-15%
- **Total codificado**: >85%

---

## 🔄 Actualización de Scripts

Al modificar los scripts:

1. Probar con datos de muestra
2. Verificar no rompe compatibilidad
3. Actualizar esta documentación
4. Commit con mensaje descriptivo

---

## 📞 Soporte

Si tienes problemas:

1. Revisa los logs de error
2. Verifica formato de archivos de entrada
3. Consulta [Issues](../../issues)
4. Contacta al equipo de datos

---

## 📝 Notas

- Los archivos limpios tienen sufijo `_Limpio`
- Pedidos genera timestamp en nombre de archivo
- Archivos originales NO se modifican
- Encoding de salida: `utf-8-sig` (compatible con Excel)
