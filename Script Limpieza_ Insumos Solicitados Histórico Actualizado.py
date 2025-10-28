import pandas as pd
import re

# --- Rutas ---
ruta_archivo = r"C:\Users\luste\Downloads\drive-download-20250926T023828Z-1-001\Insumos Solicitados Histórico Actualizado.csv"
ruta_salida = r"C:\Users\luste\Downloads\Insumos Solicitados Histórico Actualizado Limpio.csv"

# --- Detectar delimitador ---
def detectar_delimitador(ruta):
    with open(ruta, 'r', encoding='latin1', errors='ignore') as f:
        primera_linea = f.readline()
    posibles = [',', ';', '\t', '|']
    conteos = {sep: primera_linea.count(sep) for sep in posibles}
    delimitador = max(conteos, key=conteos.get)
    print(f"🕵️ Delimitador detectado: '{delimitador}'")
    return delimitador

sep_detectado = detectar_delimitador(ruta_archivo)

# --- Leer CSV ---
# Se usa utf-8-sig para eliminar automáticamente el BOM (ï»¿)
insumos = pd.read_csv(
    ruta_archivo,
    sep=sep_detectado,
    encoding='utf-8-sig',
    on_bad_lines='skip'
)

# --- Limpieza general ---
# Normalizar nombres de columnas
insumos.columns = (
    insumos.columns
    .str.replace(r'[\ufeff\u200b]', '', regex=True)  # elimina BOM y caracteres invisibles
    .str.strip()
)

# Quitar espacios en celdas tipo texto
for col in insumos.select_dtypes(include='object').columns:
    insumos[col] = insumos[col].astype(str).str.strip()

# --- Eliminar registros tipo DEMO ---
columna_id = 'Identificacion Paciente'
if columna_id not in insumos.columns:
    print(f"❌ No se encontró la columna '{columna_id}'. Columnas detectadas: {insumos.columns.tolist()}")
    exit()

# Convertir a string y eliminar registros con 'demo' en cualquier forma
insumos[columna_id] = insumos[columna_id].astype(str).str.strip()
mascara_demo = insumos[columna_id].str.contains(r'demo', flags=re.IGNORECASE, na=False)
insumos_limpio = insumos[~mascara_demo]

# --- Guardar el nuevo archivo limpio (sin cambiar ruta) ---
insumos_limpio.to_csv(ruta_salida, index=False, encoding='utf-8-sig', sep=sep_detectado)

# --- Resumen ---
eliminados = mascara_demo.sum()
print(f"🧹 Registros eliminados con 'DEMO' (en cualquier forma): {eliminados}")
print(f"✅ Registros finales: {len(insumos_limpio)}")
print(f"📋 Columnas finales: {list(insumos_limpio.columns)}")
print(f"💾 Archivo limpio guardado en:\n{ruta_salida}")
