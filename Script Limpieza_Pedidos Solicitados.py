import pandas as pd
import os
import unicodedata
import re
from tqdm import tqdm
from rapidfuzz import process, fuzz

# ===============================================================
# 📂 RUTAS DE ARCHIVOS
# ===============================================================
ruta_pedidos = r"C:\Users\luste\Downloads\Pedidos Solictados.csv"
ruta_insumos = r"C:\Users\luste\Downloads\Insumos Medicos.csv"
ruta_maestro = r"C:\Users\luste\Downloads\Maestro Medicamentos.csv"

for ruta in [ruta_pedidos, ruta_insumos, ruta_maestro]:
    if not os.path.exists(ruta):
        print(f"⚠️ El archivo no existe: {ruta}")
        exit()

# ===============================================================
# 🧽 FUNCIÓN DE NORMALIZACIÓN
# ===============================================================
def normalizar_texto(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto).upper()
    texto = ''.join(c for c in unicodedata.normalize('NFKD', texto)
                    if not unicodedata.combining(c))
    texto = re.sub(r'\bCMS\b', 'CM', texto)
    texto = re.sub(r'\bMTS\b', 'MT', texto)
    texto = re.sub(r'\bPU\b', '', texto)
    texto = re.sub(r'\b(\d+)\s*MG\b', r'\1MG', texto)
    texto = re.sub(r'\b(\d+)\s*ML\b', r'\1ML', texto)
    texto = re.sub(r'\b(DE|POR|X|EL|LA|LOS|LAS|EN|CON|A|AL|INTRAMUSCULAR|INTRAVENOSA|ORAL)\b', '', texto)
    texto = re.sub(r'[^A-Z0-9 ]+', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()

# ===============================================================
# 📥 LECTURA DE ARCHIVOS
# ===============================================================
pedidos = pd.read_csv(ruta_pedidos, sep=';', encoding='latin1', dtype=str)
insumos = pd.read_csv(ruta_insumos, sep=';', encoding='latin1', dtype=str)
maestro = pd.read_csv(ruta_maestro, sep=';', encoding='latin1', dtype=str)

# ===============================================================
# 💊 CONSOLIDADO
# ===============================================================
insumos['codigo'] = insumos['CODIGO INTERNO'].astype(str).str.replace(r'\.0$', '', regex=True)
insumos['nombre'] = insumos['DESCRIPCIÓN DEL INSUMO']
maestro['codigo'] = maestro['Código del Medicamento'].astype(str).str.replace(r'\.0$', '', regex=True)
maestro['nombre'] = maestro['Nombre']

consolidado = pd.concat([insumos[['codigo', 'nombre']], maestro[['codigo', 'nombre']]], ignore_index=True)
consolidado = consolidado[consolidado['codigo'].notna() & (consolidado['codigo'] != '') & 
                          consolidado['nombre'].notna() & (consolidado['nombre'].str.strip() != '')]
consolidado['nombre_norm'] = consolidado['nombre'].apply(normalizar_texto)

diccionario_codigos = consolidado.set_index('nombre_norm')['codigo'].to_dict()
nombres_consolidado = list(diccionario_codigos.keys())

# ===============================================================
# 🧮 PROCESAMIENTO DE PEDIDOS
# ===============================================================
pedidos = pedidos[~pedidos['Cedula'].str.contains('DEMO', case=False, na=False)].copy()
pedidos_norm = pedidos.copy()
pedidos_norm['Insumo_Solicitado_norm'] = pedidos_norm['Insumo Solicitado'].apply(normalizar_texto)

# --- Exact match ---
pedidos_norm['codigo'] = pedidos_norm['Insumo_Solicitado_norm'].map(diccionario_codigos)

# --- Parcial por primeras 4 palabras ---
def buscar_codigo_parcial(nombre):
    if not nombre:
        return None
    for maestro_norm in nombres_consolidado:
        codigo = diccionario_codigos[maestro_norm]
        primeras = maestro_norm.split()[:4]
        if all(p in nombre for p in primeras):
            return codigo
    return None

tqdm.pandas()
mask = pedidos_norm['codigo'].isna()
pedidos_norm.loc[mask, 'codigo'] = pedidos_norm.loc[mask, 'Insumo_Solicitado_norm'].progress_apply(buscar_codigo_parcial)

# --- Fuzzy match último recurso ---
def buscar_codigo_fuzzy(nombre, umbral=85):
    if not nombre:
        return None
    mejor = process.extractOne(nombre, nombres_consolidado, scorer=fuzz.token_sort_ratio)
    if mejor and mejor[1] >= umbral:
        return diccionario_codigos[mejor[0]]
    return None

mask = pedidos_norm['codigo'].isna()
pedidos_norm.loc[mask, 'codigo'] = pedidos_norm.loc[mask, 'Insumo_Solicitado_norm'].progress_apply(buscar_codigo_fuzzy)

# ===============================================================
# 🔹 ELIMINAR REGISTROS SIN COINCIDENCIA
# ===============================================================
pedidos_filtrados = pedidos_norm[pd.notna(pedidos_norm['codigo'])].copy()

# Reemplazar Insumo Solicitado con el código encontrado
pedidos_filtrados['Insumo Solicitado'] = pedidos_filtrados['codigo']

# Mantener solo columnas originales
columnas_originales = pedidos.columns.tolist()
final = pedidos_filtrados[columnas_originales]

# ===============================================================
# 💾 EXPORTAR
# ===============================================================
from datetime import datetime
ruta_salida = os.path.join(os.path.dirname(ruta_pedidos), f"Pedidos_Limpio_{datetime.now():%Y%m%d_%H%M}.csv")
final.to_csv(ruta_salida, sep=';', index=False, encoding='utf-8-sig')

print(f"\n✅ Archivo final exportado: {ruta_salida}")
print(f"📊 Total de registros con código: {len(final):,}")
