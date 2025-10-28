import pandas as pd
import os
import unicodedata
import re
from fuzzywuzzy import process

# --- RUTAS DE ARCHIVOS ---
ruta_pedidos = r"C:\Users\luste\Downloads\Pedidos Solictados.csv"
ruta_insumos = r"C:\Users\luste\Downloads\Insumos Medicos.csv"
ruta_pacientes = r"C:\Users\luste\Downloads\Pacientes.csv"

# --- VALIDACIÓN DE EXISTENCIA ---
for ruta in [ruta_pedidos, ruta_insumos, ruta_pacientes]:
    if not os.path.exists(ruta):
        print(f"⚠️ El archivo no existe: {ruta}")
        exit()

# --- FUNCIÓN DE NORMALIZACIÓN ---
def normalizar_texto(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto).upper()
    texto = ''.join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )  # elimina tildes
    texto = re.sub(r'[^A-Z0-9 ]', ' ', texto)  # elimina símbolos raros
    texto = re.sub(r'\s+', ' ', texto)  # reduce múltiples espacios a uno solo
    texto = texto.strip()  # quita espacios al inicio y final
    return texto

# --- LECTURA DE ARCHIVOS ---
print("📥 Leyendo archivos...")
pedidos = pd.read_csv(ruta_pedidos, sep=';', encoding='latin1', dtype=str)
insumos = pd.read_csv(ruta_insumos, sep=';', encoding='latin1', dtype=str)
pacientes = pd.read_csv(ruta_pacientes, sep=';', encoding='latin1', dtype=str)
print("✅ Archivos cargados correctamente.")

# --- LIMPIEZA DE PEDIDOS ---
pedidos = pedidos[~pedidos['Cedula'].str.contains('DEMO', case=False, na=False)]

# --- NORMALIZAR Y CRUZAR ---
pedidos['Insumo Solicitado (norm)'] = pedidos['Insumo Solicitado'].apply(normalizar_texto)
insumos['DESCRIPCIÓN DEL INSUMO (norm)'] = insumos['DESCRIPCIÓN DEL INSUMO'].apply(normalizar_texto)
# --- CRUCE EXACTO PRIMERO ---
merged = pedidos.merge(
    insumos[['DESCRIPCIÓN DEL INSUMO (norm)', 'CODIGO INTERNO']],
    left_on='Insumo Solicitado (norm)',
    right_on='DESCRIPCIÓN DEL INSUMO (norm)',
    how='left'
)

# --- COINCIDENCIAS PARCIALES (solo donde no hubo match exacto) ---
sin_codigo = merged[merged['CODIGO INTERNO'].isna()].copy()

if not sin_codigo.empty:
    print(f"🔍 Realizando coincidencias parciales para {len(sin_codigo)} insumos sin código...")

    insumo_dict = dict(zip(insumos['DESCRIPCIÓN DEL INSUMO (norm)'], insumos['CODIGO INTERNO']))
    lista_insumos = list(insumo_dict.keys())

    codigos_asignados = []
    for texto in sin_codigo['Insumo Solicitado (norm)']:
        match, score = process.extractOne(texto, lista_insumos)
        if score >= 85:  # umbral de similitud, se puede ajustar
            codigos_asignados.append(insumo_dict[match])
        else:
            codigos_asignados.append(None)

    sin_codigo['CODIGO INTERNO'] = codigos_asignados

    # Reemplazar los valores nulos originales por los encontrados
    merged.update(sin_codigo)

# --- REEMPLAZAR INSUMO SOLICITADO POR CÓDIGO INTERNO ---
merged['Insumo Solicitado'] = merged['CODIGO INTERNO'].fillna(merged['Insumo Solicitado'])
merged.drop(['DESCRIPCIÓN DEL INSUMO (norm)', 'CODIGO INTERNO', 'Insumo Solicitado (norm)'], axis=1, inplace=True)

# --- AGREGAR COLUMNA CLIENTE ---
pacientes_subset = pacientes[['Identificacion', 'Nombre']].copy()
pacientes_subset.rename(columns={'Identificacion': 'Cedula', 'Nombre': 'Cliente'}, inplace=True)

final = merged.merge(pacientes_subset, on='Cedula', how='left')

# --- EXPORTAR RESULTADO ---
ruta_salida = r"C:\Users\luste\Downloads\Pedidos_Solicitados_Limpio.csv"
final.to_csv(ruta_salida, sep=';', index=False, encoding='latin1')

print(f"💾 Archivo limpio y cruzado exportado en:\n{ruta_salida}")
print("✅ Coincidencias exactas y parciales completadas correctamente.")
