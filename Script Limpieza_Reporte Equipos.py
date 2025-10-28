import pandas as pd
import os

ruta_descargas = r"C:\Users\luste\Downloads"
ruta_reporte = os.path.join(ruta_descargas, "Reporte Equipos.csv")
ruta_aseguradoras = os.path.join(ruta_descargas, "Aseguradoras.csv")
ruta_salida = os.path.join(ruta_descargas, "Reporte_Equipos_Limpio.csv")

# --- Detectar separador con tolerancia a encoding ---
def detectar_sep(ruta):
    import csv
    for encoding in ['utf-8-sig', 'latin1', 'windows-1252']:
        try:
            with open(ruta, 'r', encoding=encoding) as f:
                primera_linea = f.readline()
                sniffer = csv.Sniffer()
                return sniffer.sniff(primera_linea).delimiter, encoding
        except Exception:
            continue
    raise ValueError("No se pudo detectar separador ni codificación del archivo")

# Detectar separador y encoding
sep_reporte, enc_reporte = detectar_sep(ruta_reporte)
sep_aseg, enc_aseg = detectar_sep(ruta_aseguradoras)

print(f"📄 Reporte Equipos -> separador: '{sep_reporte}', encoding: {enc_reporte}")
print(f"🏥 Aseguradoras -> separador: '{sep_aseg}', encoding: {enc_aseg}")

# --- Cargar archivos ---
reporte = pd.read_csv(ruta_reporte, sep=sep_reporte, encoding=enc_reporte)
aseguradoras = pd.read_csv(ruta_aseguradoras, sep=sep_aseg, encoding=enc_aseg)

# --- Limpieza: eliminar DEMO ---
reporte = reporte[~reporte['Documento Paciente'].astype(str).str.contains("DEMO", case=False, na=False)]

# --- Unificar columnas aseguradoras ---
aseguradoras.rename(columns={'Nit': 'Codigo_Aseguradora', 'Nombre': 'Aseguradora_Nombre'}, inplace=True)

# --- Normalizar texto ---
reporte['Aseguradora'] = reporte['Aseguradora'].astype(str).str.strip()
aseguradoras['Aseguradora_Nombre'] = aseguradoras['Aseguradora_Nombre'].astype(str).str.strip()

# --- Cruce por nombre ---
reporte = reporte.merge(
    aseguradoras[['Codigo_Aseguradora', 'Aseguradora_Nombre']],
    left_on='Aseguradora',
    right_on='Aseguradora_Nombre',
    how='left'
)

# --- Reemplazar valores ---
reporte['Aseguradora'] = reporte['Codigo_Aseguradora'].fillna('No Aplica')
reporte.drop(columns=['Codigo_Aseguradora', 'Aseguradora_Nombre'], inplace=True)

# --- Guardar resultado ---
reporte.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
print(f"✅ Archivo limpio generado en: {ruta_salida}")
