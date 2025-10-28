import pandas as pd
import os

ruta_descargas = r"C:\Users\luste\Downloads"
ruta_reporte = os.path.join(ruta_descargas, "Reporte Equipos.csv")
ruta_aseguradoras = r"C:\Users\luste\Downloads\Aseguradora y Capita.csv"
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
print(f"🏥 Aseguradora y Capita -> separador: '{sep_aseg}', encoding: {enc_aseg}")

# --- Cargar archivos ---
reporte = pd.read_csv(ruta_reporte, sep=sep_reporte, encoding=enc_reporte)
aseguradoras = pd.read_csv(ruta_aseguradoras, sep=sep_aseg, encoding=enc_aseg, dtype=str)  # 👈 leer todo como texto

# --- Limpieza: eliminar DEMO ---
reporte = reporte[~reporte['Documento Paciente'].astype(str).str.contains("DEMO", case=False, na=False)]

# --- Normalizar texto ---
reporte['Aseguradora'] = reporte['Aseguradora'].astype(str).str.strip()
aseguradoras['Aseguradora'] = aseguradoras['Aseguradora'].astype(str).str.strip()
aseguradoras['Codigo Sistema'] = aseguradoras['Codigo Sistema'].astype(str).str.strip()  # 👈 asegurar texto limpio

# --- Cruce por nombre de aseguradora ---
reporte = reporte.merge(
    aseguradoras[['Aseguradora', 'Codigo Sistema']],
    on='Aseguradora',
    how='left'
)

# --- Reemplazar valores ---
reporte['Aseguradora'] = reporte['Codigo Sistema'].fillna('No Aplica')
reporte.drop(columns=['Codigo Sistema'], inplace=True)

# --- Guardar resultado ---
reporte.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
print(f"✅ Archivo limpio generado en: {ruta_salida}")
