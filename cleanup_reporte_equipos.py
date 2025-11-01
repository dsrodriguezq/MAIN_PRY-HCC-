"""
Script de Limpieza: Reporte Equipos
Autor: Data Team
Descripción: Limpia archivo de reporte de equipos y normaliza aseguradoras
"""
import pandas as pd
import os
import csv
import sys

# ===============================================================
# CONFIGURACIÓN
# ===============================================================
# Ajustar estas rutas según tu estructura
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DOWNLOADS_DIR = os.path.expanduser('~/Downloads')

# Archivos de entrada
RUTA_REPORTE = os.path.join(DATA_DIR, "Reporte Equipos.csv")
RUTA_ASEGURADORAS = os.path.join(DATA_DIR, "Aseguradora y Capita.csv")

# Archivo de salida
RUTA_SALIDA = os.path.join(DATA_DIR, "Reporte_Equipos_Limpio.csv")

# ===============================================================
# FUNCIONES AUXILIARES
# ===============================================================
def detectar_separador_y_encoding(ruta):
    """
    Detecta automáticamente el separador y encoding de un archivo CSV
    
    Args:
        ruta (str): Ruta al archivo CSV
        
    Returns:
        tuple: (separador, encoding)
    """
    encodings = ['utf-8-sig', 'latin1', 'windows-1252', 'utf-8']
    
    for encoding in encodings:
        try:
            with open(ruta, 'r', encoding=encoding) as f:
                primera_linea = f.readline()
                sniffer = csv.Sniffer()
                separador = sniffer.sniff(primera_linea).delimiter
                return separador, encoding
        except Exception:
            continue
    
    raise ValueError(f"No se pudo detectar separador ni encoding del archivo: {ruta}")

def verificar_archivo(ruta, nombre):
    """Verifica que el archivo exista"""
    if not os.path.exists(ruta):
        print(f"❌ Error: No se encontró el archivo {nombre}")
        print(f"   Ubicación esperada: {ruta}")
        return False
    return True

# ===============================================================
# PROCESO PRINCIPAL
# ===============================================================
def limpiar_reporte_equipos():
    """Limpia el reporte de equipos"""
    print("="*60)
    print("LIMPIEZA: REPORTE EQUIPOS")
    print("="*60)
    
    # Verificar archivos
    if not verificar_archivo(RUTA_REPORTE, "Reporte Equipos"):
        return False
    if not verificar_archivo(RUTA_ASEGURADORAS, "Aseguradora y Capita"):
        return False
    
    try:
        # Detectar formato
        print("\n📄 Detectando formato de archivos...")
        sep_reporte, enc_reporte = detectar_separador_y_encoding(RUTA_REPORTE)
        sep_aseg, enc_aseg = detectar_separador_y_encoding(RUTA_ASEGURADORAS)
        
        print(f"   Reporte Equipos → separador: '{sep_reporte}', encoding: {enc_reporte}")
        print(f"   Aseguradoras → separador: '{sep_aseg}', encoding: {enc_aseg}")
        
        # Cargar archivos
        print("\n📥 Cargando archivos...")
        reporte = pd.read_csv(RUTA_REPORTE, sep=sep_reporte, encoding=enc_reporte)
        aseguradoras = pd.read_csv(
            RUTA_ASEGURADORAS, 
            sep=sep_aseg, 
            encoding=enc_aseg, 
            dtype=str
        )
        
        print(f"   Registros originales: {len(reporte):,}")
        
        # Limpieza: eliminar registros DEMO
        print("\n🧹 Eliminando registros DEMO...")
        reporte_original = len(reporte)
        reporte = reporte[
            ~reporte['Documento Paciente']
            .astype(str)
            .str.contains("DEMO", case=False, na=False)
        ]
        eliminados = reporte_original - len(reporte)
        print(f"   Eliminados: {eliminados:,} registros")
        
        # Normalizar texto
        print("\n🔤 Normalizando texto...")
        reporte['Aseguradora'] = reporte['Aseguradora'].astype(str).str.strip()
        aseguradoras['Aseguradora'] = aseguradoras['Aseguradora'].astype(str).str.strip()
        aseguradoras['Codigo Sistema'] = aseguradoras['Codigo Sistema'].astype(str).str.strip()
        
        # Cruce con catálogo de aseguradoras
        print("\n🔗 Cruzando con catálogo de aseguradoras...")
        reporte_antes = len(reporte)
        reporte = reporte.merge(
            aseguradoras[['Aseguradora', 'Codigo Sistema']],
            on='Aseguradora',
            how='left'
        )
        
        # Reemplazar nombre por código
        sin_codigo = reporte['Codigo Sistema'].isna().sum()
        if sin_codigo > 0:
            print(f"   ⚠️  {sin_codigo} registros sin código de aseguradora (se marcará 'No Aplica')")
        
        reporte['Aseguradora'] = reporte['Codigo Sistema'].fillna('No Aplica')
        reporte.drop(columns=['Codigo Sistema'], inplace=True)
        
        # Guardar resultado
        print("\n💾 Guardando archivo limpio...")
        reporte.to_csv(RUTA_SALIDA, index=False, encoding='utf-8-sig')
        
        # Resumen
        print("\n" + "="*60)
        print("✅ LIMPIEZA COMPLETADA")
        print("="*60)
        print(f"Registros originales: {reporte_original:,}")
        print(f"Registros eliminados: {eliminados:,}")
        print(f"Registros finales: {len(reporte):,}")
        print(f"\nArchivo guardado en:\n{RUTA_SALIDA}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante la limpieza: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# ===============================================================
# EJECUCIÓN
# ===============================================================
if __name__ == "__main__":
    try:
        exito = limpiar_reporte_equipos()
        sys.exit(0 if exito else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
        sys.exit(1)
