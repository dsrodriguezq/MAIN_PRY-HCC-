# Robust CSV cleaning pipeline for "Equipos Entregados.csv"
# - Detecta encoding y separador
# - Normaliza nombres de columnas (snake_case)
# - Quita espacios y HTML
# - Parsea fechas (cols con "fecha", "date", "fch", "fech")
# - Parsea números con . y , (locale-aware)
# - Elimina duplicados y filas totalmente vacías
# - Exporta CSV limpio

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Tuple

# === Configura estas rutas ===
# Ejemplo Windows: src_path = Path(r"C:\Users\moran\OneDrive\Escritorio\Univalle\TERCERO\ETL Tesis\dw_pack\Equipos Entregados.csv")
src_path = Path("C:/Users/moran/OneDrive/Escritorio/Univalle/TERCERO/ETL Tesis/dw_pack/Insumos por programar.csv")
clean_path = Path("C:/Users/moran/OneDrive/Escritorio/Univalle/TERCERO/ETL Tesis/dw_pack/Limpieza/Insumos por programar_clean.csv")

def robust_read_csv(path: Path) -> Tuple[pd.DataFrame, dict]:
    """Leer CSV probando múltiples encodings y autodetección de separador."""
    attempts = [
        {"encoding": "utf-8", "sep": None, "engine": "python"},
        {"encoding": "utf-8-sig", "sep": None, "engine": "python"},
        {"encoding": "latin-1", "sep": None, "engine": "python"},
    ]
    errors = []
    for opts in attempts:
        try:
            df = pd.read_csv(path, **opts)
            meta = {"used_encoding": opts["encoding"], "used_sep": "auto", "engine": opts["engine"]}
            return df, meta
        except Exception as e:
            errors.append((opts, repr(e)))
            continue
    raise RuntimeError(f"Fallo al leer el CSV. Errores: {errors}")

def snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\s\-\/]", "", name, flags=re.UNICODE)  # quitar signos raros excepto - y /
    name = name.replace("/", "_").replace("-", "_")
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.lower()

def strip_html(val):
    if pd.isna(val):
        return val
    s = str(val)
    s = re.sub(r"<[^>]*>", "", s)  # elimina etiquetas HTML
    return s

def likely_date_col(col: str) -> bool:
    col_l = col.lower()
    keywords = ["fecha", "date", "fch", "fech"]
    return any(k in col_l for k in keywords)

def parse_date_series(s: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True, infer_datetime_format=True)
    if parsed.isna().mean() > 0.8:
        parsed2 = pd.to_datetime(s, errors="coerce", dayfirst=False, infer_datetime_format=True)
        if parsed2.notna().sum() > parsed.notna().sum():
            parsed = parsed2
    return parsed

def to_numeric_locale_aware(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return s
    t = s.astype(str)
    t = t.replace({"": np.nan, "nan": np.nan, "None": np.nan})
    t = t.str.replace(r"[^\d,\.\-]", "", regex=True)  # quita símbolos tipo $ % espacio
    sample = t.dropna().head(100)
    comma_as_decimal = (sample.str.contains(r",\d{1,2}$").mean() > 0.2) and (
        sample.str.count(r"\.").mean() >= 1 or sample.str.count(r"\.").mean() == 0
    )
    if comma_as_decimal:
        t = t.str.replace(r"\.", "", regex=True)  # quita separador de miles .
        t = t.str.replace(",", ".", regex=False)  # coma decimal -> punto
    else:
        t = t.str.replace(",", "", regex=False)   # quita separador de miles ,
    return pd.to_numeric(t, errors="coerce")

def clean_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    report = {}

    # 1) Normalizar nombres de columnas
    original_cols = df.columns.tolist()
    new_cols = []
    used = set()
    for c in original_cols:
        sc = snake_case(str(c))
        base = sc or "col"
        i = 1
        name = base
        while name in used:
            i += 1
            name = f"{base}_{i}"
        used.add(name)
        new_cols.append(name)
    df.columns = new_cols
    report["columns_before"] = original_cols
    report["columns_after"] = new_cols

    # 2) Eliminar filas totalmente vacías
    before_rows = len(df)
    df = df.dropna(how="all")
    after_drop_empty = len(df)
    report["dropped_fully_empty_rows"] = before_rows - after_drop_empty

    # 3) Limpiar textos (strip y sin HTML)
    obj_cols = df.select_dtypes(include=["object"]).columns.tolist()
    for c in obj_cols:
        df[c] = df[c].astype(str).str.strip().apply(strip_html)

    # 4) Parseo de fechas por nombre de columna
    date_cols = [c for c in df.columns if likely_date_col(c)]
    parsed_dates = []
    for c in date_cols:
        parsed = parse_date_series(df[c])
        if parsed.notna().sum() >= max(5, int(0.5 * len(df))):
            df[c] = parsed
            parsed_dates.append(c)
    report["date_columns_parsed"] = parsed_dates

    # 5) Conversión numérica heurística
    numeric_converted = []
    for c in df.columns:
        if c in parsed_dates or pd.api.types.is_numeric_dtype(df[c]):
            continue
        s = df[c].dropna().astype(str)
        if len(s) == 0:
            continue
        ratio_digits = s.str.contains(r"\d").mean()
        if ratio_digits >= 0.7:
            new_s = to_numeric_locale_aware(df[c])
            old_numeric_ratio = pd.to_numeric(df[c], errors="coerce").notna().mean()
            new_numeric_ratio = new_s.notna().mean()
            if new_numeric_ratio >= max(0.5, old_numeric_ratio):
                df[c] = new_s
                numeric_converted.append(c)
    report["numeric_columns_parsed"] = numeric_converted

    # 6) Eliminar duplicados exactos
    before_dups = len(df)
    df = df.drop_duplicates()
    after_dups = len(df)
    report["removed_duplicates"] = before_dups - after_dups

    # 7) Normalización simple de booleanos (si aplica)
    bool_map = {
        "si": True, "sí": True, "true": True, "1": True, "y": True, "yes": True, "verdadero": True,
        "no": False, "false": False, "0": False, "n": False, "not": False, "falso": False
    }
    for c in obj_cols:
        if df[c].dtype == "object":
            ser = df[c].str.lower()
            mask = ser.isin(bool_map.keys())
            if mask.mean() > 0.6:
                df.loc[mask, c] = ser[mask].map(bool_map)

    return df, report

# --- Ejecutar pipeline ---
raw_df, meta = robust_read_csv(src_path)
clean_df, rep = clean_dataframe(raw_df.copy())

# Guardar CSV limpio
clean_df.to_csv(clean_path, index=False)

# Reporte compacto en consola
summary = {
    "encoding_used": meta["used_encoding"],
    "separator": meta["used_sep"],
    "rows_original": len(raw_df),
    "rows_after_drop_empty": len(raw_df.dropna(how="all")),
    "rows_final": len(clean_df),
    "columns_before": rep["columns_before"],
    "columns_after": rep["columns_after"],
    "date_columns_parsed": rep["date_columns_parsed"],
    "numeric_columns_parsed": rep["numeric_columns_parsed"],
    "dropped_fully_empty_rows": rep["dropped_fully_empty_rows"],
    "removed_duplicates": rep["removed_duplicates"],
    "output_file": str(clean_path),
}
print(summary)
