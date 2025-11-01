#!/bin/bash
# ============================================================
# Script de Limpieza de Datos - Data Warehouse
# Sistema: Linux/Mac
# Autor: Data Team
# ============================================================

set -e

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "\n${BLUE}============================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# ============================================================
# INICIO
# ============================================================
print_header "LIMPIEZA DE DATOS - DATA WAREHOUSE"

# Verificar Python
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 no está instalado"
    exit 1
fi

print_info "Python encontrado: $(python3 --version)"
echo ""

# ============================================================
# LIMPIEZA DE ARCHIVOS
# ============================================================

# Script 1: Reporte Equipos
if [ -f "data_cleaning/clean_reporte_equipos.py" ]; then
    print_info "Limpiando Reporte Equipos..."
    python3 data_cleaning/clean_reporte_equipos.py
    if [ $? -eq 0 ]; then
        print_success "Reporte Equipos completado"
    else
        print_error "Error en Reporte Equipos"
        exit 1
    fi
else
    print_error "Script clean_reporte_equipos.py no encontrado"
    exit 1
fi

echo ""

# Script 2: Insumos Solicitados
if [ -f "data_cleaning/clean_insumos_solicitados.py" ]; then
    print_info "Limpiando Insumos Solicitados..."
    python3 data_cleaning/clean_insumos_solicitados.py
    if [ $? -eq 0 ]; then
        print_success "Insumos Solicitados completado"
    else
        print_error "Error en Insumos Solicitados"
        exit 1
    fi
else
    print_error "Script clean_insumos_solicitados.py no encontrado"
    exit 1
fi

echo ""

# Script 3: Pedidos con Codificación
if [ -f "data_cleaning/clean_pedidos_codificacion.py" ]; then
    print_info "Limpiando y Codificando Pedidos..."
    python3 data_cleaning/clean_pedidos_codificacion.py
    if [ $? -eq 0 ]; then
        print_success "Pedidos completado"
    else
        print_error "Error en Pedidos"
        exit 1
    fi
else
    print_error "Script clean_pedidos_codificacion.py no encontrado"
    exit 1
fi

# ============================================================
# RESUMEN
# ============================================================
print_header "✅ LIMPIEZA COMPLETADA"

echo -e "${GREEN}"
echo "Todos los archivos han sido limpiados exitosamente:"
echo "  ✓ Reporte Equipos"
echo "  ✓ Insumos Solicitados"
echo "  ✓ Pedidos (con codificación)"
echo ""
echo "Los archivos limpios están en data/ con sufijo '_Limpio'"
echo -e "${NC}"

print_info "Próximos pasos:"
echo "  1. Verificar archivos generados en data/"
echo "  2. Ejecutar ETL: ./run_all.sh"
echo "  3. O ejecutar ETL manualmente"
echo ""
