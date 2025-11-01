#!/bin/bash
# ============================================================
# Script de Ejecución Completa - Data Warehouse
# Sistema: Linux/Mac
# Autor: Data Team
# ============================================================

set -e  # Salir si algún comando falla

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funciones auxiliares
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

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Verificar Python
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 no está instalado"
        exit 1
    fi
    print_success "Python 3 encontrado: $(python3 --version)"
}

# Verificar PostgreSQL
check_postgresql() {
    if ! command -v psql &> /dev/null; then
        print_warning "PostgreSQL no encontrado en PATH"
        print_info "Puedes continuar si ya tienes PostgreSQL instalado"
    else
        print_success "PostgreSQL encontrado: $(psql --version)"
    fi
}

# ============================================================
# INICIO
# ============================================================
print_header "DATA WAREHOUSE - EJECUCIÓN COMPLETA"

# Verificaciones
print_info "Verificando requisitos..."
check_python
check_postgresql

# ============================================================
# FASE 1: LIMPIEZA DE DATOS
# ============================================================
print_header "FASE 1: LIMPIEZA DE DATOS"

print_info "Ejecutando limpiezas de datos..."

# Limpiar Reporte Equipos
if [ -f "data_cleaning/clean_reporte_equipos.py" ]; then
    echo ""
    python3 data_cleaning/clean_reporte_equipos.py
    if [ $? -eq 0 ]; then
        print_success "Reporte Equipos limpiado"
    else
        print_error "Error limpiando Reporte Equipos"
        exit 1
    fi
else
    print_warning "Script de limpieza de Reporte Equipos no encontrado"
fi

# Limpiar Insumos Solicitados
if [ -f "data_cleaning/clean_insumos_solicitados.py" ]; then
    echo ""
    python3 data_cleaning/clean_insumos_solicitados.py
    if [ $? -eq 0 ]; then
        print_success "Insumos Solicitados limpiado"
    else
        print_error "Error limpiando Insumos Solicitados"
        exit 1
    fi
else
    print_warning "Script de limpieza de Insumos no encontrado"
fi

# Limpiar y Codificar Pedidos
if [ -f "data_cleaning/clean_pedidos_codificacion.py" ]; then
    echo ""
    python3 data_cleaning/clean_pedidos_codificacion.py
    if [ $? -eq 0 ]; then
        print_success "Pedidos limpiados y codificados"
    else
        print_error "Error limpiando Pedidos"
        exit 1
    fi
else
    print_warning "Script de limpieza de Pedidos no encontrado"
fi

# ============================================================
# FASE 2: CREACIÓN DE ESTRUCTURA BD
# ============================================================
print_header "FASE 2: ESTRUCTURA DE BASE DE DATOS"

print_info "¿Deseas crear/recrear la estructura de BD? (s/n)"
read -r respuesta

if [ "$respuesta" = "s" ] || [ "$respuesta" = "S" ]; then
    print_info "Ejecutando scripts SQL..."
    
    DB_NAME="${DB_NAME:-dw_HHCC}"
    DB_USER="${DB_USER:-postgres}"
    
    for script in sql/0*.sql; do
        if [ -f "$script" ]; then
            echo ""
            print_info "Ejecutando: $(basename $script)"
            psql -U "$DB_USER" -d "$DB_NAME" -f "$script"
            if [ $? -eq 0 ]; then
                print_success "$(basename $script) ejecutado"
            else
                print_error "Error en $(basename $script)"
                exit 1
            fi
        fi
    done
else
    print_info "Omitiendo creación de estructura BD"
fi

# ============================================================
# FASE 3: ETL - DIMENSIONES
# ============================================================
print_header "FASE 3: ETL - CARGA DE DIMENSIONES"

if [ -f "etl/etl_dimensions.py" ]; then
    echo ""
    python3 etl/etl_dimensions.py
    if [ $? -eq 0 ]; then
        print_success "Dimensiones cargadas"
    else
        print_error "Error cargando dimensiones"
        exit 1
    fi
else
    print_error "Script ETL de dimensiones no encontrado"
    exit 1
fi

# ============================================================
# FASE 4: ETL - HECHOS EQUIPOS
# ============================================================
print_header "FASE 4: ETL - CARGA HECHOS EQUIPOS"

if [ -f "etl/etl_fact_equipos.py" ]; then
    echo ""
    python3 etl/etl_fact_equipos.py
    if [ $? -eq 0 ]; then
        print_success "Hechos de equipos cargados"
    else
        print_error "Error cargando hechos de equipos"
        exit 1
    fi
else
    print_error "Script ETL de equipos no encontrado"
    exit 1
fi

# ============================================================
# FASE 5: ETL - HECHOS SERVICIOS
# ============================================================
print_header "FASE 5: ETL - CARGA HECHOS SERVICIOS"

if [ -f "etl/etl_fact_servicios.py" ]; then
    echo ""
    python3 etl/etl_fact_servicios.py
    if [ $? -eq 0 ]; then
        print_success "Hechos de servicios cargados"
    else
        print_error "Error cargando hechos de servicios"
        exit 1
    fi
else
    print_error "Script ETL de servicios no encontrado"
    exit 1
fi

# ============================================================
# FASE 6: VALIDACIÓN
# ============================================================
print_header "FASE 6: VALIDACIÓN DE DATOS"

print_info "¿Deseas ejecutar validaciones? (s/n)"
read -r respuesta

if [ "$respuesta" = "s" ] || [ "$respuesta" = "S" ]; then
    if [ -f "sql/05_queries_validation.sql" ]; then
        echo ""
        psql -U "$DB_USER" -d "$DB_NAME" -f "sql/05_queries_validation.sql"
        print_success "Validaciones ejecutadas"
    else
        print_warning "Script de validación no encontrado"
    fi
else
    print_info "Omitiendo validaciones"
fi

# ============================================================
# FINALIZACIÓN
# ============================================================
print_header "✅ PROCESO COMPLETADO EXITOSAMENTE"

echo -e "${GREEN}"
echo "Todas las fases se ejecutaron correctamente:"
echo "  ✓ Limpieza de datos"
echo "  ✓ Creación de estructura (si se ejecutó)"
echo "  ✓ Carga de dimensiones"
echo "  ✓ Carga de hechos de equipos"
echo "  ✓ Carga de hechos de servicios"
echo "  ✓ Validaciones (si se ejecutaron)"
echo ""
echo "Tu Data Warehouse está listo para consultas! 🎉"
echo -e "${NC}"

print_info "Próximos pasos:"
echo "  1. Conectar herramientas BI (Power BI, Tableau, etc.)"
echo "  2. Ejecutar consultas de análisis"
echo "  3. Crear dashboards y reportes"
echo ""
