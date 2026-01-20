#!/bin/bash
# Script para ejecutar tests del proyecto SICOSS Backend

set -e

echo "🧪 EJECUTANDO TESTS SICOSS BACKEND"
echo "=================================="
echo ""

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Función para ejecutar tests
run_tests() {
    local test_path=$1
    local description=$2
    
    echo -e "${YELLOW}📋 $description${NC}"
    echo "Ejecutando: pytest $test_path"
    echo ""
    
    if python -m pytest "$test_path" -v --no-cov; then
        echo -e "${GREEN}✅ Tests pasaron exitosamente${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}❌ Algunos tests fallaron${NC}"
        echo ""
        return 1
    fi
}

# Menú de opciones
case "${1:-all}" in
    "licencias")
        run_tests "tests/unit/test_extractors/test_licencias_extractor.py" "Tests de LicenciasExtractor"
        ;;
    
    "calculos")
        run_tests "tests/unit/test_processors/test_calculos_processor_unit.py" "Tests de CalculosProcessor"
        ;;
    
    "sicoss")
        run_tests "tests/unit/test_processors/test_sicoss_processor_unit.py" "Tests de SicossDataProcessor"
        ;;
    
    "conceptos")
        run_tests "tests/unit/test_processors/test_conceptos_processor_unit.py" "Tests de ConceptosProcessor"
        ;;
    
    "unit")
        echo -e "${YELLOW}📋 Ejecutando todos los tests unitarios${NC}"
        echo ""
        run_tests "tests/unit/" "Tests Unitarios Completos"
        ;;
    
    "integration")
        echo -e "${YELLOW}📋 Ejecutando tests de integración${NC}"
        echo ""
        run_tests "tests/integration/" "Tests de Integración"
        ;;
    
    "all")
        echo -e "${YELLOW}📋 Ejecutando todos los tests (unitarios + integración)${NC}"
        echo ""
        
        # Tests unitarios principales
        run_tests "tests/unit/test_extractors/test_licencias_extractor.py" "Tests de LicenciasExtractor"
        run_tests "tests/unit/test_processors/test_calculos_processor_unit.py" "Tests de CalculosProcessor"
        run_tests "tests/unit/test_processors/test_sicoss_processor_unit.py" "Tests de SicossDataProcessor"
        run_tests "tests/unit/test_processors/test_conceptos_processor_unit.py" "Tests de ConceptosProcessor"
        
        echo -e "${GREEN}✅ Todos los tests principales completados${NC}"
        ;;
    
    "quick")
        echo -e "${YELLOW}📋 Ejecutando tests rápidos (sin integración)${NC}"
        echo ""
        echo -e "${YELLOW}📋 Tests de LicenciasExtractor (sin BD)${NC}"
        echo "Ejecutando: pytest tests/unit/test_extractors/test_licencias_extractor.py -k 'not integration'"
        echo ""
        if python -m pytest "tests/unit/test_extractors/test_licencias_extractor.py" -k "not integration" -v --no-cov; then
            echo -e "${GREEN}✅ Tests pasaron exitosamente${NC}"
            echo ""
        else
            echo -e "${RED}❌ Algunos tests fallaron${NC}"
            echo ""
        fi
        run_tests "tests/unit/test_processors/test_calculos_processor_unit.py" "Tests de CalculosProcessor"
        run_tests "tests/unit/test_processors/test_sicoss_processor_unit.py" "Tests de SicossDataProcessor"
        ;;
    
    "help"|"-h"|"--help")
        echo "Uso: ./scripts/ejecutar_tests.sh [opción]"
        echo ""
        echo "Opciones:"
        echo "  licencias    - Ejecuta tests de LicenciasExtractor"
        echo "  calculos     - Ejecuta tests de CalculosProcessor"
        echo "  sicoss       - Ejecuta tests de SicossDataProcessor"
        echo "  conceptos    - Ejecuta tests de ConceptosProcessor"
        echo "  unit         - Ejecuta todos los tests unitarios"
        echo "  integration  - Ejecuta tests de integración"
        echo "  quick        - Ejecuta tests rápidos (sin BD)"
        echo "  all          - Ejecuta todos los tests principales (default)"
        echo "  help         - Muestra esta ayuda"
        echo ""
        echo "Ejemplos:"
        echo "  ./scripts/ejecutar_tests.sh licencias"
        echo "  ./scripts/ejecutar_tests.sh quick"
        echo "  ./scripts/ejecutar_tests.sh all"
        ;;
    
    *)
        echo -e "${RED}❌ Opción desconocida: $1${NC}"
        echo "Usa './scripts/ejecutar_tests.sh help' para ver las opciones disponibles"
        exit 1
        ;;
esac
