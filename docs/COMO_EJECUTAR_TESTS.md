# 🧪 Cómo Ejecutar Tests - SICOSS Backend

## 📋 Resumen

Este documento explica cómo ejecutar los tests del proyecto SICOSS Backend.

## 🚀 Métodos de Ejecución

### 1. Usando el Script Helper (Recomendado)

```bash
# Ejecutar todos los tests principales
./scripts/ejecutar_tests.sh all

# Ejecutar tests rápidos (sin BD)
./scripts/ejecutar_tests.sh quick

# Ejecutar tests específicos
./scripts/ejecutar_tests.sh licencias
./scripts/ejecutar_tests.sh calculos
./scripts/ejecutar_tests.sh sicoss
./scripts/ejecutar_tests.sh conceptos

# Ver ayuda
./scripts/ejecutar_tests.sh help
```

### 2. Usando pytest Directamente

#### Ejecutar todos los tests unitarios

```bash
python -m pytest tests/unit/ -v --no-cov
```

#### Ejecutar un archivo de tests específico

```bash
# Tests de LicenciasExtractor
python -m pytest tests/unit/test_extractors/test_licencias_extractor.py -v --no-cov

# Tests de CalculosProcessor
python -m pytest tests/unit/test_processors/test_calculos_processor_unit.py -v --no-cov

# Tests de SicossDataProcessor
python -m pytest tests/unit/test_processors/test_sicoss_processor_unit.py -v --no-cov

# Tests de ConceptosProcessor
python -m pytest tests/unit/test_processors/test_conceptos_processor_unit.py -v --no-cov
```

#### Ejecutar un test específico

```bash
python -m pytest tests/unit/test_extractors/test_licencias_extractor.py::test_extract_for_legajos_lista_vacia -v --no-cov
```

#### Excluir tests de integración (que requieren BD)

```bash
python -m pytest tests/unit/test_extractors/test_licencias_extractor.py -k "not integration" -v --no-cov
```

### 3. Opciones Útiles de pytest

#### Ver output detallado

```bash
python -m pytest tests/unit/ -v --no-cov
```

#### Ver output muy detallado (con prints)

```bash
python -m pytest tests/unit/ -v -s --no-cov
```

#### Ejecutar solo tests que fallaron la última vez

```bash
python -m pytest tests/unit/ --lf --no-cov
```

#### Ejecutar tests en paralelo (requiere pytest-xdist)

```bash
python -m pytest tests/unit/ -n auto --no-cov
```

#### Ver cobertura (si está disponible)

```bash
python -m pytest tests/unit/ --cov=processors --cov=extractors --cov-report=html
```

## 📊 Tests Disponibles

### Tests Unitarios Principales

| Archivo | Descripción | Tests | Estado |
|---------|-------------|-------|--------|
| `test_licencias_extractor.py` | Tests de LicenciasExtractor | 21 | ✅ |
| `test_calculos_processor_unit.py` | Tests de CalculosProcessor | 14 | ✅ |
| `test_sicoss_processor_unit.py` | Tests de SicossDataProcessor | 17 | ✅ |
| `test_conceptos_processor_unit.py` | Tests de ConceptosProcessor | 40 | ✅ |

**Total: 92 tests unitarios**

### Tests de Integración

Los tests marcados con `@pytest.mark.integration` requieren:
- Base de datos configurada (`database.ini`)
- Datos reales en la BD

Para ejecutarlos:

```bash
python -m pytest tests/ -m integration -v --no-cov
```

## ⚠️ Notas Importantes

### 1. Tests de Integración

Los tests de integración requieren una base de datos configurada. Si no tienes `database.ini` configurado, estos tests fallarán con:

```
Exception: Section postgresql not found in database.ini
```

**Solución:** Excluir tests de integración con `-k "not integration"` o configurar la BD.

### 2. Coverage (pytest-cov)

Hay un bug conocido de compatibilidad entre `pytest-cov`/`coverage` y pandas 2.3.3/numpy 2.3.4 que puede causar errores `_NoValueType`. 

**Recomendación:** Usar `--no-cov` para ejecutar tests sin coverage, o usar herramientas alternativas.

### 3. Tests que Requieren BD Real

Los siguientes tests requieren BD real:
- `test_extract_for_legajos_integracion_bd_real`
- `test_extract_integracion_bd_real`

Estos están marcados con `@pytest.mark.integration` y se pueden excluir con `-k "not integration"`.

## 📈 Estadísticas de Tests

### Resumen por Componente

```
✅ LicenciasExtractor:        21 tests (19 unitarios + 2 integración)
✅ CalculosProcessor:          14 tests (todos unitarios)
✅ SicossDataProcessor:        17 tests (todos unitarios)
✅ ConceptosProcessor:         40 tests (todos unitarios)
─────────────────────────────────────────────────────
   TOTAL:                      92 tests
```

### Cobertura Objetivo

- ✅ `test_licencias_extractor.py`: >90% cobertura
- ✅ `test_calculos_processor.py`: Casos de borde validados
- ✅ `test_sicoss_processor.py`: Pipeline completo validado
- ✅ `test_conceptos_processor.py`: >90% cobertura

## 🔧 Troubleshooting

### Error: "ModuleNotFoundError"

```bash
# Asegúrate de estar en el directorio raíz del proyecto
cd /home/usuario/development/sicoss_backend

# Verifica que Python puede importar los módulos
python -c "from processors.sicoss_processor import SicossDataProcessor; print('OK')"
```

### Error: "Database connection failed"

Los tests unitarios NO requieren BD. Si ves este error, probablemente estás ejecutando tests de integración.

**Solución:**
```bash
# Excluir tests de integración
python -m pytest tests/unit/ -k "not integration" -v --no-cov
```

### Tests muy lentos

```bash
# Ejecutar solo tests rápidos
./scripts/ejecutar_tests.sh quick

# O excluir tests marcados como "slow"
python -m pytest tests/unit/ -m "not slow" -v --no-cov
```

## 📝 Ejemplos Prácticos

### Ejemplo 1: Ejecutar todos los tests rápidos

```bash
./scripts/ejecutar_tests.sh quick
```

### Ejemplo 2: Ejecutar un test específico con output detallado

```bash
python -m pytest tests/unit/test_processors/test_calculos_processor_unit.py::test_importe_imponible_6_tipo_operacion_2_diferencia_mayor_5 -v -s --no-cov
```

### Ejemplo 3: Ejecutar tests de un componente específico

```bash
python -m pytest tests/unit/test_processors/test_calculos_processor_unit.py -v --no-cov
```

### Ejemplo 4: Ver qué tests hay disponibles

```bash
python -m pytest tests/unit/test_processors/test_calculos_processor_unit.py --collect-only
```

## 🎯 Mejores Prácticas

1. **Ejecutar tests antes de commit:**
   ```bash
   ./scripts/ejecutar_tests.sh quick
   ```

2. **Ejecutar tests completos antes de push:**
   ```bash
   ./scripts/ejecutar_tests.sh all
   ```

3. **Ejecutar tests específicos durante desarrollo:**
   ```bash
   python -m pytest tests/unit/test_processors/test_calculos_processor_unit.py -v --no-cov
   ```

4. **Ver solo tests que fallan:**
   ```bash
   python -m pytest tests/unit/ --lf -v --no-cov
   ```

## 📚 Referencias

- [Documentación de pytest](https://docs.pytest.org/)
- [pytest.ini](./pytest.ini) - Configuración de pytest del proyecto
- [TODO.md](../TODO.md) - Estado de los tests y cobertura
