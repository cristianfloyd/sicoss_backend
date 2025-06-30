# 🧪 Testing ConceptosProcessor Vectorizado

Documentación para probar la nueva implementación vectorizada del ConceptosProcessor.

## 📋 Scripts Disponibles

### 1. **test_conceptos_processor.py** - Prueba Completa
Script detallado con análisis profundo y visualización completa.

```bash
# Sintaxis básica
python test_conceptos_processor.py --legajo NUMERO_LEGAJO [OPCIONES]

# Ejemplos
python test_conceptos_processor.py --legajo 123456
python test_conceptos_processor.py --legajo 123456 --periodo 2024/12
python test_conceptos_processor.py --legajo 123456 --debug
python test_conceptos_processor.py -l 123456 -p 2024/11 -d
```

**Opciones:**
- `--legajo, -l`: Número de legajo (requerido)
- `--periodo, -p`: Período YYYY/MM (opcional, default: actual)
- `--debug, -d`: Modo debug (opcional)

### 2. **quick_test.py** - Prueba Rápida
Script simple para testing básico.

```bash
# Sintaxis
python quick_test.py NUMERO_LEGAJO

# Ejemplo
python quick_test.py 123456
```

## 🎯 ¿Qué Se Prueba?

### ✅ **Funcionalidad Principal:**
1. **Extracción de datos** desde PostgreSQL
2. **Expansión de tipos_grupos** `{6,9,21}` → múltiples filas
3. **Mapeo vectorizado** de tipos a campos SICOSS
4. **Agrupación y sumarización** por legajo
5. **Generación de campos** según lógica PHP legacy

### ✅ **Casos de Prueba Cubiertos:**
- Conceptos simples (tipos 6,7,8,21,22,45,etc.)
- SAC con escalafón (DOCE, AUTO, NODO)
- Investigadores (tipos 11-15, 48-49)
- Casos especiales (tipo 58 - Seguro Vida)
- Conceptos sin tipos_grupos
- Legajos sin conceptos

## 📊 Ejemplo de Salida

### **Prueba Completa:**
```
================================================================================
🧪 RESULTADOS DE PRUEBA - CONCEPTOS PROCESSOR VECTORIZADO
================================================================================

👤 LEGAJO: 123456
   Nombre: EMPLEADO EJEMPLO
   CUIT: 20999999999
   Estado: A

📊 ESTADÍSTICAS:
   Conceptos procesados: 8
   Importe total: $45,287.50
   Campos SICOSS con valor: 5
   Tipos grupos únicos: [6, 9, 21, 45]

📥 CONCEPTOS LIQUIDADOS (8):
------------------------------------------------------------
   1001 | C |  $15,000.00 | {9} | DOCE
   2001 | C |   $2,500.00 | {6} | DOCE
   3001 | C |   $8,750.00 | {21} | DOCE
   ...

⚡ CAMPOS SICOSS PROCESADOS (5):
------------------------------------------------------------
   ImporteSAC                    |   $15,000.00
   ImporteHorasExtras           |    $2,500.00
   ImporteAdicionales           |    $8,750.00
   ImporteSACDoce               |   $15,000.00
   PrioridadTipoDeActividad     |           38
```

### **Prueba Rápida:**
```
🧪 Prueba rápida para legajo 123456
--------------------------------------------------
✅ EMPLEADO EJEMPLO (123456)
📊 8 conceptos → 5 campos SICOSS
💰 Total: $45,287.50

🎯 Campos principales:
   ImporteAdicionales: $8,750.00
   ImporteHorasExtras: $2,500.00
   ImporteSAC: $15,000.00
   ImporteSACDoce: $15,000.00
   PrioridadTipoDeActividad: 38

✅ Prueba completada
```

## 🔧 Prerequisitos

### **Configuración Requerida:**
1. **Base de datos PostgreSQL** configurada
2. **Archivo database.ini** con credenciales
3. **Dependencias Python** instaladas:
   ```bash
   pip install pandas psycopg2-binary
   ```

### **Estructura del Proyecto:**
```
sicoss_backend/
├── database/
│   └── database_connection.py
├── extractors/
│   ├── legajos_extractor.py
│   └── conceptos_extractor.py
├── processors/
│   └── conceptos_processor.py ← NUEVO VECTORIZADO
├── config/
│   └── sicoss_config.py
├── test_conceptos_processor.py ← SCRIPT COMPLETO
├── quick_test.py ← SCRIPT RÁPIDO
└── database.ini
```

## 🚀 Casos de Uso

### **Desarrollo:**
```bash
# Probar un legajo específico durante desarrollo
python quick_test.py 123456

# Análisis detallado con debug
python test_conceptos_processor.py -l 123456 -d
```

### **Validación:**
```bash
# Probar diferentes períodos
python test_conceptos_processor.py -l 123456 -p 2024/01
python test_conceptos_processor.py -l 123456 -p 2024/12

# Comparar legajos con diferentes tipos de conceptos
python quick_test.py 100001  # Legajo con SAC
python quick_test.py 100002  # Legajo con horas extras
python quick_test.py 100003  # Legajo investigador
```

### **Testing de Rendimiento:**
```bash
# Medir tiempo de procesamiento
time python quick_test.py 123456

# Con debug para ver detalles de rendimiento
python test_conceptos_processor.py -l 123456 -d
```

## ⚠️ Troubleshooting

### **Errores Comunes:**

1. **"Legajo no encontrado"**
   - Verificar que el legajo existe en la BD
   - Verificar período (puede no tener liquidación ese mes)

2. **"Error de conexión BD"**
   - Verificar `database.ini`
   - Verificar conectividad a PostgreSQL

3. **"Error en tipos_grupos"**
   - Verificar formato del campo en BD
   - Puede ser string `"{1,2,3}"` o array `[1,2,3]`

4. **"ImportError"**
   - Verificar que todos los módulos estén en el path
   - Ejecutar desde directorio raíz del proyecto

### **Debug Avanzado:**
```bash
# Ver logs detallados
python test_conceptos_processor.py -l 123456 -d 2>&1 | tee debug.log

# Verificar solo extracción
python -c "
from extractors.conceptos_extractor import ConceptosExtractor
from database.database_connection import DatabaseConnection
db = DatabaseConnection()
extractor = ConceptosExtractor(db)
df = extractor.extract_for_legajos(2024, 12, [123456])
print(df.head())
"
```

## 📈 Métricas de Rendimiento

El ConceptosProcessor vectorizado debería mostrar:
- **~10-50x más rápido** que implementación con loops
- **Tiempo típico**: 0.001-0.1s para legajo individual
- **Memoria**: Uso eficiente con operaciones pandas nativas

## 🎯 Próximos Pasos

1. **Validar** con legajos reales de diferentes tipos
2. **Comparar** resultados con código PHP legacy
3. **Optimizar** casos específicos si es necesario
4. **Extender** con pruebas batch para múltiples legajos 