#!/usr/bin/env python3
"""
test_recordset_exporter.py

Test completo para verificar la funcionalidad del SicossRecordsetExporter
para respuestas API estructuradas FastAPI → Laravel
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime

# Agregar directorio padre al path
sys.path.append(os.path.dirname(__file__))

try:
    from exporters.recordset_exporter import SicossRecordsetExporter, SicossApiResponse
    from config.sicoss_config import SicossConfig
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print("📋 Asegúrate de que todos los módulos estén implementados")
    exit(1)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def crear_datos_prueba():
    """Crea datos de prueba simulados para testing"""
    
    # DataFrame de legajos procesados simulado
    legajos_test = pd.DataFrame({
        'nro_legaj': [12345, 67890, 11111],
        'cuil': ['20123456789', '20987654321', '20111111111'],
        'apnom': ['EMPLEADO TEST A', 'EMPLEADO TEST B', 'EMPLEADO TEST C'],
        'IMPORTE_BRUTO': [150000.50, 200000.75, 175000.25],
        'IMPORTE_IMPON': [140000.00, 190000.00, 165000.00],
        'ImporteSAC': [12500.00, 16667.00, 14583.00],
        'codigosituacion': [1, 1, 2],
        'TipoDeActividad': [1, 2, 1],
        'ImporteNoRemun': [10000.50, 10000.75, 10000.25],
        'ImporteImponible_4': [140000.00, 190000.00, 165000.00],
        'ImporteImponible_5': [140000.00, 190000.00, 165000.00],
        'ImporteImponible_6': [0.0, 0.0, 0.0],
        'ImporteImponible_8': [0.0, 0.0, 0.0],
        'ImporteImponible_9': [0.0, 0.0, 0.0]
    })
    
    # Resultado SICOSS simulado completo
    resultado_sicoss = {
        'legajos_procesados': legajos_test,
        'estadisticas': {
            'total_legajos': 3,
            'legajos_validos': 3,
            'legajos_rechazados': 0,
            'porcentaje_aprobacion': 100.0,
            'distribucion_situacion': {'1': 2, '2': 1},
            'distribucion_actividad': {'1': 2, '2': 1}
        },
        'totales': {
            'bruto': 525001.50,
            'imponible_1': 495000.00,
            'imponible_2': 495000.00,
            'imponible_4': 495000.00,
            'imponible_5': 495000.00,
            'sac': 43750.00,
            'cantidad_legajos': 3
        },
        'metricas': {
            'tiempo_total_segundos': 1.234
        }
    }
    
    return resultado_sicoss

def test_inicializacion():
    """Test 1: Inicialización del exporter"""
    print("\n🧪 TEST 1: Inicialización SicossRecordsetExporter")
    print("-" * 50)
    
    try:
        # Sin debug info
        exporter_basic = SicossRecordsetExporter()
        print("✅ Exporter básico inicializado correctamente")
        
        # Con debug info
        exporter_debug = SicossRecordsetExporter(include_debug_info=True)
        print("✅ Exporter con debug inicializado correctamente")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en inicialización: {e}")
        return False

def test_transformacion_completa():
    """Test 2: Transformación completa de resultados"""
    print("\n🧪 TEST 2: Transformación completa")
    print("-" * 50)
    
    try:
        exporter = SicossRecordsetExporter(include_debug_info=True)
        resultado_sicoss = crear_datos_prueba()
        
        # Test transformación completa con detalles
        api_response = exporter.transformar_resultado_completo(
            resultado_sicoss, 
            include_details=True
        )
        
        # Verificaciones básicas
        assert api_response.success == True, "Response debe ser exitosa"
        assert "legajos" in api_response.data, "Debe contener legajos"
        assert "estadisticas" in api_response.data, "Debe contener estadísticas"
        assert "resumen" in api_response.data, "Debe contener resumen"
        assert len(api_response.data['legajos']) == 3, "Debe tener 3 legajos"
        
        print(f"✅ Transformación exitosa: {api_response.message}")
        print(f"   - Success: {api_response.success}")
        print(f"   - Legajos: {len(api_response.data['legajos'])}")
        print(f"   - Timestamp: {api_response.timestamp}")
        
        # Verificar estructura de legajo
        primer_legajo = api_response.data['legajos'][0]
        campos_esperados = ['nro_legaj', 'cuil', 'apnom', 'bruto', 'imponible', 'sac', 'cod_situacion', 'cod_actividad']
        
        for campo in campos_esperados:
            assert campo in primer_legajo, f"Campo {campo} debe estar presente"
        
        print(f"   - Primer legajo: {primer_legajo['nro_legaj']} - {primer_legajo['apnom']}")
        print(f"   - Bruto: ${primer_legajo['bruto']:,.2f}")
        print(f"   - Detalles incluidos: {'detalles' in primer_legajo}")
        
        # Verificar estadísticas
        estadisticas = api_response.data['estadisticas']
        assert estadisticas['legajos_procesados'] == 3, "Debe procesar 3 legajos"
        assert estadisticas['tiempo_procesamiento_ms'] > 0, "Debe tener tiempo de procesamiento"
        
        print(f"   - Tiempo procesamiento: {estadisticas['tiempo_procesamiento_ms']:.0f}ms")
        print(f"   - Total bruto: ${estadisticas['totales']['bruto']:,.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en transformación completa: {e}")
        return False

def test_formatos_respuesta():
    """Test 3: Diferentes formatos de respuesta"""
    print("\n🧪 TEST 3: Formatos de respuesta")
    print("-" * 50)
    
    try:
        exporter = SicossRecordsetExporter()
        resultado_sicoss = crear_datos_prueba()
        
        formatos = ["completo", "resumen", "solo_totales"]
        
        for formato in formatos:
            print(f"\n   🔄 Probando formato: '{formato}'")
            
            resultado_laravel = exporter.exportar_para_laravel(
                resultado_sicoss, 
                formato=formato
            )
            
            # Verificaciones básicas
            assert resultado_laravel['success'] == True, f"Formato {formato} debe ser exitoso"
            assert 'data' in resultado_laravel, f"Formato {formato} debe tener data"
            assert 'metadata' in resultado_laravel, f"Formato {formato} debe tener metadata"
            
            print(f"      ✅ Formato '{formato}': OK")
            print(f"         - Success: {resultado_laravel['success']}")
            
            # Verificaciones específicas por formato
            if formato == "solo_totales":
                assert 'totales' in resultado_laravel['data'], "Solo totales debe tener totales"
                assert 'resumen' in resultado_laravel['data'], "Solo totales debe tener resumen"
                legajos_count = len(resultado_laravel['data'].get('legajos', []))
                print(f"         - Solo datos esenciales: {len(resultado_laravel['data'])} secciones")
                
            elif formato == "resumen":
                legajos = resultado_laravel['data'].get('legajos', [])
                legajos_count = len(legajos)
                assert legajos_count <= 100, "Resumen debe limitar legajos a 100"
                print(f"         - Legajos limitados: {legajos_count} (máx 100)")
                
            else:  # completo
                legajos = resultado_laravel['data'].get('legajos', [])
                legajos_count = len(legajos)
                print(f"         - Legajos completos: {legajos_count}")
                
                # Verificar que tiene detalles
                if legajos_count > 0:
                    primer_legajo = legajos[0]
                    tiene_detalles = 'detalles' in primer_legajo
                    print(f"         - Detalles incluidos: {tiene_detalles}")
        
        print("✅ Todos los formatos funcionan correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error en formatos de respuesta: {e}")
        return False

def test_respuesta_fastapi():
    """Test 4: Respuesta específica para FastAPI"""
    print("\n🧪 TEST 4: Respuesta FastAPI")
    print("-" * 50)
    
    try:
        exporter = SicossRecordsetExporter()
        resultado_sicoss = crear_datos_prueba()
        
        resultado_fastapi = exporter.generar_respuesta_fastapi(resultado_sicoss)
        
        # Verificaciones FastAPI específicas
        assert resultado_fastapi['success'] == True, "FastAPI response debe ser exitosa"
        assert 'api_version' in resultado_fastapi, "Debe tener api_version"
        assert 'content_type' in resultado_fastapi, "Debe tener content_type"
        assert resultado_fastapi['api_version'] == 'v1', "API version debe ser v1"
        assert resultado_fastapi['content_type'] == 'application/json', "Content type debe ser JSON"
        
        print(f"✅ Respuesta FastAPI generada correctamente")
        print(f"   - Success: {resultado_fastapi['success']}")
        print(f"   - API version: {resultado_fastapi['api_version']}")
        print(f"   - Content type: {resultado_fastapi['content_type']}")
        print(f"   - Total records: {resultado_fastapi['metadata']['total_records']}")
        print(f"   - Processing time: {resultado_fastapi['metadata']['processing_time_ms']:.0f}ms")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en respuesta FastAPI: {e}")
        return False

def test_manejo_errores():
    """Test 5: Manejo de errores"""
    print("\n🧪 TEST 5: Manejo de errores")
    print("-" * 50)
    
    try:
        exporter = SicossRecordsetExporter()
        
        # Test con datos inválidos
        datos_invalidos = {
            "datos_corruptos": "test",
            "sin_legajos": True
        }
        
        respuesta_error = exporter.transformar_resultado_completo(datos_invalidos)
        
        # Verificar que maneja el error correctamente
        assert respuesta_error.success == False, "Debe fallar con datos inválidos"
        assert "Error en procesamiento SICOSS" in respuesta_error.message, "Debe tener mensaje de error"
        assert respuesta_error.data.get('error_details'), "Debe tener detalles del error"
        
        print(f"✅ Manejo de errores funcional:")
        print(f"   - Success: {respuesta_error.success}")
        print(f"   - Es error: {not respuesta_error.success}")
        print(f"   - Mensaje: {respuesta_error.message[:50]}...")
        print(f"   - Error details: {bool(respuesta_error.data.get('error_details'))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en test de manejo de errores: {e}")
        return False

def test_integracion_processor():
    """Test 6: Integración con SicossDataProcessor"""
    print("\n🧪 TEST 6: Integración con SicossDataProcessor")
    print("-" * 50)
    
    try:
        from processors.sicoss_processor import SicossDataProcessor
        
        # Configuración de prueba
        config = SicossConfig(
            tope_jubilatorio_patronal=1000000.0,
            tope_jubilatorio_personal=800000.0,
            tope_otros_aportes_personales=900000.0,
            trunca_tope=True
        )
        
        # Crear procesador
        processor = SicossDataProcessor(config)
        print("✅ SicossDataProcessor inicializado con recordset_exporter")
        
        # Verificar que tiene el exporter
        assert hasattr(processor, 'recordset_exporter'), "Processor debe tener recordset_exporter"
        assert processor.recordset_exporter is not None, "recordset_exporter no debe ser None"
        
        # Datos simulados más completos
        datos_simulados = {
            'legajos': pd.DataFrame({
                'nro_legaj': [99999],
                'apyno': ['TEST API INTEGRATION'],
                'cuit': ['20999999999'],
                'codigoescalafon': [1],
                'codigosituacion': [1],
                'codact': [1],
                'codlug': [1]
            }),
            'conceptos': pd.DataFrame({
                'nro_legaj': [99999],
                'codn_conce': [100],
                'impp_conce': [50000.0],
                'tipos_grupos': [[1]],
                'codigoescalafon': [1]
            }),
            'otra_actividad': pd.DataFrame(),
            'obra_social': pd.DataFrame()
        }
        
        # Procesar CON respuesta API
        print("   🚀 Procesando con generación de respuesta API...")
        resultado = processor.procesar_datos_extraidos(
            datos=datos_simulados,
            formato_respuesta="completo"
        )
        
        # Verificar que se generó la respuesta API
        assert 'api_response' in resultado, "Debe generar api_response"
        api_response = resultado['api_response']
        assert api_response['success'] == True, "API response debe ser exitosa"
        assert 'legajos' in api_response['data'], "API response debe tener legajos"
        assert api_response['metadata']['backend'] == 'sicoss_python', "Debe tener metadata correcto"
        
        print(f"✅ API response generada en pipeline:")
        print(f"   - Success: {api_response['success']}")
        print(f"   - Legajos: {len(api_response['data']['legajos'])}")
        print(f"   - Backend: {api_response['metadata']['backend']}")
        
        # Test método directo
        print("   🎯 Test método generar_respuesta_api directo...")
        respuesta_directa = processor.generar_respuesta_api(resultado, "fastapi")
        
        assert respuesta_directa['api_version'] == 'v1', "Debe tener API version"
        assert respuesta_directa['content_type'] == 'application/json', "Debe tener content type"
        
        print(f"✅ Método directo funcional:")
        print(f"   - API version: {respuesta_directa['api_version']}")
        print(f"   - Content type: {respuesta_directa['content_type']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en integración con processor: {e}")
        return False

def test_performance_basico():
    """Test 7: Performance básico"""
    print("\n🧪 TEST 7: Performance básico")
    print("-" * 50)
    
    try:
        exporter = SicossRecordsetExporter()
        
        # Crear dataset más grande para performance
        legajos_grandes = pd.DataFrame({
            'nro_legaj': range(1000, 1100),  # 100 legajos
            'cuil': [f'2012345{i:04d}' for i in range(100)],
            'apnom': [f'EMPLEADO TEST {i}' for i in range(100)],
            'IMPORTE_BRUTO': [150000.0 + (i * 1000) for i in range(100)],
            'IMPORTE_IMPON': [140000.0 + (i * 900) for i in range(100)],
            'ImporteSAC': [12500.0 + (i * 100) for i in range(100)],
            'codigosituacion': [1] * 100,
            'TipoDeActividad': [1] * 100
        })
        
        resultado_grande = {
            'legajos_procesados': legajos_grandes,
            'estadisticas': {
                'total_legajos': 100,
                'legajos_validos': 100,
                'legajos_rechazados': 0,
                'porcentaje_aprobacion': 100.0
            },
            'totales': {
                'bruto': 19450000.0,
                'imponible_1': 18550000.0,
                'sac': 1495000.0,
                'cantidad_legajos': 100
            },
            'metricas': {
                'tiempo_total_segundos': 2.5
            }
        }
        
        # Medir tiempo de transformación
        start_time = datetime.now()
        
        resultado_api = exporter.transformar_resultado_completo(resultado_grande)
        
        end_time = datetime.now()
        tiempo_ms = (end_time - start_time).total_seconds() * 1000
        
        assert resultado_api.success == True, "Transformación debe ser exitosa"
        assert len(resultado_api.data['legajos']) == 100, "Debe procesar 100 legajos"
        
        print(f"✅ Performance test completado:")
        print(f"   - Legajos procesados: {len(resultado_api.data['legajos'])}")
        print(f"   - Tiempo transformación: {tiempo_ms:.1f}ms")
        print(f"   - Tiempo por legajo: {tiempo_ms/100:.2f}ms")
        
        # Verificar que es razonablemente rápido (menos de 1 segundo para 100 legajos)
        assert tiempo_ms < 1000, f"Debe ser rápido, pero tomó {tiempo_ms:.1f}ms"
        
        return True
        
    except Exception as e:
        print(f"❌ Error en test de performance: {e}")
        return False

def main():
    """Ejecuta todos los tests"""
    print("🧪 INICIANDO TESTS SICOSS RECORDSET EXPORTER")
    print("=" * 60)
    
    tests = [
        test_inicializacion,
        test_transformacion_completa,
        test_formatos_respuesta,
        test_respuesta_fastapi,
        test_manejo_errores,
        test_integracion_processor,
        test_performance_basico
    ]
    
    resultados = []
    
    for test_func in tests:
        try:
            resultado = test_func()
            resultados.append(resultado)
            
            if resultado:
                print(f"   ✅ {test_func.__name__}: EXITOSO")
            else:
                print(f"   ❌ {test_func.__name__}: FALLÓ")
                
        except Exception as e:
            print(f"   💥 {test_func.__name__}: ERROR - {e}")
            resultados.append(False)
    
    # Resumen final
    exitosos = sum(resultados)
    total = len(resultados)
    porcentaje = (exitosos / total) * 100
    
    print("\n" + "=" * 60)
    print(f"📊 RESUMEN DE TESTS: {exitosos}/{total} exitosos ({porcentaje:.0f}%)")
    
    if exitosos == total:
        print("🎉 TODOS LOS TESTS EXITOSOS - RECORDSET EXPORTER READY FOR API")
        print("🚀 Listo para implementar FastAPI endpoints")
        print("🔌 Integración Laravel completamente funcional")
    else:
        print(f"❌ {total - exitosos} TESTS FALLARON")
        print("🔧 Revisar implementación antes de usar en producción")
    
    print("=" * 60)
    
    return exitosos == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 