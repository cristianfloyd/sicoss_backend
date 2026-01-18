#!/usr/bin/env python3
"""
Test script para verificar las clases refactorizadas de SICOSS

Prueba la integración entre:
- Extractors especializados
- Processors especializados  
- Manager coordinator
"""

import logging
import time
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from extractors.data_extractor_manager import DataExtractorManager
from processors.sicoss_processor import SicossDataProcessor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_refactored_sicoss():
    """
    Test completo de las clases refactorizadas
    """
    logger.info("🚀 === INICIANDO TEST DE CLASES REFACTORIZADAS ===")
    
    try:
        # 1. Configuración
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=700000.0,
            trunca_tope=True,
            check_lic=False,
            check_retro=False,
            check_sin_activo=False,
            asignacion_familiar=False,
            trabajador_convencionado="S"
        )
        
        logger.info("✅ Configuración creada")
        
        # 2. Conexión a BD
        db = DatabaseConnection('database.ini')
        logger.info("✅ Conexión a BD establecida")
        
        # 3. Inicializar Manager de Extractors
        extractor_manager = DataExtractorManager(db)
        logger.info("✅ ExtractorManager inicializado")
        
        # 4. Inicializar Processor Principal
        processor = SicossDataProcessor(config)
        logger.info("✅ SicossDataProcessor inicializado")
        
        # 5. Test de Extracción
        logger.info("📊 === INICIANDO EXTRACCIÓN ===")
        inicio_extraccion = time.time()
        
        datos_extraidos = extractor_manager.extraer_datos_completos(
            config=config,
            per_anoct=2025,
            per_mesct=6,
            nro_legajo=110830  # Cambiar por un legajo que exista
        )
        
        tiempo_extraccion = time.time() - inicio_extraccion
        logger.info(f"✅ Extracción completada en {tiempo_extraccion:.2f}s")
        
        # 6. Test de Procesamiento
        logger.info("⚙️ === INICIANDO PROCESAMIENTO ===")
        inicio_procesamiento = time.time()
        
        resultado = processor.procesar_datos_extraidos(datos_extraidos)
        
        tiempo_procesamiento = time.time() - inicio_procesamiento
        logger.info(f"✅ Procesamiento completado en {tiempo_procesamiento:.2f}s")
        
        # 7. Mostrar Resultados
        _mostrar_resultados_test(resultado, tiempo_extraccion, tiempo_procesamiento)
        
        logger.info("🎉 === TEST DE CLASES REFACTORIZADAS EXITOSO ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en test de clases refactorizadas: {e}")
        logger.exception("Detalles del error:")
        return False

def test_componentes_individuales():
    """
    Test individual de cada componente
    """
    logger.info("🔧 === TEST DE COMPONENTES INDIVIDUALES ===")
    
    try:
        # Test 1: SicossConfig
        logger.info("🔧 Test SicossConfig...")
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=700000.0,
            trunca_tope=True
        )
        
        # Verificar propiedades calculadas
        assert config.tope_sac_jubilatorio_patr == 400000.0
        assert config.tope_sac_jubilatorio_pers == 300000.0
        logger.info("✅ SicossConfig OK")
        
        # Test 2: DatabaseConnection
        logger.info("🔧 Test DatabaseConnection...")
        db = DatabaseConnection('database.ini')
        # Verificar que tenga engine
        assert hasattr(db, 'engine')
        logger.info("✅ DatabaseConnection OK")
        
        # Test 3: Extractors individuales
        logger.info("🔧 Test Extractors individuales...")
        from extractors.legajos_extractor import LegajosExtractor
        from extractors.conceptos_extractor import ConceptosExtractor
        
        legajos_extractor = LegajosExtractor(db)
        conceptos_extractor = ConceptosExtractor(db)
        
        assert hasattr(legajos_extractor, 'extract')
        assert hasattr(conceptos_extractor, 'extract')
        logger.info("✅ Extractors OK")
        
        # Test 4: Processors individuales
        logger.info("🔧 Test Processors individuales...")
        from processors.conceptos_processor import ConceptosProcessor
        from processors.calculos_processor import CalculosSicossProcessor
        from processors.topes_processor import TopesProcessor
        from processors.validator import LegajosValidator
        
        conceptos_proc = ConceptosProcessor(config)
        calculos_proc = CalculosSicossProcessor(config)
        topes_proc = TopesProcessor(config)
        validator = LegajosValidator(config)
        
        assert hasattr(conceptos_proc, 'process')
        assert hasattr(calculos_proc, 'process')
        assert hasattr(topes_proc, 'process')
        assert hasattr(validator, 'validate')
        logger.info("✅ Processors OK")
        
        # Test 5: Utils
        logger.info("🔧 Test Utils...")
        from utils.statistics import EstadisticasHelper
        
        stats_helper = EstadisticasHelper()
        assert hasattr(stats_helper, 'calcular_totales')
        assert hasattr(stats_helper, 'calcular_estadisticas_procesamiento')
        logger.info("✅ Utils OK")
        
        # Test 6: Queries
        logger.info("🔧 Test Queries...")
        from queries.sicoss_queries import SicossSQLQueries
        
        queries = SicossSQLQueries()
        assert hasattr(queries, 'get_legajos_query')
        assert hasattr(queries, 'get_conceptos_liquidados_query')
        logger.info("✅ Queries OK")
        
        logger.info("🎉 === TODOS LOS COMPONENTES INDIVIDUALES OK ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en test de componentes: {e}")
        logger.exception("Detalles del error:")
        return False

def _mostrar_resultados_test(resultado, tiempo_extraccion, tiempo_procesamiento):
    """Muestra los resultados del test"""
    logger.info("📊 === RESULTADOS DEL TEST ===")
    
    # Tiempos
    logger.info(f"⏱️ TIEMPOS:")
    logger.info(f"  📊 Extracción: {tiempo_extraccion:.2f}s")
    logger.info(f"  ⚙️ Procesamiento: {tiempo_procesamiento:.2f}s")
    logger.info(f"  🏁 Total: {tiempo_extraccion + tiempo_procesamiento:.2f}s")
    
    # Estadísticas
    stats = resultado['estadisticas']
    logger.info(f"📈 ESTADÍSTICAS:")
    logger.info(f"  📋 Total legajos: {stats['total_legajos']}")
    logger.info(f"  ✅ Legajos válidos: {stats['legajos_validos']}")
    logger.info(f"  ❌ Legajos rechazados: {stats['legajos_rechazados']}")
    logger.info(f"  📊 % Aprobación: {stats.get('porcentaje_aprobacion', 0):.1f}%")
    
    # Totales
    totales = resultado['totales']
    logger.info(f"💰 TOTALES:")
    for concepto, valor in totales.items():
        if valor > 0:
            logger.info(f"  💰 {concepto}: ${valor:,.2f}")
    
    logger.info("=" * 50)

if __name__ == "__main__":
    print("🚀 INICIANDO TESTS DE CLASES REFACTORIZADAS")
    
    # Test componentes individuales
    test1_ok = test_componentes_individuales()
    
    # Test integración completa
    test2_ok = test_refactored_sicoss()
    
    # Resultado final
    if test1_ok and test2_ok:
        print("🎉 ¡TODOS LOS TESTS PASARON EXITOSAMENTE!")
        print("✅ Las clases refactorizadas están funcionando correctamente")
    else:
        print("❌ Algunos tests fallaron. Revisar logs para detalles.")
        
    print("🏁 Tests completados.") 