from .conceptos_processor import ConceptosProcessor
from .calculos_processor import CalculosSicossProcessor
from .topes_processor import TopesProcessor
from .validator import LegajosValidator
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.statistics import EstadisticasHelper
import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SicossDataProcessor:
    """
    Coordinador principal del procesamiento de datos SICOSS
    Orquesta los diferentes procesadores especializados
    """
    
    def __init__(self, config):
        self.config = config
        self.conceptos_processor = ConceptosProcessor(config)
        self.calculos_processor = CalculosSicossProcessor(config)
        self.topes_processor = TopesProcessor(config)
        self.validator = LegajosValidator(config)
        self.stats_helper = EstadisticasHelper()
    
    def procesar_datos_extraidos(self, datos: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Procesa los datos extraídos coordinando todos los procesadores
        """
        logger.info("🚀 Iniciando procesamiento coordinado de datos...")
        
        df_legajos = datos['legajos'].copy()
        df_conceptos = datos['conceptos']
        df_otra_actividad = datos['otra_actividad']
        df_obra_social = datos['obra_social']
        
        if df_legajos.empty:
            return self._crear_resultado_vacio()
        
        # Pipeline de procesamiento
        pipeline_steps = [
            ("Sumarización de conceptos", self._procesar_conceptos),
            ("Agregar otra actividad", self._agregar_otra_actividad),
            ("Agregar obra social", self._agregar_obra_social),
            ("Aplicar cálculos SICOSS", self._aplicar_calculos),
            ("Aplicar topes", self._aplicar_topes),
            ("Validar legajos", self._validar_legajos)
        ]
        
        # Ejecutar pipeline
        result_data = {
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': df_otra_actividad,
            'obra_social': df_obra_social
        }
        
        for step_name, step_func in pipeline_steps:
            logger.info(f"📋 Ejecutando: {step_name}")
            result_data = step_func(result_data)
        
        # Calcular totales y estadísticas finales
        df_final = result_data['legajos_validos']
        totales = self.stats_helper.calcular_totales(df_final)
        estadisticas = self.stats_helper.calcular_estadisticas_procesamiento(
            result_data['legajos'], df_final
        )
        
        logger.info("✅ Procesamiento coordinado completado")
        
        return {
            'legajos_procesados': df_final,
            'totales': totales,
            'estadisticas': estadisticas
        }
    
    def _procesar_conceptos(self, data: Dict) -> Dict:
        """Ejecuta procesamiento de conceptos"""
        data['legajos'] = self.conceptos_processor.process(
            data['legajos'], data['conceptos']
        )
        return data
    
    def _agregar_otra_actividad(self, data: Dict) -> Dict:
        """Agrega datos de otra actividad"""
        # Implementación simplificada - puede ser otro procesador
        df_legajos = data['legajos']
        df_otra = data['otra_actividad']
        
        if not df_otra.empty:
            df_legajos = df_legajos.merge(df_otra, on='nro_legaj', how='left')
        
        df_legajos = df_legajos.fillna({
            'ImporteBrutoOtraActividad': 0.0,
            'ImporteSACOtraActividad': 0.0
        })
        
        data['legajos'] = df_legajos
        return data
    
    def _agregar_obra_social(self, data: Dict) -> Dict:
        """Agrega códigos de obra social"""
        df_legajos = data['legajos']
        df_os = data['obra_social']
        
        if not df_os.empty:
            df_legajos = df_legajos.merge(df_os, on='nro_legaj', how='left')
        
        df_legajos['codigo_os'] = df_legajos.get('codigo_os', '000000').fillna('000000')
        
        data['legajos'] = df_legajos
        return data
    
    def _aplicar_calculos(self, data: Dict) -> Dict:
        """Aplica cálculos de SICOSS"""
        data['legajos'] = self.calculos_processor.process(data['legajos'])
        return data
    
    def _aplicar_topes(self, data: Dict) -> Dict:
        """Aplica topes jubilatorios"""
        data['legajos'] = self.topes_processor.process(data['legajos'])
        return data
    
    def _validar_legajos(self, data: Dict) -> Dict:
        """Valida legajos según criterios"""
        data['legajos_validos'] = self.validator.validate(data['legajos'])
        return data
    
    def _crear_resultado_vacio(self) -> Dict[str, Any]:
        """Crea resultado vacío"""
        return {
            'legajos_procesados': pd.DataFrame(),
            'totales': self.stats_helper.crear_totales_vacios(),
            'estadisticas': {
                'total_legajos': 0,
                'legajos_validos': 0,
                'legajos_rechazados': 0
            }
        } 