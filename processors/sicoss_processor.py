from .conceptos_processor import ConceptosProcessor
from .calculos_processor import CalculosSicossProcessor
from .topes_processor import TopesProcessor
from .validator import LegajosValidator
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.statistics import EstadisticasHelper
from exporters.recordset_exporter import SicossRecordsetExporter
from value_objects.periodo_fiscal import PeriodoFiscal
import pandas as pd
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class PipelineStep:
    """Definición de un paso del pipeline"""
    name: str
    function: Callable
    required_columns: List[str]
    validate_result: bool = True
    critical: bool = True

@dataclass
class ProcessingMetrics:
    """Métricas de procesamiento"""
    total_time: float = 0.0
    step_times: Dict[str, float] = field(default_factory=dict)
    input_records: int = 0
    output_records: int = 0
    error_count: int = 0
    warnings: List[str] = field(default_factory=list)

class SicossDataProcessor:
    """
    Coordinador principal del procesamiento de datos SICOSS
    Orquesta los diferentes procesadores especializados con máximo control y robustez
    """
    
    def __init__(self, config):
        self.config = config
        self._initialize_processors()
        self._initialize_pipeline()
        self.stats_helper = EstadisticasHelper()
        self.metrics = ProcessingMetrics()
        # ✅ Inicializar exportador de recordsets para API
        self.recordset_exporter = SicossRecordsetExporter()
        # 🚧 TODO: Inicializar guardador de BD
        self._database_saver = None  # Se inicializa bajo demanda
        
    def _initialize_processors(self):
        """Inicializa todos los procesadores especializados"""
        try:
            logger.info("🔧 Inicializando procesadores especializados...")
            
            self.conceptos_processor = ConceptosProcessor(self.config)
            self.calculos_processor = CalculosSicossProcessor(self.config)
            self.topes_processor = TopesProcessor(self.config)
            self.validator = LegajosValidator(self.config)
            
            logger.info("✅ Todos los procesadores inicializados correctamente")
            
        except Exception as e:
            logger.error(f"❌ Error inicializando procesadores: {e}")
            raise RuntimeError(f"Fallo en inicialización de procesadores: {e}")
    
    def _initialize_pipeline(self):
        """Define el pipeline de procesamiento con validaciones"""
        self.pipeline_steps = [
            PipelineStep(
                name="Sumarización de conceptos",
                function=self._procesar_conceptos,
                required_columns=['nro_legaj'],
                validate_result=True,
                critical=True
            ),
            PipelineStep(
                name="Agregar otra actividad",
                function=self._agregar_otra_actividad,
                required_columns=['nro_legaj'],
                validate_result=False,
                critical=False
            ),
            PipelineStep(
                name="Agregar obra social",
                function=self._agregar_obra_social,
                required_columns=['nro_legaj'],
                validate_result=False,
                critical=False
            ),
            PipelineStep(
                name="Aplicar cálculos SICOSS",
                function=self._aplicar_calculos,
                required_columns=['nro_legaj', 'ImporteImponiblePatronal'],
                validate_result=True,
                critical=True
            ),
            PipelineStep(
                name="Aplicar topes jubilatorios",
                function=self._aplicar_topes,
                required_columns=['nro_legaj', 'IMPORTE_IMPON'],
                validate_result=True,
                critical=True
            ),
            PipelineStep(
                name="Validar legajos finales",
                function=self._validar_legajos,
                required_columns=['nro_legaj'],
                validate_result=True,
                critical=True
            )
        ]
    
    def procesar_datos_extraidos(self, datos: Dict[str, pd.DataFrame], 
                               validate_input: bool = True,
                               guardar_en_bd: bool = False,
                               formato_respuesta: str = "completo",
                               periodo_fiscal: Optional[PeriodoFiscal] = None) -> Dict[str, Any]:
        """
        Procesa los datos extraídos coordinando todos los procesadores
        con máximo control de errores y validaciones
        
        Args:
            datos: Datos extraídos por DataExtractorManager
            validate_input: Si validar integridad de datos de entrada
            guardar_en_bd: Si guardar resultados en BD
            formato_respuesta: Formato de respuesta API ("completo", "resumen", "solo_totales")
            periodo_fiscal: Período fiscal para BD
        """
        logger.info("🚀 Iniciando procesamiento coordinado de datos SICOSS...")
        start_time = time.time()
        
        try:
            # 1. Validar entrada
            if validate_input:
                self._validate_input_data(datos)
            
            # 2. Preparar datos iniciales
            result_data = self._prepare_initial_data(datos)
            self.metrics.input_records = len(result_data['legajos'])
            
            # 3. Ejecutar pipeline paso a paso
            result_data = self._execute_pipeline(result_data)
            
            # 4. Generar resultado final
            final_result = self._generate_final_result(result_data)
            
            # 5. Calcular métricas finales
            self.metrics.total_time = time.time() - start_time
            self.metrics.output_records = len(final_result.get('legajos_procesados', pd.DataFrame()))
            
            # 6. ✅ Generar respuesta API estructurada
            final_result['api_response'] = self.recordset_exporter.exportar_para_laravel(
                final_result, formato_respuesta
            )
            
            # 7. 🚧 TODO: Guardar en BD si está solicitado (FUNCIONALIDAD PENDIENTE)
            if guardar_en_bd:
                final_result['guardado_bd'] = self._guardar_en_bd_sicoss(final_result, periodo_fiscal)
            
            self._log_final_metrics()
            
            logger.info("✅ Procesamiento coordinado completado exitosamente")
            
            # Agregar métricas al resultado
            final_result['metricas'] = self._get_metrics_summary()
            
            return final_result
            
        except Exception as e:
            self.metrics.error_count += 1
            self.metrics.total_time = time.time() - start_time
            
            logger.error(f"❌ Error en procesamiento coordinado: {e}")
            logger.exception("Detalles del error:")
            
            # Retornar resultado de emergencia
            return self._create_emergency_result(str(e))
    
    def _validate_input_data(self, datos: Dict[str, pd.DataFrame]):
        """Valida la integridad de los datos de entrada"""
        logger.info("🔍 Validando datos de entrada...")
        
        required_keys = ['legajos', 'conceptos', 'otra_actividad', 'obra_social']
        
        for key in required_keys:
            if key not in datos:
                raise ValueError(f"Datos de entrada incompletos: falta '{key}'")
            
            if not isinstance(datos[key], pd.DataFrame):
                raise TypeError(f"'{key}' debe ser un DataFrame")
        
        # Validar que legajos no esté vacío
        if datos['legajos'].empty:
            raise ValueError("No hay legajos para procesar")
        
        # Validar integridad entre legajos y conceptos
        integridad = self.stats_helper.validar_integridad_datos(
            datos['legajos'], datos['conceptos']
        )
        
        if not integridad['es_integro']:
            for warning in integridad['warnings']:
                self.metrics.warnings.append(f"Integridad: {warning}")
                logger.warning(f"⚠️ {warning}")
        
        logger.info("✅ Validación de entrada completada")
    
    def _prepare_initial_data(self, datos: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Prepara los datos iniciales con copias seguras"""
        logger.info("📋 Preparando datos iniciales...")
        
        return {
            'legajos': datos['legajos'].copy(),
            'conceptos': datos['conceptos'].copy(),
            'otra_actividad': datos['otra_actividad'].copy(),
            'obra_social': datos['obra_social'].copy()
        }
    
    def _execute_pipeline(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Ejecuta el pipeline paso a paso con control de errores"""
        logger.info("⚙️ Ejecutando pipeline de procesamiento...")
        
        for i, step in enumerate(self.pipeline_steps, 1):
            step_start = time.time()
            
            try:
                logger.info(f"📋 [{i}/{len(self.pipeline_steps)}] {step.name}...")
                
                # Validar prerequisitos
                self._validate_step_prerequisites(data, step)
                
                # Ejecutar paso
                data = step.function(data)
                
                # Validar resultado si es requerido
                if step.validate_result:
                    self._validate_step_result(data, step)
                
                # Registrar tiempo
                step_time = time.time() - step_start
                self.metrics.step_times[step.name] = step_time
                
                logger.info(f"✅ {step.name} completado en {step_time:.3f}s")
                
            except Exception as e:
                step_time = time.time() - step_start
                self.metrics.step_times[step.name] = step_time
                self.metrics.error_count += 1
                
                error_msg = f"Error en '{step.name}': {e}"
                logger.error(f"❌ {error_msg}")
                
                if step.critical:
                    logger.error(f"💥 Paso crítico fallido - Abortando procesamiento")
                    raise RuntimeError(error_msg)
                else:
                    logger.warning(f"⚠️ Paso no crítico fallido - Continuando...")
                    self.metrics.warnings.append(error_msg)
        
        return data
    
    def _validate_step_prerequisites(self, data: Dict, step: PipelineStep):
        """Valida que un paso tenga los prerequisitos necesarios"""
        df_legajos = data.get('legajos', pd.DataFrame())
        
        if df_legajos.empty:
            raise ValueError(f"No hay datos para procesar en paso '{step.name}'")
        
        # Verificar columnas requeridas
        missing_cols = [col for col in step.required_columns if col not in df_legajos.columns]
        if missing_cols:
            raise ValueError(f"Columnas faltantes para '{step.name}': {missing_cols}")
    
    def _validate_step_result(self, data: Dict, step: PipelineStep):
        """Valida el resultado de un paso"""
        df_legajos = data.get('legajos', pd.DataFrame())
        
        if df_legajos.empty:
            raise ValueError(f"Paso '{step.name}' resultó en datos vacíos")
        
        # Verificar que no se hayan perdido registros críticos
        if step.critical and len(df_legajos) == 0:
            raise ValueError(f"Paso crítico '{step.name}' eliminó todos los registros")
    
    def _procesar_conceptos(self, data: Dict) -> Dict:
        """Ejecuta procesamiento de conceptos con manejo de errores"""
        try:
            logger.debug("Procesando conceptos...")
            data['legajos'] = self.conceptos_processor.process(
                data['legajos'], data['conceptos']
            )
            
            # Validación específica post-conceptos
            required_fields = ['ImporteImponiblePatronal', 'IMPORTE_IMPON', 'IMPORTE_BRUTO']
            missing_fields = [f for f in required_fields if f not in data['legajos'].columns]
            
            if missing_fields:
                raise ValueError(f"ConceptosProcessor no generó campos requeridos: {missing_fields}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error en procesamiento de conceptos: {e}")
            raise
    
    def _agregar_otra_actividad(self, data: Dict) -> Dict:
        """Agrega datos de otra actividad con validaciones"""
        try:
            logger.debug("Agregando otra actividad...")
            df_legajos = data['legajos']
            df_otra = data['otra_actividad']
            
            if not df_otra.empty:
                # Validar que no haya duplicados
                if df_otra['nro_legaj'].duplicated().any():
                    logger.warning("⚠️ Duplicados en otra_actividad - tomando primero")
                    df_otra = df_otra.drop_duplicates('nro_legaj', keep='first')
                
                df_legajos = df_legajos.merge(df_otra, on='nro_legaj', how='left')
                logger.info(f"Otra actividad agregada para {len(df_otra)} legajos")
            
            # Llenar valores faltantes
            df_legajos = df_legajos.fillna({
                'ImporteBrutoOtraActividad': 0.0,
                'ImporteSACOtraActividad': 0.0
            })
            
            data['legajos'] = df_legajos
            return data
            
        except Exception as e:
            logger.error(f"Error agregando otra actividad: {e}")
            # Para pasos no críticos, continuar sin los datos
            self.metrics.warnings.append(f"Otra actividad omitida: {e}")
            return data
    
    def _agregar_obra_social(self, data: Dict) -> Dict:
        """Agrega códigos de obra social con validaciones"""
        try:
            logger.debug("Agregando obra social...")
            df_legajos = data['legajos']
            df_os = data['obra_social']
            
            if not df_os.empty:
                # Validar que no haya duplicados
                if df_os['nro_legaj'].duplicated().any():
                    logger.warning("⚠️ Duplicados en obra_social - tomando primero")
                    df_os = df_os.drop_duplicates('nro_legaj', keep='first')
                
                df_legajos = df_legajos.merge(df_os, on='nro_legaj', how='left')
                logger.info(f"Obra social agregada para {len(df_os)} legajos")
            
            # Valor por defecto
            df_legajos['codigo_os'] = df_legajos.get('codigo_os', '000000').fillna('000000')
            
            data['legajos'] = df_legajos
            return data
            
        except Exception as e:
            logger.error(f"Error agregando obra social: {e}")
            # Para pasos no críticos, continuar con valor por defecto
            data['legajos']['codigo_os'] = '000000'
            self.metrics.warnings.append(f"Obra social omitida: {e}")
            return data
    
    def _aplicar_calculos(self, data: Dict) -> Dict:
        """Aplica cálculos de SICOSS con validaciones"""
        try:
            logger.debug("Aplicando cálculos SICOSS...")
            
            before_count = len(data['legajos'])
            data['legajos'] = self.calculos_processor.process(data['legajos'])
            after_count = len(data['legajos'])
            
            if after_count != before_count:
                logger.warning(f"⚠️ CalculosProcessor cambió cantidad de registros: {before_count} → {after_count}")
            
            # Validar campos críticos generados
            required_fields = ['ImporteImponible_4', 'ImporteImponible_5', 'importeimponible_9']
            missing_fields = [f for f in required_fields if f not in data['legajos'].columns]
            
            if missing_fields:
                raise ValueError(f"CalculosProcessor no generó campos críticos: {missing_fields}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error en cálculos SICOSS: {e}")
            raise
    
    def _aplicar_topes(self, data: Dict) -> Dict:
        """Aplica topes jubilatorios con validaciones"""
        try:
            logger.debug("Aplicando topes jubilatorios...")
            
            before_count = len(data['legajos'])
            importe_antes = data['legajos']['IMPORTE_IMPON'].sum()
            
            data['legajos'] = self.topes_processor.process(data['legajos'])
            
            after_count = len(data['legajos'])
            importe_despues = data['legajos']['IMPORTE_IMPON'].sum()
            
            if after_count != before_count:
                logger.warning(f"⚠️ TopesProcessor cambió cantidad de registros: {before_count} → {after_count}")
            
            # Log del impacto de topes
            diferencia_topes = importe_antes - importe_despues
            if diferencia_topes > 0:
                logger.info(f"💰 Topes aplicados - Reducción: ${diferencia_topes:,.2f}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error aplicando topes: {e}")
            raise
    
    def _validar_legajos(self, data: Dict) -> Dict:
        """Valida legajos según criterios con métricas"""
        try:
            logger.debug("Validando legajos finales...")
            
            before_count = len(data['legajos'])
            data['legajos_validos'] = self.validator.validate(data['legajos'])
            after_count = len(data['legajos_validos'])
            
            rechazados = before_count - after_count
            if rechazados > 0:
                logger.info(f"📊 Validación: {after_count}/{before_count} válidos ({rechazados} rechazados)")
            
            return data
            
        except Exception as e:
            logger.error(f"Error en validación final: {e}")
            raise
    
    def _generate_final_result(self, data: Dict) -> Dict[str, Any]:
        """Genera el resultado final con totales y estadísticas"""
        logger.info("📊 Generando resultado final...")
        
        df_final = data.get('legajos_validos', pd.DataFrame())
        df_original = data.get('legajos', pd.DataFrame())
        
        # Calcular totales y estadísticas
        totales = self.stats_helper.calcular_totales(df_final)
        estadisticas = self.stats_helper.calcular_estadisticas_procesamiento(df_original, df_final)
        
        return {
            'legajos_procesados': df_final,
            'totales': totales,
            'estadisticas': estadisticas,
            'datos_intermedios': {
                'legajos_pre_validacion': data.get('legajos', pd.DataFrame()),
                'conceptos_originales': data.get('conceptos', pd.DataFrame())
            }
        }
    
    def _create_emergency_result(self, error_message: str) -> Dict[str, Any]:
        """Crea resultado de emergencia en caso de error crítico"""
        logger.warning("🆘 Creando resultado de emergencia...")
        
        return {
            'legajos_procesados': pd.DataFrame(),
            'totales': self.stats_helper.crear_totales_vacios(),
            'estadisticas': {
                'total_legajos': 0,
                'legajos_validos': 0,
                'legajos_rechazados': 0,
                'porcentaje_aprobacion': 0.0,
                'error': error_message
            },
            'metricas': self._get_metrics_summary()
        }
    
    def _log_final_metrics(self):
        """Log de métricas finales de procesamiento"""
        logger.info("📈 MÉTRICAS FINALES DE PROCESAMIENTO:")
        logger.info(f"  ⏱️ Tiempo total: {self.metrics.total_time:.3f}s")
        logger.info(f"  📊 Registros: {self.metrics.input_records} → {self.metrics.output_records}")
        
        if self.metrics.error_count > 0:
            logger.info(f"  ❌ Errores: {self.metrics.error_count}")
        
        if self.metrics.warnings:
            logger.info(f"  ⚠️ Advertencias: {len(self.metrics.warnings)}")
        
        # Tiempos por paso
        logger.info("  🔄 Tiempos por paso:")
        for step_name, step_time in self.metrics.step_times.items():
            percentage = (step_time / self.metrics.total_time * 100) if self.metrics.total_time > 0 else 0
            logger.info(f"    - {step_name}: {step_time:.3f}s ({percentage:.1f}%)")
    
    def _get_metrics_summary(self) -> Dict[str, Any]:
        """Obtiene resumen de métricas para el resultado"""
        return {
            'tiempo_total_segundos': round(self.metrics.total_time, 3),
            'registros_entrada': self.metrics.input_records,
            'registros_salida': self.metrics.output_records,
            'errores': self.metrics.error_count,
            'advertencias': len(self.metrics.warnings),
            'tiempos_por_paso': self.metrics.step_times.copy(),
            'warnings_detalle': self.metrics.warnings.copy()
        }
    
    def get_processor_status(self) -> Dict[str, Any]:
        """Obtiene el estado de todos los procesadores"""
        return {
            'conceptos_processor': {
                'clase': self.conceptos_processor.__class__.__name__,
                'config': hasattr(self.conceptos_processor, 'config')
            },
            'calculos_processor': {
                'clase': self.calculos_processor.__class__.__name__,
                'config': hasattr(self.calculos_processor, 'config')
            },
            'topes_processor': {
                'clase': self.topes_processor.__class__.__name__,
                'config': hasattr(self.topes_processor, 'config')
            },
            'validator': {
                'clase': self.validator.__class__.__name__,
                'config': hasattr(self.validator, 'config')
            }
        }
    
    def generar_respuesta_api(self, resultado: Dict[str, Any], formato: str = "completo") -> Dict[str, Any]:
        """
        ✅ Genera respuesta API estructurada para FastAPI/Laravel
        
        Args:
            resultado: Resultado del procesamiento SICOSS
            formato: Formato de respuesta ("completo", "resumen", "solo_totales")
            
        Returns:
            Dict: Respuesta estructurada para API
        """
        logger.info(f"🚀 Generando respuesta API en formato: {formato}")
        
        try:
            if formato == "fastapi":
                return self.recordset_exporter.generar_respuesta_fastapi(resultado)
            else:
                return self.recordset_exporter.exportar_para_laravel(resultado, formato)
                
        except Exception as e:
            logger.error(f"❌ Error generando respuesta API: {e}")
            # Retornar respuesta de error estructurada
            return {
                "success": False,
                "message": f"Error generando respuesta API: {str(e)}",
                "data": {},
                "metadata": {"error": True},
                "timestamp": datetime.now().isoformat()
            }
    

    
    def _get_database_saver(self):
        """
        🚧 TODO: Obtiene database_saver con inicialización lazy
        
        Returns:
            SicossDatabaseSaver: Instancia del guardador de BD
        """
        if self._database_saver is None:
            logger.info("🚧 Inicializando SicossDatabaseSaver bajo demanda...")
            from .database_saver import SicossDatabaseSaver
            self._database_saver = SicossDatabaseSaver(self.config)
            
        return self._database_saver
    
    def _guardar_en_bd_sicoss(self, resultado: Dict[str, Any], periodo_fiscal: Optional[PeriodoFiscal] = None) -> Dict[str, Any]:
        """
        🚧 TODO: Guarda resultados SICOSS en base de datos
        
        Args:
            resultado: Resultado del procesamiento
            periodo_fiscal: Período fiscal (opcional)
            
        Returns:
            Dict: Resultado del guardado en BD
            
        FUNCIONALIDAD PENDIENTE - PLACEHOLDER PARA TESTING
        """
        logger.info("🚧 TODO: Guardando en BD SICOSS - FUNCIONALIDAD PENDIENTE")
        
        try:
            # Determinar período fiscal
            if periodo_fiscal is None:
                periodo_fiscal = PeriodoFiscal.current()
                logger.info(f"📅 Usando período actual: {periodo_fiscal}")
            
            # Obtener legajos procesados
            legajos_procesados = resultado.get('legajos_procesados', pd.DataFrame())
            
            if legajos_procesados.empty:
                logger.warning("⚠️ No hay legajos procesados para guardar en BD")
                return {
                    'success': False,
                    'message': 'No hay datos para guardar en BD',
                    'legajos_guardados': 0
                }
            
            # 🚧 TODO: Usar database_saver para guardado
            database_saver = self._get_database_saver()
            resultado_bd = database_saver.guardar_en_bd(
                legajos=legajos_procesados,
                periodo_fiscal=periodo_fiscal,
                incluir_inactivos=False
            )
            
            logger.info(f"💾 BD guardado (simulado): {resultado_bd.get('legajos_guardados', 0)} legajos")
            return resultado_bd
            
        except Exception as e:
            logger.error(f"❌ Error guardando en BD: {e}")
            # No fallar el procesamiento por error en BD
            return {
                'success': False,
                'message': f'Error en guardado BD: {str(e)}',
                'legajos_guardados': 0,
                'error': str(e)
            }
    
    def generar_sicoss_bd(self, datos: Dict[str, pd.DataFrame], 
                         periodo_fiscal: Optional[PeriodoFiscal] = None,
                         incluir_inactivos: bool = False) -> Dict[str, Any]:
        """
        🚧 TODO: Genera SICOSS directo a BD sin archivos
        
        Replica la funcionalidad generar_sicoss_bd() del PHP legacy
        
        Args:
            datos: Datos extraídos para procesamiento
            periodo_fiscal: Período fiscal (opcional, usa actual si no se especifica)
            incluir_inactivos: Si incluir legajos inactivos
            
        Returns:
            Dict con resultado del procesamiento y guardado en BD
        """
        logger.info("🚧 TODO: Generando SICOSS directo a BD - FUNCIONALIDAD PENDIENTE")
        
        try:
            # Determinar período fiscal
            if periodo_fiscal is None:
                periodo_fiscal = PeriodoFiscal.current()
            
            # Procesar datos normalmente + guardar en BD
            resultado = self.procesar_datos_extraidos(
                datos=datos,
                guardar_en_bd=True,
                periodo_fiscal=periodo_fiscal
            )
            
            # Agregar información específica de BD
            resultado.update({
                'metodo': 'generar_sicoss_bd',
                'periodo_fiscal': periodo_fiscal.to_dict(),
                'incluir_inactivos': incluir_inactivos,
                'directo_a_bd': True
            })
            
            logger.info(f"✅ Generación SICOSS a BD completada para {periodo_fiscal}")
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Error en generación SICOSS BD: {e}")
            return {
                'success': False,
                'message': f'Error en generación BD: {str(e)}',
                'error': str(e)
            }
    
    def verificar_estructura_bd(self, periodo_fiscal: Optional[PeriodoFiscal] = None) -> Dict[str, Any]:
        """
        🚧 TODO: Verifica estructura de datos en BD
        
        Args:
            periodo_fiscal: Período a verificar (opcional)
            
        Returns:
            Dict con resultado de la verificación
        """
        logger.info("🚧 TODO: Verificando estructura BD - FUNCIONALIDAD PENDIENTE")
        
        try:
            if periodo_fiscal is None:
                periodo_fiscal = PeriodoFiscal.current()
            
            database_saver = self._get_database_saver()
            return database_saver.verificar_estructura_datos(periodo_fiscal)
            
        except Exception as e:
            logger.error(f"❌ Error verificando estructura BD: {e}")
            return {
                'estructura_valida': False,
                'error': str(e)
            } 