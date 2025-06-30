"""
recordset_exporter.py

Exportador de datos SICOSS para respuestas API estructuradas
Optimizado para consumo desde FastAPI → Laravel PHP
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class SicossApiResponse:
    """Estructura estándar para respuestas API SICOSS"""
    success: bool
    message: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str

@dataclass
class EstadisticasRecord:
    """Estructura de estadísticas para API"""
    total_legajos: int
    legajos_procesados: int
    legajos_rechazados: int
    porcentaje_aprobacion: float
    tiempo_procesamiento_ms: float
    totales: Dict[str, float]
    distribucion: Dict[str, Dict[str, Any]]

class SicossRecordsetExporter:
    """
    Exportador de datos SICOSS optimizado para API responses
    
    Convierte resultados de procesamiento SICOSS a formatos JSON estructurados
    listos para consumo desde FastAPI → Laravel PHP
    """
    
    def __init__(self, include_debug_info: bool = False):
        """
        Inicializa el exportador
        
        Args:
            include_debug_info: Si incluir información de debug en responses
        """
        self.include_debug_info = include_debug_info
        logger.info("✅ SicossRecordsetExporter inicializado para API responses")
    
    def transformar_resultado_completo(self, resultado_sicoss: Dict[str, Any], 
                                     include_details: bool = True) -> SicossApiResponse:
        """
        Transforma resultado completo de SICOSS a respuesta API estructurada
        
        Args:
            resultado_sicoss: Resultado del procesamiento SICOSS
            include_details: Si incluir detalles completos de legajos
            
        Returns:
            SicossApiResponse: Respuesta estructurada para API
        """
        logger.info("🔄 Transformando resultado SICOSS a formato API...")
        
        try:
            # Validar que el resultado tiene la estructura mínima esperada
            if not isinstance(resultado_sicoss, dict):
                raise ValueError("resultado_sicoss debe ser un diccionario")
            
            # Extraer componentes del resultado
            legajos_df = resultado_sicoss.get('legajos_procesados', pd.DataFrame())
            estadisticas = resultado_sicoss.get('estadisticas', {})
            totales = resultado_sicoss.get('totales', {})
            metricas = resultado_sicoss.get('metricas', {})
            
            # Validar que tenemos datos mínimos
            if len(legajos_df) == 0 and not resultado_sicoss.get('datos_corruptos'):
                logger.warning("⚠️ No hay legajos procesados en el resultado")
            
            # Si hay datos corruptos explícitos, fallar
            if 'datos_corruptos' in resultado_sicoss:
                raise ValueError("Datos corruptos detectados en resultado_sicoss")
            
            # Generar recordset de legajos
            legajos_recordset = self._generar_recordset_legajos(legajos_df, include_details)
            
            # Generar estadísticas estructuradas
            estadisticas_record = self._generar_estadisticas_record(
                estadisticas, totales, metricas
            )
            
            # Construir respuesta API
            api_response = SicossApiResponse(
                success=True,
                message=f"Procesamiento SICOSS exitoso: {len(legajos_df)} legajos",
                data={
                    "legajos": legajos_recordset,
                    "estadisticas": asdict(estadisticas_record),
                    "resumen": self._generar_resumen_ejecutivo(estadisticas_record)
                },
                metadata={
                    "version": "1.0",
                    "backend": "sicoss_python",
                    "total_records": len(legajos_df),
                    "processing_time_ms": metricas.get('tiempo_total_segundos', 0) * 1000,
                    "include_details": include_details,
                    "debug_info": self._generar_debug_info() if self.include_debug_info else None
                },
                timestamp=datetime.now().isoformat()
            )
            
            logger.info("✅ Transformación a API completada exitosamente")
            return api_response
            
        except Exception as e:
            logger.error(f"❌ Error transformando resultado a API: {e}")
            return self._generar_respuesta_error(str(e))
    
    def _safe_get_value(self, series: pd.Series, key: str, default_value: Any, value_type: str) -> Any:
        """Helper para obtener valores de manera segura del Series"""
        try:
            value = series.get(key, default_value)
            if pd.isna(value) or value is None:
                return default_value
            
            if value_type == 'int':
                return int(float(value))  # Convertir via float primero
            elif value_type == 'float':
                return float(value)
            elif value_type == 'str':
                return str(value)
            else:
                return value
        except (ValueError, TypeError, AttributeError):
            return default_value
    
    def _generar_recordset_legajos(self, legajos_df: pd.DataFrame, 
                                 include_details: bool) -> List[Dict[str, Any]]:
        """
        Genera recordset de legajos optimizado para API
        
        Args:
            legajos_df: DataFrame con legajos procesados
            include_details: Si incluir todos los campos o solo principales
            
        Returns:
            List[Dict]: Lista de legajos en formato JSON-friendly
        """
        if legajos_df.empty:
            return []
        
        legajos_list = []
        
        # Campos adicionales si se solicitan detalles
        campos_adicionales = [
            'ImporteNoRemun', 'ImporteImponiblePatronal', 'Remuner78805',
            'AsignacionesFliaresPagadas', 'ImporteImponible_4', 'ImporteImponible_5',
            'ImporteImponible_6', 'ImporteImponible_8', 'ImporteImponible_9'
        ] if include_details else []
        
        for _, legajo in legajos_df.iterrows():
            legajo_record = {
                # Identificación
                "nro_legaj": self._safe_get_value(legajo, 'nro_legaj', 0, 'int'),
                "cuil": self._safe_get_value(legajo, 'cuil', '', 'str'),
                "apnom": self._safe_get_value(legajo, 'apnom', '', 'str'),
                
                # Importes principales (siempre incluidos)
                "bruto": self._safe_get_value(legajo, 'IMPORTE_BRUTO', 0.0, 'float'),
                "imponible": self._safe_get_value(legajo, 'IMPORTE_IMPON', 0.0, 'float'),
                "sac": self._safe_get_value(legajo, 'ImporteSAC', 0.0, 'float'),
                
                # Códigos
                "cod_situacion": self._safe_get_value(legajo, 'codigosituacion', 0, 'int'),
                "cod_actividad": self._safe_get_value(legajo, 'TipoDeActividad', 0, 'int'),
            }
            
            # Agregar campos adicionales si se solicitan
            if include_details:
                otros_campos = {}
                for campo in campos_adicionales:
                    if campo in legajo.index:
                        valor = legajo[campo]
                        # Convertir a tipos JSON-serializable
                        if pd.isna(valor) or valor is None:
                            otros_campos[campo] = None
                        elif isinstance(valor, (int, float)):
                            otros_campos[campo] = float(valor)
                        else:
                            otros_campos[campo] = str(valor)
                
                legajo_record["detalles"] = otros_campos
            
            legajos_list.append(legajo_record)
        
        logger.info(f"📊 Recordset generado: {len(legajos_list)} legajos")
        return legajos_list
    
    def _generar_estadisticas_record(self, estadisticas: Dict, totales: Dict, 
                                   metricas: Dict) -> EstadisticasRecord:
        """
        Genera estadísticas estructuradas para API
        
        Args:
            estadisticas: Estadísticas del procesamiento
            totales: Totales calculados
            metricas: Métricas de performance
            
        Returns:
            EstadisticasRecord: Estadísticas estructuradas
        """
        return EstadisticasRecord(
            total_legajos=estadisticas.get('total_legajos', 0),
            legajos_procesados=estadisticas.get('legajos_validos', 0),
            legajos_rechazados=estadisticas.get('legajos_rechazados', 0),
            porcentaje_aprobacion=estadisticas.get('porcentaje_aprobacion', 0.0),
            tiempo_procesamiento_ms=metricas.get('tiempo_total_segundos', 0) * 1000,
            totales={
                "bruto": float(totales.get('bruto', 0.0)),
                "imponible_1": float(totales.get('imponible_1', 0.0)),
                "imponible_2": float(totales.get('imponible_2', 0.0)),
                "imponible_4": float(totales.get('imponible_4', 0.0)),
                "imponible_5": float(totales.get('imponible_5', 0.0)),
                "sac": float(totales.get('sac', 0.0))
            },
            distribucion={
                "por_situacion": self._generar_distribucion_situacion(estadisticas),
                "por_actividad": self._generar_distribucion_actividad(estadisticas),
                "por_rango_bruto": self._generar_distribucion_rangos(totales)
            }
        )
    
    def _generar_distribucion_situacion(self, estadisticas: Dict) -> Dict[str, Any]:
        """Genera distribución por situación de revista"""
        return estadisticas.get('distribucion_situacion', {})
    
    def _generar_distribucion_actividad(self, estadisticas: Dict) -> Dict[str, Any]:
        """Genera distribución por tipo de actividad"""
        return estadisticas.get('distribucion_actividad', {})
    
    def _generar_distribucion_rangos(self, totales: Dict) -> Dict[str, Any]:
        """Genera distribución por rangos de bruto"""
        bruto_total = totales.get('bruto', 0.0)
        cantidad_legajos = totales.get('cantidad_legajos', 1)
        return {
            "total": bruto_total,
            "promedio_estimado": bruto_total / max(1, cantidad_legajos),
            "rangos": {}  # TODO: implementar rangos
        }
    
    def _generar_resumen_ejecutivo(self, estadisticas: EstadisticasRecord) -> Dict[str, Any]:
        """
        Genera resumen ejecutivo para dashboard Laravel
        
        Args:
            estadisticas: Estadísticas procesadas
            
        Returns:
            Dict: Resumen ejecutivo estructurado
        """
        return {
            "procesamiento": {
                "estado": "exitoso" if estadisticas.legajos_rechazados == 0 else "con_observaciones",
                "total_procesado": estadisticas.legajos_procesados,
                "porcentaje_exito": estadisticas.porcentaje_aprobacion,
                "tiempo_ms": estadisticas.tiempo_procesamiento_ms
            },
            "financiero": {
                "bruto_total": estadisticas.totales["bruto"],
                "imponible_principal": estadisticas.totales["imponible_1"],
                "sac_total": estadisticas.totales["sac"],
                "promedio_bruto": estadisticas.totales["bruto"] / max(1, estadisticas.legajos_procesados)
            },
            "alertas": self._generar_alertas(estadisticas)
        }
    
    def _generar_alertas(self, estadisticas: EstadisticasRecord) -> List[Dict[str, Any]]:
        """Genera alertas para el frontend Laravel"""
        alertas = []
        
        if estadisticas.legajos_rechazados > 0:
            alertas.append({
                "tipo": "warning",
                "mensaje": f"{estadisticas.legajos_rechazados} legajos rechazados",
                "detalle": "Revisar datos de entrada"
            })
        
        if estadisticas.tiempo_procesamiento_ms > 5000:  # 5 segundos
            alertas.append({
                "tipo": "info",
                "mensaje": "Procesamiento lento detectado",
                "detalle": f"Tiempo: {estadisticas.tiempo_procesamiento_ms:.0f}ms"
            })
        
        return alertas
    
    def _generar_debug_info(self) -> Optional[Dict[str, Any]]:
        """Genera información de debug si está habilitada"""
        if not self.include_debug_info:
            return None
        
        return {
            "python_version": "3.8+",
            "pandas_version": pd.__version__,
            "export_timestamp": datetime.now().isoformat(),
            "exporter_version": "1.0.0"
        }
    
    def _generar_respuesta_error(self, error_message: str) -> SicossApiResponse:
        """
        Genera respuesta de error estructurada
        
        Args:
            error_message: Mensaje de error
            
        Returns:
            SicossApiResponse: Respuesta de error para API
        """
        return SicossApiResponse(
            success=False,
            message=f"Error en procesamiento SICOSS: {error_message}",
            data={
                "error_details": error_message,
                "suggested_action": "Verificar datos de entrada y configuración"
            },
            metadata={
                "version": "1.0",
                "backend": "sicoss_python",
                "error": True
            },
            timestamp=datetime.now().isoformat()
        )
    
    def exportar_para_laravel(self, resultado_sicoss: Dict[str, Any], 
                            formato: str = "completo") -> Dict[str, Any]:
        """
        Exporta resultado optimizado específicamente para Laravel PHP
        
        Args:
            resultado_sicoss: Resultado del procesamiento
            formato: "completo", "resumen", "solo_totales"
            
        Returns:
            Dict: Estructura optimizada para Laravel
        """
        logger.info(f"📤 Exportando para Laravel en formato: {formato}")
        
        api_response = self.transformar_resultado_completo(
            resultado_sicoss, 
            include_details=(formato == "completo")
        )
        
        # Convertir a dict serializable para Laravel
        response_dict = asdict(api_response)
        
        # Optimizaciones específicas para Laravel
        if formato == "solo_totales":
            response_dict["data"] = {
                "totales": response_dict["data"]["estadisticas"]["totales"],
                "resumen": response_dict["data"]["resumen"]
            }
        elif formato == "resumen":
            response_dict["data"]["legajos"] = response_dict["data"]["legajos"][:100]  # Primeros 100
        
        logger.info("✅ Exportación para Laravel completada")
        return response_dict
    
    def generar_respuesta_fastapi(self, resultado_sicoss: Dict[str, Any]) -> Dict[str, Any]:
        """
        Genera respuesta optimizada para FastAPI
        
        Args:
            resultado_sicoss: Resultado del procesamiento SICOSS
            
        Returns:
            Dict: Respuesta lista para FastAPI JSON response
        """
        logger.info("🚀 Generando respuesta para FastAPI...")
        
        try:
            api_response = self.transformar_resultado_completo(resultado_sicoss)
            response_dict = asdict(api_response)
            
            # Optimizaciones específicas para FastAPI
            response_dict["api_version"] = "v1"
            response_dict["content_type"] = "application/json"
            
            logger.info("✅ Respuesta FastAPI generada exitosamente")
            return response_dict
            
        except Exception as e:
            logger.error(f"❌ Error generando respuesta FastAPI: {e}")
            error_response = self._generar_respuesta_error(str(e))
            return asdict(error_response) 