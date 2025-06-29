import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class EstadisticasHelper:
    """Helper para cálculos de estadísticas y totales de SICOSS"""
    
    def calcular_totales(self, df_legajos: pd.DataFrame) -> Dict[str, float]:
        """Calcula totales para el informe de control"""
        if df_legajos.empty:
            return self.crear_totales_vacios()
        
        # Asegurar que las columnas existen
        columnas_requeridas = {
            'IMPORTE_BRUTO': 0.0,
            'IMPORTE_IMPON': 0.0,
            'ImporteImponiblePatronal': 0.0,
            'ImporteImponible_4': 0.0,
            'ImporteImponible_5': 0.0,
            'ImporteImponible_6': 0.0,
            'Remuner78805': 0.0,
            'importeimponible_9': 0.0
        }
        
        for columna, valor_default in columnas_requeridas.items():
            if columna not in df_legajos.columns:
                df_legajos[columna] = valor_default
        
        totales = {
            'bruto': float(df_legajos['IMPORTE_BRUTO'].sum()),
            'imponible_1': float(df_legajos['IMPORTE_IMPON'].sum()),
            'imponible_2': float(df_legajos['ImporteImponiblePatronal'].sum()),
            'imponible_4': float(df_legajos['ImporteImponible_4'].sum()),
            'imponible_5': float(df_legajos['ImporteImponible_5'].sum()),
            'imponible_6': float(df_legajos['ImporteImponible_6'].sum()),
            'imponible_8': float(df_legajos['Remuner78805'].sum()),
            'imponible_9': float(df_legajos['importeimponible_9'].sum())
        }
        
        logger.info(f"📊 Totales calculados para {len(df_legajos)} legajos")
        return totales
    
    def calcular_estadisticas_procesamiento(self, df_original: pd.DataFrame, 
                                           df_validos: pd.DataFrame) -> Dict[str, Any]:
        """Calcula estadísticas del procesamiento"""
        total_legajos = len(df_original)
        legajos_validos = len(df_validos)
        legajos_rechazados = total_legajos - legajos_validos
        
        porcentaje_aprobacion = (legajos_validos / total_legajos * 100) if total_legajos > 0 else 0.0
        
        estadisticas = {
            'total_legajos': total_legajos,
            'legajos_validos': legajos_validos,
            'legajos_rechazados': legajos_rechazados,
            'porcentaje_aprobacion': round(porcentaje_aprobacion, 2)
        }
        
        logger.info(f"📈 Estadísticas: {legajos_validos}/{total_legajos} válidos ({porcentaje_aprobacion:.1f}%)")
        return estadisticas
    
    def crear_totales_vacios(self) -> Dict[str, float]:
        """Crea diccionario de totales vacíos"""
        return {
            'bruto': 0.0,
            'imponible_1': 0.0,
            'imponible_2': 0.0,
            'imponible_4': 0.0,
            'imponible_5': 0.0,
            'imponible_6': 0.0,
            'imponible_8': 0.0,
            'imponible_9': 0.0
        }
    
    def mostrar_estadisticas_detalladas(self, datos: Dict[str, pd.DataFrame]):
        """Muestra estadísticas detalladas de extracción"""
        logger.info("=== ESTADÍSTICAS DETALLADAS ===")
        
        for nombre, df in datos.items():
            if isinstance(df, pd.DataFrame):
                logger.info(f"{nombre.upper()}: {len(df)} registros")
                
                if not df.empty and 'nro_legaj' in df.columns:
                    legajos_unicos = df['nro_legaj'].nunique()
                    logger.info(f"  - Legajos únicos: {legajos_unicos}")
                    
                    if nombre == 'conceptos':
                        conceptos_por_legajo = df.groupby('nro_legaj').size()
                        logger.info(f"  - Promedio conceptos/legajo: {conceptos_por_legajo.mean():.1f}")
                        logger.info(f"  - Máximo conceptos/legajo: {conceptos_por_legajo.max()}")
                        
                        if 'impp_conce' in df.columns:
                            total_importes = df['impp_conce'].sum()
                            logger.info(f"  - Total importes: ${total_importes:,.2f}")
        
        logger.info("=" * 40)
    
    def validar_integridad_datos(self, df_legajos: pd.DataFrame, 
                                df_conceptos: pd.DataFrame) -> Dict[str, Any]:
        """Valida la integridad entre legajos y conceptos"""
        logger.info("🔍 Validando integridad de datos...")
        
        integridad = {
            'legajos_sin_conceptos': 0,
            'conceptos_sin_legajo': 0,
            'legajos_con_conceptos': 0,
            'es_integro': True,
            'warnings': []
        }
        
        if df_legajos.empty or df_conceptos.empty:
            integridad['warnings'].append("DataFrames vacíos")
            return integridad
        
        # Legajos que no tienen conceptos
        legajos_ids = set(df_legajos['nro_legaj'].unique())
        conceptos_legajos_ids = set(df_conceptos['nro_legaj'].unique())
        
        legajos_sin_conceptos = legajos_ids - conceptos_legajos_ids
        conceptos_sin_legajo = conceptos_legajos_ids - legajos_ids
        
        integridad['legajos_sin_conceptos'] = len(legajos_sin_conceptos)
        integridad['conceptos_sin_legajo'] = len(conceptos_sin_legajo)
        integridad['legajos_con_conceptos'] = len(legajos_ids & conceptos_legajos_ids)
        
        if legajos_sin_conceptos:
            integridad['warnings'].append(f"{len(legajos_sin_conceptos)} legajos sin conceptos")
            integridad['es_integro'] = False
        
        if conceptos_sin_legajo:
            integridad['warnings'].append(f"{len(conceptos_sin_legajo)} conceptos huérfanos")
            integridad['es_integro'] = False
        
        logger.info(f"✅ Integridad: {integridad['legajos_con_conceptos']} legajos con conceptos")
        
        for warning in integridad['warnings']:
            logger.warning(f"⚠️ {warning}")
        
        return integridad 