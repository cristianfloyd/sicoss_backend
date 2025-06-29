from .base_processor import BaseProcessor
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class LegajosValidator(BaseProcessor):
    """Validador especializado para legajos según criterios SICOSS"""
    
    def process(self, df_legajos: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Este método no se usa en el validator, usamos validate directamente"""
        return self.validate(df_legajos)
    
    def validate(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Valida legajos según criterios de SICOSS"""
        logger.info("Validando legajos según criterios SICOSS...")
        
        if df_legajos.empty:
            return df_legajos
        
        # Verificar que tenga importes distintos de cero o situaciones especiales
        campos_importes = ['IMPORTE_BRUTO', 'IMPORTE_IMPON', 'ImporteImponiblePatronal']
        
        # Asegurar que las columnas existen
        for campo in campos_importes:
            if campo not in df_legajos.columns:
                df_legajos[campo] = 0.0
        
        mask_importes_validos = (
            df_legajos[campos_importes].sum(axis=1) > 0
        )
        
        # Situaciones especiales (maternidad, etc.)
        if 'codigosituacion' in df_legajos.columns:
            mask_situaciones_especiales = df_legajos['codigosituacion'].isin([5, 11])
        else:
            mask_situaciones_especiales = pd.Series([False] * len(df_legajos), index=df_legajos.index)
        
        # Licencias si está activado el check
        if hasattr(self.config, 'check_lic') and self.config.check_lic:
            if 'licencia' in df_legajos.columns:
                mask_licencias = (df_legajos['licencia'] == 1)
            else:
                mask_licencias = pd.Series([False] * len(df_legajos), index=df_legajos.index)
        else:
            mask_licencias = pd.Series([False] * len(df_legajos), index=df_legajos.index)
        
        # Situación reserva de puesto
        if 'codigosituacion' in df_legajos.columns:
            mask_reserva_puesto = df_legajos['codigosituacion'] == 14
        else:
            mask_reserva_puesto = pd.Series([False] * len(df_legajos), index=df_legajos.index)
        
        # Combinar todas las condiciones
        mask_validos = (
            mask_importes_validos | 
            mask_situaciones_especiales | 
            mask_licencias | 
            mask_reserva_puesto
        )
        
        df_validos = df_legajos[mask_validos].copy()
        
        self._log_process_info("LegajosValidator", len(df_legajos), len(df_validos))
        
        logger.info(f"✅ Validación completada: {len(df_validos)}/{len(df_legajos)} legajos válidos")
        
        return df_validos #type: ignore