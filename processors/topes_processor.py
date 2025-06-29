from .base_processor import BaseProcessor
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class TopesProcessor(BaseProcessor):
    """Procesador especializado para aplicación de topes"""
    
    def process(self, df_legajos: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Aplica topes jubilatorios"""
        logger.info("Aplicando topes jubilatorios...")
        
        df = df_legajos.copy()
        
        if not self.config.trunca_tope:
            logger.info("Topes desactivados en configuración")
            return df
        
        # Aplicar topes
        df = self._aplicar_tope_sac_patronal(df)
        df = self._aplicar_tope_imponible_sin_sac(df)
        df = self._recalcular_importe_bruto(df)
        
        self._log_process_info("TopesProcessor", len(df_legajos), len(df))
        return df
    
    def _aplicar_tope_sac_patronal(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica tope SAC patronal"""
        tope = self.config.tope_sac_jubilatorio_patr
        
        mask_excede_tope = df['ImporteSAC'] > tope
        
        if mask_excede_tope.any():
            logger.info(f"Aplicando tope SAC patronal: {mask_excede_tope.sum()} legajos afectados")
            
            df.loc[mask_excede_tope, 'DiferenciaSACImponibleConTope'] = (
                df.loc[mask_excede_tope, 'ImporteSAC'] - tope
            )
            df.loc[mask_excede_tope, 'ImporteImponiblePatronal'] -= (
                df.loc[mask_excede_tope, 'DiferenciaSACImponibleConTope']
            )
            df.loc[mask_excede_tope, 'ImporteSACPatronal'] = tope
        
        return df
    
    def _aplicar_tope_imponible_sin_sac(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica tope imponible sin SAC"""
        tope = self.config.tope_jubilatorio_patronal
        
        mask_excede_tope = df['ImporteImponibleSinSAC'] > tope
        
        if mask_excede_tope.any():
            logger.info(f"Aplicando tope imponible: {mask_excede_tope.sum()} legajos afectados")
            
            df.loc[mask_excede_tope, 'DiferenciaImponibleConTope'] = (
                df.loc[mask_excede_tope, 'ImporteImponibleSinSAC'] - tope
            )
            df.loc[mask_excede_tope, 'ImporteImponiblePatronal'] -= (
                df.loc[mask_excede_tope, 'DiferenciaImponibleConTope']
            )
        
        return df
    
    def _recalcular_importe_bruto(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recalcula importe bruto después de aplicar topes"""
        df['IMPORTE_BRUTO'] = (
            df['ImporteImponiblePatronal'] + df.get('ImporteNoRemun', 0)
        )
        return df 