from .base_processor import BaseProcessor
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class CalculosSicossProcessor(BaseProcessor):
    """Procesador especializado para cálculos de SICOSS"""
    
    def process(self, df_legajos: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Aplica cálculos principales de SICOSS"""
        logger.info("Aplicando cálculos de SICOSS...")
        
        df = df_legajos.copy()
        
        # Asegurar que existan las columnas base necesarias
        df = self._asegurar_columnas_base(df)
        
        # Cálculos base
        df = self._calcular_remuneracion_base(df)
        df = self._calcular_importes_imponibles(df)
        df = self._calcular_importes_brutos(df)
        df = self._inicializar_campos_adicionales(df)
        
        self._log_process_info("CalculosSicossProcessor", len(df_legajos), len(df))
        return df
    
    def _asegurar_columnas_base(self, df: pd.DataFrame) -> pd.DataFrame:
        """Asegura que existan las columnas base necesarias"""
        columnas_requeridas = {
            'ImporteSAC': 0.0,
            'ImporteNoRemun': 0.0,
            'ImporteHorasExtras': 0.0,
            'ImporteZonaDesfavorable': 0.0,
            'ImporteVacaciones': 0.0,
            'ImportePremios': 0.0,
            'ImporteOtros': 0.0
        }
        
        for columna, valor_default in columnas_requeridas.items():
            if columna not in df.columns:
                df[columna] = valor_default
                logger.debug(f"Agregada columna faltante: {columna} = {valor_default}")
        
        return df
    
    def _calcular_remuneracion_base(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula remuneración base tipo C"""
        df['Remuner78805'] = df['ImporteSAC']
        df['AsignacionesFliaresPagadas'] = 0.0
        df['ImporteImponiblePatronal'] = df['Remuner78805']
        return df
    
    def _calcular_importes_imponibles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula importes imponibles"""
        df['ImporteSACPatronal'] = df['ImporteSAC']
        df['ImporteImponibleSinSAC'] = (
            df['ImporteImponiblePatronal'] - df['ImporteSACPatronal']
        )
        df['IMPORTE_IMPON'] = df['Remuner78805']
        return df
    
    def _calcular_importes_brutos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula importes brutos"""
        df['IMPORTE_BRUTO'] = (
            df['ImporteImponiblePatronal'] + df.get('ImporteNoRemun', 0)
        )
        return df
    
    def _inicializar_campos_adicionales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Inicializa campos adicionales requeridos"""
        campos_adicionales = {
            'DiferenciaSACImponibleConTope': 0.0,
            'DiferenciaImponibleConTope': 0.0,
            'ImporteSACNoDocente': 0.0,
            'ImporteImponible_4': 0.0,
            'ImporteImponible_5': 0.0,
            'ImporteImponible_6': 0.0,
            'TipoDeOperacion': 1,
            'ImporteSueldoMasAdicionales': 0.0,
            'ImporteSACOtraActividad': 0.0,
            'ImporteSACOtroAporte': 0.0
        }
        
        for campo, valor in campos_adicionales.items():
            if campo not in df.columns:
                df[campo] = valor
        
        # Asignar valores específicos basados en campos existentes
        df['ImporteSACNoDocente'] = df['ImporteSAC']
        df['ImporteImponible_4'] = df['IMPORTE_IMPON']
        df['ImporteImponible_5'] = df['ImporteImponible_4']
        df['ImporteImponible_6'] = df['ImporteImponible_5']
        df['ImporteSueldoMasAdicionales'] = df['IMPORTE_BRUTO']
        df['ImporteSACOtroAporte'] = df['ImporteSAC']
        
        return df 