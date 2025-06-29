from .base_processor import BaseProcessor
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ConceptosProcessor(BaseProcessor):
    """Procesador especializado para sumarización de conceptos"""
    
    def process(self, df_legajos: pd.DataFrame, df_conceptos: pd.DataFrame, 
                **kwargs) -> pd.DataFrame:
        """Sumariza conceptos por legajo"""
        logger.info("Procesando sumarización de conceptos...")
        
        if df_conceptos.empty:
            return self._inicializar_columnas_importes(df_legajos)
        
        # Clasificar conceptos por tipo
        df_conceptos_clasificados = self._clasificar_conceptos(df_conceptos)
        
        # Sumarizar por legajo y tipo
        conceptos_sumarizados = self._sumarizar_por_legajo(df_conceptos_clasificados)
        
        # Merge con legajos
        result = df_legajos.merge(conceptos_sumarizados, on='nro_legaj', how='left')
        
        # Llenar valores faltantes
        result = self._llenar_valores_faltantes(result)
        
        self._log_process_info("ConceptosProcessor", len(df_legajos), len(result))
        return result
    
    def _clasificar_conceptos(self, df_conceptos: pd.DataFrame) -> pd.DataFrame:
        """Clasifica conceptos según tipo SICOSS"""
        df = df_conceptos.copy()
        df['tipo_sicoss'] = 'OTROS'
        
        # SAC - Códigos más amplios para capturar conceptos SAC
        mask_sac = (
            df['codn_conce'].isin([1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010]) |
            (df['tipo_conce'] == 'SAC') |
            df['codn_conce'].between(1000, 1099)  # Rango amplio para SAC
        )
        df.loc[mask_sac, 'tipo_sicoss'] = 'SAC'
        
        # Horas Extras
        mask_he = df['codn_conce'].between(2000, 2099)
        df.loc[mask_he, 'tipo_sicoss'] = 'HORAS_EXTRAS'
        
        # No Remunerativo
        mask_no_remun = df['codn_conce'].between(3000, 3099)
        df.loc[mask_no_remun, 'tipo_sicoss'] = 'NO_REMUN'
        
        # Zona Desfavorable
        mask_zona = df['codn_conce'].between(4000, 4099)
        df.loc[mask_zona, 'tipo_sicoss'] = 'ZONA_DESFAVORABLE'
        
        # Vacaciones
        mask_vac = df['codn_conce'].between(5000, 5099)
        df.loc[mask_vac, 'tipo_sicoss'] = 'VACACIONES'
        
        # Premios
        mask_premios = df['codn_conce'].between(6000, 6099)
        df.loc[mask_premios, 'tipo_sicoss'] = 'PREMIOS'
        
        return df
    
    def _sumarizar_por_legajo(self, df_conceptos: pd.DataFrame) -> pd.DataFrame:
        """Sumariza importes por legajo y tipo de concepto"""
        # Sumarizar por legajo y tipo
        pivot_result = df_conceptos.pivot_table(
            index='nro_legaj',
            columns='tipo_sicoss',
            values='impp_conce',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Renombrar columnas para que coincidan con lo esperado
        column_mapping = {
            'SAC': 'ImporteSAC',
            'HORAS_EXTRAS': 'ImporteHorasExtras',
            'NO_REMUN': 'ImporteNoRemun',
            'ZONA_DESFAVORABLE': 'ImporteZonaDesfavorable',
            'VACACIONES': 'ImporteVacaciones',
            'PREMIOS': 'ImportePremios',
            'OTROS': 'ImporteOtros'
        }
        
        # Renombrar columnas que existen
        existing_columns = {col: column_mapping[col] for col in pivot_result.columns if col in column_mapping}
        pivot_result.rename(columns=existing_columns, inplace=True)
        
        # Agregar columnas faltantes con ceros
        required_columns = list(column_mapping.values())
        for col in required_columns:
            if col not in pivot_result.columns:
                pivot_result[col] = 0.0
        
        return pivot_result
    
    def _inicializar_columnas_importes(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Inicializa columnas de importes con ceros"""
        columnas_importes = [
            'ImporteSAC', 'ImporteNoRemun', 'ImporteHorasExtras',
            'ImporteZonaDesfavorable', 'ImporteVacaciones', 'ImportePremios', 'ImporteOtros'
        ]
        df = df_legajos.copy()
        for col in columnas_importes:
            df[col] = 0.0
        return df
    
    def _llenar_valores_faltantes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Llena valores faltantes con ceros"""
        # Columnas específicas de importes
        columnas_importes = [
            'ImporteSAC', 'ImporteNoRemun', 'ImporteHorasExtras',
            'ImporteZonaDesfavorable', 'ImporteVacaciones', 'ImportePremios', 'ImporteOtros'
        ]
        
        for col in columnas_importes:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)
        
        return df 