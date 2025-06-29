from .legajos_extractor import LegajosExtractor
from .conceptos_extractor import ConceptosExtractor
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from queries.sicoss_queries import SicossSQLQueries
from config.sicoss_config import SicossConfig
import pandas as pd
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class DataExtractorManager:
    """
    Manager que coordina todos los extractores especializados
    """
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.legajos_extractor = LegajosExtractor(db_connection)
        self.conceptos_extractor = ConceptosExtractor(db_connection)
        self.queries = SicossSQLQueries()
    
    def extraer_datos_completos(self, config: SicossConfig, 
                              per_anoct: int, per_mesct: int,
                              nro_legajo: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """Extrae todos los datos necesarios para procesar SICOSS"""
        logger.info(f"Iniciando extracción coordinada para período {per_anoct}/{per_mesct}")
        
        # Construir filtros
        where_legajo = "true"
        if nro_legajo:
            where_legajo = f"dh01.nro_legaj = {nro_legajo}"
        
        # 1. Extraer legajos
        df_legajos = self.legajos_extractor.extract(per_anoct, per_mesct, where_legajo)
        
        if df_legajos.empty:
            logger.warning("No se encontraron legajos para procesar")
            return self._crear_dataframes_vacios()
        
        # 2. Extraer conceptos
        legajos_ids = df_legajos['nro_legaj'].tolist()
        df_conceptos = self.conceptos_extractor.extract_for_legajos(per_anoct, per_mesct, legajos_ids)
        
        # 3. Extraer otra actividad
        df_otra_actividad = self._extraer_otra_actividad(legajos_ids)
        
        # 4. Extraer obra social
        df_obra_social = self._extraer_codigos_obra_social(legajos_ids)
        
        return {
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': df_otra_actividad,
            'obra_social': df_obra_social
        }
    
    def _extraer_otra_actividad(self, legajos: List[int]) -> pd.DataFrame:
        """Extrae datos de otra actividad"""
        if not legajos:
            return pd.DataFrame(columns=['nro_legaj', 'ImporteBrutoOtraActividad', 'ImporteSACOtraActividad']) #type: ignore
        
        query = self.queries.get_otra_actividad_query(legajos)
        return self.db.execute_query(query)
    
    def _extraer_codigos_obra_social(self, legajos: List[int]) -> pd.DataFrame:
        """Extrae códigos de obra social"""
        if not legajos:
            return pd.DataFrame(columns=['nro_legaj', 'codigo_os']) #type: ignore
        
        query = self.queries.get_codigos_obra_social_query(legajos)
        return self.db.execute_query(query)
    
    def _crear_dataframes_vacios(self) -> Dict[str, pd.DataFrame]:
        """Crea DataFrames vacíos con las columnas correctas"""
        return {
            'legajos': pd.DataFrame(columns=['nro_legaj', 'cuit', 'apyno']), #type: ignore
            'conceptos': pd.DataFrame(columns=['nro_legaj', 'codn_conce', 'impp_conce']), #type: ignore
            'otra_actividad': pd.DataFrame(columns=['nro_legaj', 'ImporteBrutoOtraActividad']), #type: ignore
            'obra_social': pd.DataFrame(columns=['nro_legaj', 'codigo_os']) #type: ignore
        } 