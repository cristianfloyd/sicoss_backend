from .base_extractor import BaseExtractor
import pandas as pd
import logging
from typing import List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from queries.sicoss_queries import SicossSQLQueries

logger = logging.getLogger(__name__)

class ConceptosExtractor(BaseExtractor):
    """Extractor especializado para conceptos liquidados"""
    
    def __init__(self, db_connection):
        super().__init__(db_connection)
        self.queries = SicossSQLQueries()
    
    def extract(self, per_anoct: int, per_mesct: int, 
                where_legajo: str = "true", **kwargs) -> pd.DataFrame:
        """Extrae conceptos liquidados"""
        self._validate_params(['per_anoct', 'per_mesct'], 
                            per_anoct=per_anoct, per_mesct=per_mesct)
        
        logger.info(f"Extrayendo conceptos para período {per_anoct}/{per_mesct}")
        
        query = self.queries.get_conceptos_liquidados_query(per_anoct, per_mesct, where_legajo)
        return self.db.execute_query(query)
    
    def extract_for_legajos(self, per_anoct: int, per_mesct: int, 
                           legajos: List[int]) -> pd.DataFrame:
        """Extrae conceptos para legajos específicos"""
        if not legajos:
            return pd.DataFrame()
        
        legajos_str = ','.join(map(str, legajos))
        where_clause = f"dh21.nro_legaj IN ({legajos_str})"
        return self.extract(per_anoct, per_mesct, where_clause) 