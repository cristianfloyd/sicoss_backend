from .base_extractor import BaseExtractor
import pandas as pd
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from queries.sicoss_queries import SicossSQLQueries

logger = logging.getLogger(__name__)

class LegajosExtractor(BaseExtractor):
    """Extractor especializado para datos de legajos"""
    
    def __init__(self, db_connection):
        super().__init__(db_connection)
        self.queries = SicossSQLQueries()
    
    def extract(self, per_anoct: int, per_mesct: int, 
                where_legajo: str = "true", **kwargs) -> pd.DataFrame:
        """Extrae datos básicos de legajos"""
        self._validate_params(['per_anoct', 'per_mesct'], 
                            per_anoct=per_anoct, per_mesct=per_mesct)
        
        logger.info(f"Extrayendo legajos para período {per_anoct}/{per_mesct}")
        
        query = self.queries.get_legajos_query(per_anoct, per_mesct, "'REPA'", where_legajo)
        return self.db.execute_query(query)
    
    def extract_for_legajo(self, per_anoct: int, per_mesct: int, 
                          nro_legajo: int) -> pd.DataFrame:
        """Extrae datos para un legajo específico"""
        where_clause = f"dh01.nro_legaj = {nro_legajo}"
        return self.extract(per_anoct, per_mesct, where_clause) 