from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Clase base para todos los extractores"""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """Método abstracto para extraer datos"""
        pass
    
    def _validate_params(self, required_params: List[str], **kwargs):
        """Valida parámetros requeridos"""
        missing = [param for param in required_params if param not in kwargs]
        if missing:
            raise ValueError(f"Parámetros faltantes: {missing}") 