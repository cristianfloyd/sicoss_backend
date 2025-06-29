from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class BaseProcessor(ABC):
    """Clase base para todos los procesadores"""
    
    def __init__(self, config):
        self.config = config
    
    @abstractmethod
    def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Método abstracto para procesar datos"""
        pass
    
    def _log_process_info(self, process_name: str, input_rows: int, output_rows: int):
        """Log información del procesamiento"""
        logger.info(f"{process_name}: {input_rows} → {output_rows} registros") 