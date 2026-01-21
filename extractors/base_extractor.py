import logging
from abc import ABC, abstractmethod
from typing import Any, List

import pandas as pd

from database.database_connection import DatabaseConnection

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Clase base para todos los extractores"""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection

    @abstractmethod
    def extract(self, **kwargs: Any) -> pd.DataFrame:
        """Método abstracto para extraer datos"""
        pass

    def _validate_params(self, required_params: List[str], **kwargs: Any):
        """Valida parámetros requeridos"""
        missing = [param for param in required_params if param not in kwargs]
        if missing:
            raise ValueError(f"Parámetros faltantes: {missing}")
