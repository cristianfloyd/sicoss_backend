import pandas as pd
import psycopg2
from typing import List, Dict, Optional, Any
import logging
from mapuche_config import MapucheConfig  # Importar la clase de configuración que creamos

class SicossOptimizado:
    """
    Clase que replica la funcionalidad de SicossOptimizado.php en Python
    """
    
    def __init__(self, connection_params: Dict[str, str]):
        """
        Inicializa la clase con parámetros de conexión a la base de datos Mapuche
        
        Args:
            connection_params: Diccionario con host, database, user, password, port
        """
        self.connection_params = connection_params
        self.config = MapucheConfig(connection_params)
        self.logger = logging.getLogger(__name__)
    







