"""
database_connection.py

Módulo para manejo de conexiones a la base de datos PostgreSQL
Extraído de SicossDataExtractor.py para mejor organización
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Optional, Any
import logging
from configparser import ConfigParser

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Maneja la conexión a la base de datos PostgreSQL"""
    
    def __init__(self, config_file: str = 'database.ini'):
        self.config = self._load_config(config_file)
        self.engine = self._create_engine()
    
    def _load_config(self, filename: str) -> Dict[str, str]:
        """Carga configuración de la base de datos"""
        parser = ConfigParser()
        parser.read(filename)
        
        config = {}
        if parser.has_section('postgresql'):
            params = parser.items('postgresql')
            for param in params:
                config[param[0]] = param[1]
        else:
            raise Exception(f'Section postgresql not found in {filename}')
        
        return config
    
    def _create_engine(self):
        """Crea el engine de SQLAlchemy"""
        db_name = self.config.get('dbname') or self.config.get('database')
        db_url = (f"postgresql+psycopg2://{self.config['user']}:"
                 f"{self.config['password']}@{self.config['host']}:"
                 f"{self.config.get('port', 5432)}/{db_name}")
        
        return create_engine(db_url, client_encoding='latin1')
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Ejecuta una consulta SQL y retorna un DataFrame"""
        try:
            logger.info(f"Ejecutando consulta: {query[:100]}...")
            return pd.read_sql_query(text(query), self.engine, params=params)
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            raise
    
    def close(self):
        """Cierra la conexión a la base de datos"""
        if hasattr(self, 'engine') and self.engine:
            self.engine.dispose()
            logger.info("Conexión a base de datos cerrada")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def test_connection(self) -> bool:
        """Prueba la conexión a la base de datos"""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                row = result.fetchone()
                if row and row[0] == 1:
                    logger.info("✅ Conexión a base de datos exitosa")
                    return True
            return False
        except Exception as e:
            logger.error(f"❌ Error al probar conexión: {e}")
            return False 