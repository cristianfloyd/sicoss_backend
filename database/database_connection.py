"""
database_connection.py

Módulo para manejo de conexiones a la base de datos PostgreSQL
Extraído de SicossDataExtractor.py para mejor organización
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Optional, Any, Literal
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
    
    def execute_insert(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Ejecuta INSERT/UPDATE/DELETE y retorna filas afectadas
        
        Args:
            query: Query SQL de escritura
            params: Parámetros de la consulta
            
        Returns:
            int: Número de filas afectadas
        """
        logger.info(f"📝 Ejecutando INSERT: {query[:100]}...")
        
        try:
            with self.engine.begin() as connection:  # Auto-commit transaction
                if params:
                    result = connection.execute(text(query), params)
                else:
                    result = connection.execute(text(query))
                
                affected_rows = result.rowcount
                logger.info(f"✅ INSERT exitoso - {affected_rows} filas afectadas")
                return affected_rows
                
        except Exception as e:
            logger.error(f"❌ Error en INSERT: {e}")
            raise
    
    def execute_batch_insert(self, table_name: str, data: pd.DataFrame, 
                           if_exists: Literal['fail', 'replace', 'append'] = 'append', schema: str = 'public') -> int:
        """
        Ejecuta INSERT masivo usando pandas.to_sql
        
        Args:
            table_name: Nombre de la tabla destino
            data: DataFrame con los datos a insertar
            if_exists: Qué hacer si la tabla existe ('append', 'replace', 'fail')
            schema: Esquema de la BD (por defecto 'public')
            
        Returns:
            int: Número de filas insertadas
        """
        logger.info(f"📦 Ejecutando INSERT masivo en {schema}.{table_name}: {len(data)} filas")
        
        try:
            # Limpiar datos antes de insertar
            data_clean = self._clean_dataframe_for_insert(data)
            
            # Ejecutar inserción masiva
            affected_rows = data_clean.to_sql(
                name=table_name, 
                con=self.engine, 
                schema=schema,
                if_exists=if_exists, 
                index=False,
                method='multi',  # Optimización para múltiples filas
                chunksize=1000   # Procesar en chunks para evitar memory issues
            )
            
            # to_sql retorna None si es exitoso, usamos len(data) como proxy
            rows_inserted = len(data_clean) if affected_rows is None else affected_rows
            
            logger.info(f"✅ INSERT masivo exitoso - {rows_inserted} filas insertadas en {schema}.{table_name}")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"❌ Error en INSERT masivo en {schema}.{table_name}: {e}")
            raise
    
    def _clean_dataframe_for_insert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia DataFrame para inserción en BD
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame limpio para inserción
        """
        df_clean = df.copy()
        
        # Reemplazar NaN con None para que se conviertan en NULL
        df_clean = df_clean.where(pd.notna(df_clean), None)
        
        # Convertir tipos problemáticos
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Convertir a string y limpiar
                df_clean[col] = df_clean[col].astype(str).replace('nan', None)
            elif df_clean[col].dtype in ['int64', 'float64']:
                # Asegurar que los números sean válidos
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        return df_clean
    
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