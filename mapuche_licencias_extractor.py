import pandas as pd
import psycopg2
import logging
from typing import List, Dict, Optional, Any
from mapuche_config import MapucheConfig  # Importar la clase de configuración

class MapucheLicenciasExtractor:
    """
    Extractor de licencias de protección integral y vacaciones desde base de datos Mapuche
    """
    
    def __init__(self, connection_params: Dict[str, str]):
        """
        Inicializa el extractor con parámetros de conexión a la base de datos Mapuche
        
        Args:
            connection_params: Diccionario con host, database, user, password, port
        """
        self.connection_params = connection_params
        self.config = MapucheConfig(connection_params)
        self.logger = logging.getLogger(__name__)
    
    def get_licencias_protecintegral_vacaciones(self, where_legajos: str) -> pd.DataFrame:
        """
        Obtiene licencias de protección integral y vacaciones
        
        Args:
            where_legajos: Condición WHERE para filtrar legajos (ej: "dh01.nro_legaj IN (1,2,3)")
            
        Returns:
            DataFrame con las licencias encontradas
        """
        # Obtener parámetros de configuración usando la nueva clase
        fecha_inicio = self.config.get_fecha_inicio_periodo_corriente()
        fecha_fin = self.config.get_fecha_fin_periodo_corriente()
        variantes_vacaciones = self.config.get_var_licencia_vacaciones()
        variantes_protecintegral = self.config.get_var_licencia_protec_integral()
        
        # Si no hay variantes configuradas, retornar DataFrame vacío
        if not variantes_vacaciones and not variantes_protecintegral:
            return pd.DataFrame(columns=['nro_legaj', 'inicio', 'final', 'es_legajo', 'condicion'])
        
        # Construir condiciones WHERE dinámicamente
        where_vacaciones = ""
        where_protecintegral = ""
        
        if variantes_vacaciones and variantes_protecintegral:
            where_vacaciones = f" WHEN dh05.nrovarlicencia IN ({variantes_vacaciones}) THEN '12'::integer "
            where_protecintegral = f" WHEN dh05.nrovarlicencia IN ({variantes_protecintegral}) THEN '51'::integer "
            variantes = f"{variantes_vacaciones},{variantes_protecintegral}"
            where_legajos += f" AND dh05.nrovarlicencia IN ({variantes})"
        elif variantes_vacaciones:
            where_vacaciones = f" WHEN dh05.nrovarlicencia IN ({variantes_vacaciones}) THEN '12'::integer "
            where_legajos += f" AND dh05.nrovarlicencia IN ({variantes_vacaciones})"
        elif variantes_protecintegral:
            where_protecintegral = f" WHEN dh05.nrovarlicencia IN ({variantes_protecintegral}) THEN '51'::integer "
            where_legajos += f" AND dh05.nrovarlicencia IN ({variantes_protecintegral})"
        
        # Resto del código SQL igual que antes...
        sql = f"""
        SELECT
            dh01.nro_legaj,
            CASE
                WHEN dh05.fec_desde <= %s::date THEN date_part('day', timestamp %s)::integer
                ELSE date_part('day', dh05.fec_desde::timestamp)
            END AS inicio,
            CASE
                WHEN dh05.fec_hasta > %s::date OR dh05.fec_hasta IS NULL THEN date_part('day', timestamp %s)::integer
                ELSE date_part('day', dh05.fec_hasta::timestamp)
            END AS final,
            TRUE AS es_legajo,
            CASE
              WHEN dl02.es_maternidad THEN '5'::integer
              {where_vacaciones}
              {where_protecintegral}
              ELSE '13'::integer
            END AS condicion
        FROM
            mapuche.dh05
            LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
            LEFT OUTER JOIN mapuche.dh01 ON (dh05.nro_legaj = dh01.nro_legaj)
        WHERE
            dh05.nro_legaj IS NOT NULL
            AND (fec_desde <= %s::date AND (fec_hasta is null OR fec_hasta >= %s::date))
            AND mapuche.map_es_licencia_vigente(dh05.nro_licencia)
            AND dl02.es_remunerada = TRUE
            AND {where_legajos}

        UNION

        SELECT
            dh01.nro_legaj,
            CASE
                WHEN dh05.fec_desde <= %s::date THEN date_part('day', timestamp %s)::integer
                ELSE date_part('day', dh05.fec_desde::timestamp)
            END AS inicio,
            CASE
                WHEN dh05.fec_hasta > %s::date OR dh05.fec_hasta IS NULL THEN date_part('day', timestamp %s)::integer
                ELSE date_part('day', dh05.fec_hasta::timestamp)
            END AS final,
            FALSE AS es_legajo,
            CASE
              WHEN dl02.es_maternidad THEN '5'::integer
              {where_vacaciones}
              {where_protecintegral}
              ELSE '13'::integer
            END AS condicion
        FROM
            mapuche.dh05
            LEFT OUTER JOIN mapuche.dh03 ON (dh03.nro_cargo = dh05.nro_cargo)
            LEFT OUTER JOIN mapuche.dh01 ON (dh03.nro_legaj = dh01.nro_legaj)
            LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
        WHERE
            dh05.nro_cargo IS NOT NULL
            AND (fec_desde <= %s::date AND (fec_hasta is null OR fec_hasta >= %s::date))
            AND mapuche.map_es_cargo_activo(dh05.nro_cargo)
            AND mapuche.map_es_licencia_vigente(dh05.nro_licencia)
            AND dl02.es_remunerada = TRUE
            AND {where_legajos}
        """
        
        # Parámetros para la consulta
        params = [
            fecha_inicio, fecha_inicio,  # Para el primer CASE
            fecha_fin, fecha_fin,        # Para el segundo CASE
            fecha_fin, fecha_inicio,     # Para la condición WHERE de la primera parte del UNION
            fecha_inicio, fecha_inicio,  # Para el primer CASE de la segunda parte del UNION
            fecha_fin, fecha_fin,        # Para el segundo CASE de la segunda parte del UNION
            fecha_fin, fecha_inicio      # Para la condición WHERE de la segunda parte del UNION
        ]
        
        # Ejecutar la consulta y retornar DataFrame
        with psycopg2.connect(**self.connection_params) as conn:
            df = pd.read_sql_query(sql, conn, params=params)
            
            # Convertir tipos de datos apropiados
            df['nro_legaj'] = df['nro_legaj'].astype('int32')
            df['inicio'] = df['inicio'].astype('int32')
            df['final'] = df['final'].astype('int32')
            df['condicion'] = df['condicion'].astype('int32')
            df['es_legajo'] = df['es_legajo'].astype('bool')
            
            return df



    def get_licencias_protecintegral_vacaciones(self, where_legajos: str) -> List[Dict[str, Any]]:
        """
        Obtiene licencias de protección integral y vacaciones
        Traduce exactamente el método PHP get_licencias_protecintegral_vacaciones
        
        Args:
            where_legajos: Condición WHERE para filtrar legajos (ej: "dh01.nro_legaj IN (1,2,3)")
            
        Returns:
            Lista de diccionarios con las licencias encontradas
        """
        try:
            # Obtener fechas de inicio y fin del período corriente
            fecha_inicio = self.config.get_fecha_inicio_periodo_corriente()
            fecha_fin = self.config.get_fecha_fin_periodo_corriente()
            
            # Obtener variantes de licencias desde configuración
            variantes_vacaciones = self.config.get_var_licencia_vacaciones()
            variantes_protecintegral = self.config.get_var_licencia_protec_integral()
            
            # Si no hay variantes configuradas, retornar lista vacía (igual que PHP)
            if not variantes_vacaciones and not variantes_protecintegral:
                return []
            
            # Variables para construir las condiciones WHERE dinámicamente
            where_vacaciones = ""
            where_protecintegral = ""
            
            # Lógica condicional exacta del PHP original
            if variantes_vacaciones and variantes_protecintegral:
                where_vacaciones = f" WHEN dh05.nrovarlicencia IN ({variantes_vacaciones}) THEN '12'::integer "
                where_protecintegral = f" WHEN dh05.nrovarlicencia IN ({variantes_protecintegral}) THEN '51'::integer "
                variantes = f"{variantes_vacaciones},{variantes_protecintegral}"
                where_legajos += f" AND dh05.nrovarlicencia IN ({variantes})"
            else:
                if variantes_vacaciones:
                    where_vacaciones = f" WHEN dh05.nrovarlicencia IN ({variantes_vacaciones}) THEN '12'::integer "
                    where_legajos += f" AND dh05.nrovarlicencia IN ({variantes_vacaciones})"
                elif variantes_protecintegral:
                    where_protecintegral = f" WHEN dh05.nrovarlicencia IN ({variantes_protecintegral}) THEN '51'::integer "
                    where_legajos += f" AND dh05.nrovarlicencia IN ({variantes_protecintegral})"
            
            # Construir la consulta SQL exacta del PHP original
            sql = f"""
            SELECT
                dh01.nro_legaj,
                CASE
                    WHEN dh05.fec_desde <= %s::date THEN date_part('day', timestamp %s)::integer
                    ELSE date_part('day', dh05.fec_desde::timestamp)
                END AS inicio,
                CASE
                    WHEN dh05.fec_hasta > %s::date OR dh05.fec_hasta IS NULL THEN date_part('day', timestamp %s)::integer
                    ELSE date_part('day', dh05.fec_hasta::timestamp)
                END AS final,
                TRUE AS es_legajo,
                CASE
                  WHEN dl02.es_maternidad THEN '5'::integer
                  {where_vacaciones}
                  {where_protecintegral}
                  ELSE '13'::integer
                END AS condicion
            FROM
                mapuche.dh05
                LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
                LEFT OUTER JOIN mapuche.dh01 ON (dh05.nro_legaj = dh01.nro_legaj)
            WHERE
                dh05.nro_legaj IS NOT NULL
                AND (fec_desde <= %s::date AND (fec_hasta is null OR fec_hasta >= %s::date))
                AND mapuche.map_es_licencia_vigente(dh05.nro_licencia)
                AND dl02.es_remunerada = TRUE
                AND {where_legajos}

            UNION

            SELECT
                dh01.nro_legaj,
                CASE
                    WHEN dh05.fec_desde <= %s::date THEN date_part('day', timestamp %s)::integer
                    ELSE date_part('day', dh05.fec_desde::timestamp)
                END AS inicio,
                CASE
                    WHEN dh05.fec_hasta > %s::date OR dh05.fec_hasta IS NULL THEN date_part('day', timestamp %s)::integer
                    ELSE date_part('day', dh05.fec_hasta::timestamp)
                END AS final,
                FALSE AS es_legajo,
                CASE
                  WHEN dl02.es_maternidad THEN '5'::integer
                  {where_vacaciones}
                  {where_protecintegral}
                  ELSE '13'::integer
                END AS condicion
            FROM
                mapuche.dh05
                LEFT OUTER JOIN mapuche.dh03 ON (dh03.nro_cargo = dh05.nro_cargo)
                LEFT OUTER JOIN mapuche.dh01 ON (dh03.nro_legaj = dh01.nro_legaj)
                LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
            WHERE
                dh05.nro_cargo IS NOT NULL
                AND (fec_desde <= %s::date AND (fec_hasta is null OR fec_hasta >= %s::date))
                AND mapuche.map_es_cargo_activo(dh05.nro_cargo)
                AND mapuche.map_es_licencia_vigente(dh05.nro_licencia)
                AND dl02.es_remunerada = TRUE
                AND {where_legajos}
            """
            
            # Parámetros para la consulta preparada
            params = [
                fecha_inicio, fecha_inicio,  # Para el primer CASE de la primera consulta
                fecha_fin, fecha_fin,        # Para el segundo CASE de la primera consulta
                fecha_fin, fecha_inicio,     # Para las condiciones WHERE de la primera consulta
                fecha_inicio, fecha_inicio,  # Para el primer CASE de la segunda consulta (UNION)
                fecha_fin, fecha_fin,        # Para el segundo CASE de la segunda consulta (UNION)
                fecha_fin, fecha_inicio      # Para las condiciones WHERE de la segunda consulta (UNION)
            ]
            
            # Ejecutar la consulta
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    resultados = cursor.fetchall()
                    
                    # Obtener nombres de columnas
                    column_names = [desc[0] for desc in cursor.description]
                    
                    # Convertir resultados a lista de diccionarios (igual que PHP array_map)
                    licencias = []
                    for row in resultados:
                        licencia_dict = dict(zip(column_names, row))
                        licencias.append(licencia_dict)
                    
                    return licencias
                    
        except Exception as e:
            self.logger.error(f"Error obteniendo licencias protecintegral/vacaciones: {str(e)}")
            return []
    
    def get_licencias_protecintegral_vacaciones_as_dataframe(self, where_legajos: str) -> pd.DataFrame:
        """
        Versión alternativa que retorna un DataFrame de pandas en lugar de lista de diccionarios
        
        Args:
            where_legajos: Condición WHERE para filtrar legajos
            
        Returns:
            DataFrame con las licencias encontradas
        """
        licencias_list = self.get_licencias_protecintegral_vacaciones(where_legajos)
        
        if not licencias_list:
            # Retornar DataFrame vacío con columnas esperadas
            return pd.DataFrame(columns=['nro_legaj', 'inicio', 'final', 'es_legajo', 'condicion'])
        
        # Convertir a DataFrame
        df = pd.DataFrame(licencias_list)
        
        # Convertir tipos de datos apropiados
        df['nro_legaj'] = df['nro_legaj'].astype('int32')
        df['inicio'] = df['inicio'].astype('int32')
        df['final'] = df['final'].astype('int32')
        df['condicion'] = df['condicion'].astype('int32')
        df['es_legajo'] = df['es_legajo'].astype('bool')
        
        return df


def create_licencias_extractor(connection_params: dict[str, str]) -> MapucheLicenciasExtractor:
    """
    Crear una instancia de MapucheLicenciasExtractor con la configuración proporcionada.

    Args:
        connection_params: Parámetros de conexión a la base de datos.

    Returns:
        Una instancia de MapucheLicenciasExtractor.
    """
    return MapucheLicenciasExtractor(connection_params)




# Ejemplo de uso combinado
def ejemplo_uso_completo():
    """
    Ejemplo de cómo usar ambas clases juntas
    """
    
    # Configuración de conexión
    connection_params = {
        'host': 'localhost',
        'database': 'mapuche_db',
        'user': 'usuario',
        'password': 'password',
        'port': '5432'
    }
    
    # Crear instancia de configuración
    config = MapucheConfig(connection_params)
    
    # Crear instancia de licencias
    extractor = create_licencias_extractor(connection_params)
    
    # Mostrar configuración actual
    print("=== Configuración Actual ===")
    print(f"Período fiscal: {config.get_periodo_fiscal()}")
    print(f"Variantes vacaciones: {config.get_var_licencia_vacaciones()}")
    print(f"Variantes protección integral: {config.get_var_licencia_protec_integral()}")
    
    # Obtener licencias
    where_legajos = "dh01.nro_legaj IN (100, 200, 300)"
    df_licencias = extractor.get_licencias_protecintegral_vacaciones(where_legajos)
    
    print(f"\n=== Resultado ===")
    print(f"Licencias encontradas: {len(df_licencias)}")
    print(df_licencias.head())
    
    return config, df_licencias

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ejemplo_uso_completo()