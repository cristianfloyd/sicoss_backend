"""
SicossDataExtractor.py

Extractor de datos para procesar SICOSS usando pandas y consultas SQL optimizadas
Simula el comportamiento de la clase SicossOptimizado.php

Autor: Asistente IA
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import dataclass
from configparser import ConfigParser

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SicossConfig:
    """Configuración para el procesamiento de SICOSS"""
    tope_jubilatorio_patronal: float
    tope_jubilatorio_personal: float
    tope_otros_aportes_personales: float
    trunca_tope: bool
    check_lic: bool = False
    check_retro: bool = False
    check_sin_activo: bool = False
    asignacion_familiar: bool = False
    trabajador_convencionado: str = "S"
    
    @property
    def tope_sac_jubilatorio_pers(self) -> float:
        return self.tope_jubilatorio_personal / 2
    
    @property 
    def tope_sac_jubilatorio_patr(self) -> float:
        return self.tope_jubilatorio_patronal / 2
    
    @property
    def tope_sac_jubilatorio_otro_ap(self) -> float:
        return self.tope_otros_aportes_personales / 2


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


class SicossSQLQueries:
    """Contiene las consultas SQL optimizadas extraídas de SicossOptimizado.php"""
    
    @staticmethod
    def get_legajos_query(per_anoct: int, per_mesct: int, 
                         codc_reparto: str = "'REPA'", 
                         where_legajo: str = "true") -> str:
        """
        Consulta optimizada para obtener legajos (basada en get_sql_legajos())
        """
        return f"""
        SELECT
            DISTINCT(dh01.nro_legaj),
            (dh01.nro_cuil1::char(2)||LPAD(dh01.nro_cuil::char(8),8,'0')||dh01.nro_cuil2::char(1))::float8 AS cuit,
            dh01.desc_appat||' '||dh01.desc_nombr AS apyno,
            dh01.tipo_estad AS estado,
            
            -- Optimización: reemplazar subconsultas con JOIN agregado
            COALESCE(familiares.conyugue, 0) AS conyugue,
            COALESCE(familiares.hijos, 0) AS hijos,
            
            dha8.ProvinciaLocalidad,
            dha8.codigosituacion,
            dha8.CodigoCondicion,
            dha8.codigozona,
            dha8.CodigoActividad,
            dha8.porcaporteadicss AS aporteAdicional,
            dha8.trabajador_convencionado AS trabajadorconvencionado,
            dha8.codigomodalcontrat AS codigocontratacion,
            
            CASE WHEN ((dh09.codc_bprev = {codc_reparto}) OR (dh09.fuerza_reparto) OR (({codc_reparto} = '') AND (dh09.codc_bprev IS NULL)))
                 THEN '1' ELSE '0' END AS regimen,
            
            dh09.cant_cargo AS adherentes,
            0 AS licencia,
            0 AS importeimponible_9

        FROM mapuche.dh01
        
        -- JOIN optimizado para contar familiares una sola vez
        LEFT JOIN (
            SELECT 
                nro_legaj,
                COUNT(CASE WHEN codc_paren = 'CONY' THEN 1 END) AS conyugue,
                COUNT(CASE WHEN codc_paren IN ('HIJO', 'HIJN', 'HINC', 'HINN') THEN 1 END) AS hijos
            FROM mapuche.dh02 
            WHERE sino_cargo != 'N'
            GROUP BY nro_legaj
        ) familiares ON familiares.nro_legaj = mapuche.dh01.nro_legaj

        LEFT OUTER JOIN mapuche.dha8 ON dha8.nro_legajo = mapuche.dh01.nro_legaj
        LEFT OUTER JOIN mapuche.dh09 ON dh09.nro_legaj = mapuche.dh01.nro_legaj
        LEFT OUTER JOIN mapuche.dhe9 ON dhe9.nro_legaj = mapuche.dh01.nro_legaj
        
        WHERE {where_legajo}
        ORDER BY dh01.nro_legaj
        """
    
    @staticmethod
    def get_conceptos_liquidados_query(per_anoct: int, per_mesct: int,
                                     where_legajo: str = "true") -> str:
        """
        Consulta optimizada para conceptos liquidados (basada en getConsultaConceptosOptimizada())
        """
        return f"""
        WITH tipos_grupos_conceptos AS (
            SELECT
                dh16.codn_conce,
                array_agg(DISTINCT dh15.codn_tipogrupo) AS tipos_grupos
            FROM mapuche.dh16
            INNER JOIN mapuche.dh15 ON dh15.codn_grupo = dh16.codn_grupo
            GROUP BY dh16.codn_conce
        )
        SELECT DISTINCT
            dh21.id_liquidacion,
            dh21.impp_conce,
            dh21.ano_retro,
            dh21.mes_retro,
            dh21.nro_legaj,
            dh21.codn_conce,
            dh21.tipo_conce,
            dh21.nro_cargo,
            dh21.nov1_conce,
            dh12.nro_orimp,
            COALESCE(tgc.tipos_grupos, ARRAY[]::integer[]) AS tipos_grupos,
            dh21.codigoescalafon
        FROM mapuche.dh21
        INNER JOIN mapuche.dh22 ON dh22.nro_liqui = dh21.nro_liqui
        LEFT JOIN mapuche.dh01 ON dh01.nro_legaj = dh21.nro_legaj
        LEFT JOIN mapuche.dh12 ON dh12.codn_conce = dh21.codn_conce
        LEFT JOIN tipos_grupos_conceptos tgc ON tgc.codn_conce = dh21.codn_conce
        WHERE dh22.per_liano = {per_anoct}
        AND dh22.per_limes = {per_mesct}
        AND dh22.sino_genimp = true
        AND dh21.codn_conce > 0
        AND {where_legajo}
        """
    
    @staticmethod
    def get_otra_actividad_query(legajos: List[int]) -> str:
        """
        Consulta para otra actividad (basada en otra_actividad())
        """
        legajos_str = ','.join(map(str, legajos))
        return f"""
        SELECT
			importe AS ImporteBrutoOtraActividad,
			importe_sac AS ImporteSACOtraActividad
		FROM
			mapuche.dhe9
		WHERE
			nro_legaj IN ({legajos_str})
		ORDER BY
			vig_ano, vig_mes DESC
		LIMIT 1
        """
    
    @staticmethod
    def get_codigos_obra_social_query(legajos: List[int]) -> str:
        """
        Consulta para códigos de obra social (basada en codigo_os())
        Siempre retorna '000000' para todos los legajos
        """
        legajos_str = ','.join(map(str, legajos))
        return f"""
        SELECT 
            nro_legaj,
            '000000' AS codigo_os
        FROM UNNEST(ARRAY[{legajos_str}]) AS nro_legaj
        """


class SicossDataExtractor:
    """
    Extractor principal que simula el flujo de SicossOptimizado.php usando pandas
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.sql_queries = SicossSQLQueries()
        
    def extraer_datos_completos(self, config: SicossConfig, 
                              per_anoct: int, per_mesct: int,
                              nro_legajo: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Extrae todos los datos necesarios para procesar SICOSS
        
        Returns:
            Dict con DataFrames: legajos, conceptos, otra_actividad, obra_social
        """
        logger.info(f"Iniciando extracción de datos para período {per_anoct}/{per_mesct}")
        
        # Construir filtro de legajo
        where_legajo = "true"
        if nro_legajo:
            where_legajo = f"dh01.nro_legaj = {nro_legajo}"
        
        # 1. Extraer legajos
        logger.info("Extrayendo datos de legajos...")
        df_legajos = self.extraer_legajos(per_anoct, per_mesct, where_legajo)
        
        if df_legajos.empty:
            logger.warning("No se encontraron legajos para procesar")
            return self.crear_dataframes_vacios()
        
        # 2. Extraer conceptos liquidados
        logger.info("Extrayendo conceptos liquidados...")
        where_legajo_conceptos = self.construir_where_conceptos(df_legajos['nro_legaj'].tolist())
        df_conceptos = self.extraer_conceptos_liquidados(per_anoct, per_mesct, where_legajo_conceptos)
        
        # 3. Extraer otra actividad
        logger.info("Extrayendo datos de otra actividad...")
        df_otra_actividad = self.extraer_otra_actividad(df_legajos['nro_legaj'].tolist())
        
        # 4. Extraer códigos de obra social
        logger.info("Extrayendo códigos de obra social...")
        df_obra_social = self.extraer_codigos_obra_social(df_legajos['nro_legaj'].tolist())
        
        # 5. Estadísticas de extracción
        self.mostrar_estadisticas_extraccion({
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': df_otra_actividad,
            'obra_social': df_obra_social
        })
        
        return {
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': df_otra_actividad,
            'obra_social': df_obra_social
        }
    
    def extraer_legajos(self, per_anoct: int, per_mesct: int, where_legajo: str) -> pd.DataFrame:
        """Extrae datos básicos de legajos"""
        query = self.sql_queries.get_legajos_query(per_anoct, per_mesct, "'REPA'", where_legajo)
        return self.db.execute_query(query)
    
    def extraer_conceptos_liquidados(self, per_anoct: int, per_mesct: int, where_legajo: str) -> pd.DataFrame:
        """Extrae conceptos liquidados con tipos de grupos"""
        query = self.sql_queries.get_conceptos_liquidados_query(per_anoct, per_mesct, where_legajo)
        return self.db.execute_query(query)
    
    def extraer_otra_actividad(self, legajos: List[int]) -> pd.DataFrame:
        """Extrae datos de otra actividad"""
        if not legajos:
            return pd.DataFrame(columns=['nro_legaj', 'importebrutootraactividad', 'importesacotraactividad'])
        
        query = self.sql_queries.get_otra_actividad_query(legajos)
        return self.db.execute_query(query)
    
    def extraer_codigos_obra_social(self, legajos: List[int]) -> pd.DataFrame:
        """Extrae códigos de obra social"""
        if not legajos:
            return pd.DataFrame(columns=['nro_legaj', 'codigo_os'])
        
        query = self.sql_queries.get_codigos_obra_social_query(legajos)
        return self.db.execute_query(query)

    def construir_where_conceptos(self, legajos: List[int]) -> str:
        """Construye cláusula WHERE para filtrar conceptos por legajos
        """
        if not legajos:
            return "false"

        legajos_str = ','.join(map(str, legajos))
        return f"dh21.nro_legaj IN ({legajos_str})"

    def crear_dataframes_vacios(self) -> Dict[str, pd.DataFrame]:
        """Crea DataFrames vacíos con las columnas correctas"""
        return {
            'legajos': pd.DataFrame(columns=[
                'nro_legaj', 'cuit', 'apyno', 'estado', 'conyugue', 'hijos',
                'provincialocalidad', 'codigosituacion', 'codigocondicion', 
                'codigozona', 'codigoactividad', 'aporteadicional',
                'trabajadorconvencionado', 'codigocontratacion', 'regimen',
                'adherentes', 'licencia', 'importeimponible_9'
            ]),
            'conceptos': pd.DataFrame(columns=[
                'id_liquidacion', 'impp_conce', 'ano_retro', 'mes_retro',
                'nro_legaj', 'codn_conce', 'tipo_conce', 'nro_cargo',
                'nov1_conce', 'nro_orimp', 'tipos_grupos', 'codigoescalafon'
            ]),
            'otra_actividad': pd.DataFrame(columns=[
                'nro_legaj', 'importebrutootraactividad', 'importesacotraactividad'
            ]),
            'obra_social': pd.DataFrame(columns=['nro_legaj', 'codigo_os'])
        }
    
    def mostrar_estadisticas_extraccion(self, dataframes: Dict[str, pd.DataFrame]):
        """Muestra estadísticas de la extracción de datos"""
        logger.info("=== ESTADÍSTICAS DE EXTRACCIÓN ===")
        for nombre, df in dataframes.items():
            logger.info(f"{nombre.upper()}: {len(df)} registros")
            
        # Estadísticas adicionales
        if not dataframes['conceptos'].empty:
            conceptos_por_legajo = dataframes['conceptos'].groupby('nro_legaj').size()
            logger.info(f"Promedio conceptos por legajo: {conceptos_por_legajo.mean():.1f}")
            logger.info(f"Máximo conceptos por legajo: {conceptos_por_legajo.max()}")


class SicossDataProcessor:
    """
    Procesador de datos extraídos usando pandas
    """
    
    def __init__(self, config: SicossConfig):
        self.config = config
    
    def procesar_datos_extraidos(self, datos: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Procesa los datos extraídos y simula procesa_sicoss()
        """
        logger.info("Iniciando procesamiento de datos extraídos...")
        
        df_legajos = datos['legajos'].copy()
        df_conceptos = datos['conceptos']
        df_otra_actividad = datos['otra_actividad']
        df_obra_social = datos['obra_social']
        
        if df_legajos.empty:
            return self._crear_resultado_vacio()
        
        # 1. Sumarizar conceptos por legajo
        df_legajos = self._sumarizar_conceptos_por_legajo(df_legajos, df_conceptos)
        
        # 2. Agregar datos de otra actividad
        df_legajos = self._agregar_otra_actividad(df_legajos, df_otra_actividad)
        
        # 3. Agregar códigos de obra social
        df_legajos = self._agregar_codigos_obra_social(df_legajos, df_obra_social)
        
        # 4. Aplicar cálculos de SICOSS
        df_legajos = self._aplicar_calculos_sicoss(df_legajos)
        
        # 5. Aplicar topes
        df_legajos = self._aplicar_topes(df_legajos)
        
        # 6. Validar legajos
        df_legajos_validos = self._validar_legajos(df_legajos)
        
        # 7. Calcular totales
        totales = self._calcular_totales(df_legajos_validos)
        
        return {
            'legajos_procesados': df_legajos_validos,
            'totales': totales,
            'estadisticas': {
                'total_legajos': len(df_legajos),
                'legajos_validos': len(df_legajos_validos),
                'legajos_rechazados': len(df_legajos) - len(df_legajos_validos)
            }
        }
    
    def _sumarizar_conceptos_por_legajo(self, df_legajos: pd.DataFrame, 
                                      df_conceptos: pd.DataFrame) -> pd.DataFrame:
        """Sumariza conceptos por legajo usando pandas groupby"""
        if df_conceptos.empty:
            # Inicializar columnas con ceros
            columnas_importes = [
                'ImporteSAC', 'ImporteNoRemun', 'ImporteHorasExtras',
                'ImporteZonaDesfavorable', 'ImporteVacaciones', 'ImportePremios',
                'ImporteAdicionales', 'IncrementoSolidario', 'ImporteImponibleBecario',
                'ImporteImponible_6', 'SACInvestigador', 'NoRemun4y8', 'ImporteTipo91',
                'ImporteNoRemun96'
            ]
            for col in columnas_importes:
                df_legajos[col] = 0.0
            return df_legajos
        
        # Agregar conceptos sumarizados por legajo
        conceptos_agrupados = df_conceptos.groupby('nro_legaj').agg({
            'impp_conce': 'sum'
        }).reset_index()
        
        # Simular la lógica de sumarización por tipos de grupos
        # (Esta es una versión simplificada - en la implementación real necesitarías
        # las reglas específicas de agrupación por tipos de conceptos)
        
        df_legajos = df_legajos.merge(
            conceptos_agrupados.rename(columns={'impp_conce': 'ImporteSAC'}),
            on='nro_legaj', how='left'
        )
        
        # Inicializar otros campos
        campos_inicializar = [
            'ImporteNoRemun', 'ImporteHorasExtras', 'ImporteZonaDesfavorable',
            'ImporteVacaciones', 'ImportePremios', 'ImporteAdicionales',
            'IncrementoSolidario', 'ImporteImponibleBecario', 'ImporteImponible_6',
            'SACInvestigador', 'NoRemun4y8', 'ImporteTipo91', 'ImporteNoRemun96'
        ]
        
        for campo in campos_inicializar:
            df_legajos[campo] = 0.0
        
        # Rellenar NAs
        df_legajos['ImporteSAC'] = df_legajos['ImporteSAC'].fillna(0.0)
        
        return df_legajos
    
    def _agregar_otra_actividad(self, df_legajos: pd.DataFrame, 
                               df_otra_actividad: pd.DataFrame) -> pd.DataFrame:
        """Agrega datos de otra actividad"""
        if df_otra_actividad.empty:
            df_legajos['ImporteBrutoOtraActividad'] = 0.0
            df_legajos['ImporteSACOtraActividad'] = 0.0
            return df_legajos
        
        return df_legajos.merge(
            df_otra_actividad.rename(columns={
                'importebrutootraactividad': 'ImporteBrutoOtraActividad',
                'importesacotraactividad': 'ImporteSACOtraActividad'
            }),
            on='nro_legaj', how='left'
        ).fillna({
            'ImporteBrutoOtraActividad': 0.0,
            'ImporteSACOtraActividad': 0.0
        })
    
    def _agregar_codigos_obra_social(self, df_legajos: pd.DataFrame,
                                   df_obra_social: pd.DataFrame) -> pd.DataFrame:
        """Agrega códigos de obra social"""
        if df_obra_social.empty:
            df_legajos['codigo_os'] = '000000'
            return df_legajos
        
        return df_legajos.merge(
            df_obra_social,
            on='nro_legaj', how='left'
        ).fillna({'codigo_os': '000000'})
    
    def _aplicar_calculos_sicoss(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Aplica los cálculos principales de SICOSS usando operaciones vectorizadas"""
        
        # Calcular remuneración tipo C (simplificado)
        df_legajos['Remuner78805'] = df_legajos['ImporteSAC']  # Simplificado
        df_legajos['AsignacionesFliaresPagadas'] = 0.0  # Simplificado
        df_legajos['ImporteImponiblePatronal'] = df_legajos['Remuner78805']
        
        # Calcular importes base
        df_legajos['ImporteSACPatronal'] = df_legajos['ImporteSAC']
        df_legajos['ImporteImponibleSinSAC'] = (
            df_legajos['ImporteImponiblePatronal'] - df_legajos['ImporteSACPatronal']
        )
        
        # Calcular IMPORTE_BRUTO
        df_legajos['IMPORTE_BRUTO'] = (
            df_legajos['ImporteImponiblePatronal'] + df_legajos['ImporteNoRemun']
        )
        
        # Calcular IMPORTE_IMPON (simplificado)
        df_legajos['IMPORTE_IMPON'] = df_legajos['Remuner78805']
        
        # Inicializar campos adicionales
        campos_adicionales = [
            'DiferenciaSACImponibleConTope', 'DiferenciaImponibleConTope',
            'ImporteSACNoDocente', 'ImporteImponible_4', 'ImporteImponible_5',
            'TipoDeOperacion', 'ImporteSueldoMasAdicionales'
        ]
        
        for campo in campos_adicionales:
            df_legajos[campo] = 0.0
        
        # Asignar valores específicos
        df_legajos['ImporteSACNoDocente'] = df_legajos['ImporteSAC']
        df_legajos['ImporteImponible_4'] = df_legajos['IMPORTE_IMPON']
        df_legajos['ImporteImponible_5'] = df_legajos['ImporteImponible_4']
        df_legajos['TipoDeOperacion'] = 1
        
        return df_legajos
    
    def _aplicar_topes(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Aplica topes jubilatorios usando operaciones vectorizadas"""
        
        # Aplicar tope SAC patronal
        mask_tope_sac = (
            (df_legajos['ImporteSAC'] > self.config.tope_sac_jubilatorio_patr) &
            (self.config.trunca_tope)
        )
        
        df_legajos.loc[mask_tope_sac, 'DiferenciaSACImponibleConTope'] = (
            df_legajos.loc[mask_tope_sac, 'ImporteSAC'] - self.config.tope_sac_jubilatorio_patr
        )
        df_legajos.loc[mask_tope_sac, 'ImporteImponiblePatronal'] -= (
            df_legajos.loc[mask_tope_sac, 'DiferenciaSACImponibleConTope']
        )
        df_legajos.loc[mask_tope_sac, 'ImporteSACPatronal'] = self.config.tope_sac_jubilatorio_patr
        
        # Aplicar tope imponible sin SAC
        mask_tope_imponible = (
            (df_legajos['ImporteImponibleSinSAC'] > self.config.tope_jubilatorio_patronal) &
            (self.config.trunca_tope)
        )
        
        df_legajos.loc[mask_tope_imponible, 'DiferenciaImponibleConTope'] = (
            df_legajos.loc[mask_tope_imponible, 'ImporteImponibleSinSAC'] - 
            self.config.tope_jubilatorio_patronal
        )
        df_legajos.loc[mask_tope_imponible, 'ImporteImponiblePatronal'] -= (
            df_legajos.loc[mask_tope_imponible, 'DiferenciaImponibleConTope']
        )
        
        # Recalcular IMPORTE_BRUTO después de aplicar topes
        df_legajos['IMPORTE_BRUTO'] = (
            df_legajos['ImporteImponiblePatronal'] + df_legajos['ImporteNoRemun']
        )
        
        return df_legajos
    
    def _validar_legajos(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Valida legajos según criterios de SICOSS"""
        
        # Verificar que tenga importes distintos de cero o situaciones especiales
        campos_importes = ['IMPORTE_BRUTO', 'IMPORTE_IMPON', 'ImporteImponiblePatronal']
        
        mask_importes_validos = (
            df_legajos[campos_importes].sum(axis=1) > 0
        )
        
        # Situaciones especiales (maternidad, etc.)
        mask_situaciones_especiales = df_legajos['codigosituacion'].isin([5, 11])
        
        # Licencias si está activado el check
        mask_licencias = (
            self.config.check_lic & (df_legajos['licencia'] == 1)
        )
        
        # Situación reserva de puesto
        mask_reserva_puesto = df_legajos['codigosituacion'] == 14
        
        # Combinear todas las condiciones
        mask_validos = (
            mask_importes_validos | 
            mask_situaciones_especiales | 
            mask_licencias | 
            mask_reserva_puesto
        )
        
        return df_legajos[mask_validos].copy()
    
    def _calcular_totales(self, df_legajos: pd.DataFrame) -> Dict[str, float]:
        """Calcula totales para el informe de control"""
        if df_legajos.empty:
            return self._crear_totales_vacios()
        
        return {
            'bruto': df_legajos['IMPORTE_BRUTO'].sum(),
            'imponible_1': df_legajos['IMPORTE_IMPON'].sum(),
            'imponible_2': df_legajos['ImporteImponiblePatronal'].sum(),
            'imponible_4': df_legajos['ImporteImponible_4'].sum(),
            'imponible_5': df_legajos['ImporteImponible_5'].sum(),
            'imponible_6': df_legajos['ImporteImponible_6'].sum(),
            'imponible_8': df_legajos['Remuner78805'].sum(),
            'imponible_9': df_legajos['importeimponible_9'].sum()
        }
    
    def _crear_resultado_vacio(self) -> Dict[str, Any]:
        """Crea resultado vacío"""
        return {
            'legajos_procesados': pd.DataFrame(),
            'totales': self._crear_totales_vacios(),
            'estadisticas': {
                'total_legajos': 0,
                'legajos_validos': 0,
                'legajos_rechazados': 0
            }
        }
    
    def _crear_totales_vacios(self) -> Dict[str, float]:
        """Crea totales vacíos"""
        return {
            'bruto': 0.0,
            'imponible_1': 0.0,
            'imponible_2': 0.0,
            'imponible_4': 0.0,
            'imponible_5': 0.0,
            'imponible_6': 0.0,
            'imponible_8': 0.0,
            'imponible_9': 0.0
        }


# Función principal de demostración
def main():
    """
    Función principal para demostrar el uso del extractor
    """
    # Configuración
    config = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=700000.0,
        trunca_tope=True
    )
    
    # Conexión a la base de datos
    try:
        db = DatabaseConnection('conexion-pstgresql/database.ini')
        extractor = SicossDataExtractor(db)
        procesador = SicossDataProcessor(config)
        
        # Extraer datos
        datos_extraidos = extractor.extraer_datos_completos(
            config=config,
            per_anoct=2024,
            per_mesct=12,
            nro_legajo=None  # Todos los legajos
        )
        
        # Procesar datos
        resultado = procesador.procesar_datos_extraidos(datos_extraidos)
        
        # Mostrar resultados
        print("\n=== RESULTADO DEL PROCESAMIENTO ===")
        print(f"Legajos procesados: {resultado['estadisticas']['legajos_validos']}")
        print(f"Legajos rechazados: {resultado['estadisticas']['legajos_rechazados']}")
        print("\nTotales:")
        for concepto, valor in resultado['totales'].items():
            print(f"  {concepto}: ${valor:,.2f}")
        
    except Exception as e:
        logger.error(f"Error en el procesamiento: {e}")
        raise


if __name__ == "__main__":
    main()