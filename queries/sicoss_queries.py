"""
sicoss_queries.py

Módulo contenedor de consultas SQL optimizadas para SICOSS
Extraído de SicossDataExtractor.py para mejor organización
"""

from typing import List
import logging

logger = logging.getLogger(__name__)

class SicossSQLQueries:
    """Contiene las consultas SQL optimizadas extraídas de SicossOptimizado.php"""
    
    @staticmethod
    def get_legajos_query(per_anoct: int, per_mesct: int, 
                         codc_reparto: str = "'REPA'", 
                         where_legajo: str = "true") -> str:
        """
        Consulta optimizada para obtener legajos (basada en get_sql_legajos())
        
        Args:
            per_anoct: Año del período
            per_mesct: Mes del período
            codc_reparto: Código de reparto (default: "'REPA'")
            where_legajo: Cláusula WHERE adicional (default: "true")
            
        Returns:
            str: Query SQL optimizada para legajos
        """
        logger.debug(f"Generando query legajos para período {per_anoct}/{per_mesct}")
        
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
        
        Args:
            per_anoct: Año del período
            per_mesct: Mes del período
            where_legajo: Cláusula WHERE adicional (default: "true")
            
        Returns:
            str: Query SQL optimizada para conceptos liquidados
        """
        logger.debug(f"Generando query conceptos para período {per_anoct}/{per_mesct}")
        
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
        
        Args:
            legajos: Lista de números de legajo
            
        Returns:
            str: Query SQL para otra actividad
        """
        if not legajos:
            return "SELECT NULL::integer AS nro_legaj, NULL::numeric AS ImporteBrutoOtraActividad, NULL::numeric AS ImporteSACOtraActividad WHERE FALSE"
        
        legajos_str = ','.join(map(str, legajos))
        logger.debug(f"Generando query otra actividad para {len(legajos)} legajos")
        
        return f"""
        SELECT
			nro_legaj,
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
        
        Args:
            legajos: Lista de números de legajo
            
        Returns:
            str: Query SQL para códigos de obra social
        """
        if not legajos:
            return "SELECT NULL::integer AS nro_legaj, NULL::text AS codigo_os WHERE FALSE"
        
        legajos_str = ','.join(map(str, legajos))
        logger.debug(f"Generando query obra social para {len(legajos)} legajos")
        
        return f"""
        SELECT 
            nro_legaj,
            '000000' AS codigo_os
        FROM UNNEST(ARRAY[{legajos_str}]) AS nro_legaj
        """
    
    @staticmethod
    def get_licencias_query(legajos: List[int], per_anoct: int, per_mesct: int) -> str:
        """
        Consulta para obtener licencias de legajos en un período específico
        
        Args:
            legajos: Lista de números de legajo
            per_anoct: Año del período
            per_mesct: Mes del período
            
        Returns:
            str: Query SQL para licencias
        """
        if not legajos:
            return "SELECT NULL::integer AS nro_legaj, NULL::integer AS licencia WHERE FALSE"
        
        legajos_str = ','.join(map(str, legajos))
        logger.debug(f"Generando query licencias para {len(legajos)} legajos, período {per_anoct}/{per_mesct}")
        
        return f"""
        SELECT 
            nro_legaj,
            CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS licencia
        FROM mapuche.licencias_tabla
        WHERE nro_legaj IN ({legajos_str})
        AND ano_licencia = {per_anoct}
        AND mes_licencia = {per_mesct}
        GROUP BY nro_legaj
        """
    
    @staticmethod
    def get_retro_query(legajos: List[int], per_anoct: int, per_mesct: int) -> str:
        """
        Consulta para obtener retroactivos de legajos
        
        Args:
            legajos: Lista de números de legajo
            per_anoct: Año del período
            per_mesct: Mes del período
            
        Returns:
            str: Query SQL para retroactivos
        """
        if not legajos:
            return "SELECT NULL::integer AS nro_legaj, NULL::numeric AS importe_retro WHERE FALSE"
        
        legajos_str = ','.join(map(str, legajos))
        logger.debug(f"Generando query retroactivos para {len(legajos)} legajos")
        
        return f"""
        SELECT 
            nro_legaj,
            SUM(impp_conce) AS importe_retro
        FROM mapuche.dh21
        WHERE nro_legaj IN ({legajos_str})
        AND ano_retro IS NOT NULL
        AND mes_retro IS NOT NULL
        GROUP BY nro_legaj
        """
    
    @classmethod
    def validate_query_params(cls, per_anoct: int, per_mesct: int) -> bool:
        """
        Valida parámetros básicos de las consultas
        
        Args:
            per_anoct: Año del período
            per_mesct: Mes del período
            
        Returns:
            bool: True si los parámetros son válidos
        """
        if not isinstance(per_anoct, int) or per_anoct < 2000 or per_anoct > 2050:
            logger.error(f"Año inválido: {per_anoct}")
            return False
        
        if not isinstance(per_mesct, int) or per_mesct < 1 or per_mesct > 12:
            logger.error(f"Mes inválido: {per_mesct}")
            return False
        
        return True 