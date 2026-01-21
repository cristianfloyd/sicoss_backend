"""
licencias_extractor.py

Extractor especializado para licencias de protección integral y vacaciones
Migrado de tests_legacy/mapuche_licencias_extractor.py a la nueva arquitectura
"""

import logging
from typing import Any, List, Optional

import pandas as pd

from value_objects.periodo_fiscal import PeriodoFiscal

from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class LicenciasExtractor(BaseExtractor):
    """
    Extractor de licencias de protección integral y vacaciones desde base de datos Mapuche
    """

    def extract(self, **kwargs: Any) -> pd.DataFrame:
        """
        Implementación obligatoria de BaseExtractor.
        Delega a extract_for_legajos.
        """
        periodo = kwargs.get("periodo")
        legajos_ids = kwargs.get("legajos_ids", [])

        if not isinstance(periodo, PeriodoFiscal):
            # Intentar obtener de anio/mes si no viene el objeto
            anio = kwargs.get("per_anoct")
            mes = kwargs.get("per_mesct")
            if anio and mes:
                periodo = PeriodoFiscal(year=int(anio), month=int(mes))
            else:
                raise ValueError(
                    "Se requiere un objeto PeriodoFiscal o per_anoct/per_mesct"
                )

        return self.extract_for_legajos(
            periodo=periodo,
            legajos_ids=legajos_ids,
            variantes_vacaciones=kwargs.get("variantes_vacaciones"),
            variantes_protecintegral=kwargs.get("variantes_protecintegral"),
        )

    def extract_for_legajos(
        self,
        periodo: PeriodoFiscal,
        legajos_ids: List[int],
        variantes_vacaciones: Optional[str] = None,
        variantes_protecintegral: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Obtiene licencias de protección integral y vacaciones para una lista de legajos

        Args:
            periodo: PeriodoFiscal a consultar
            legajos_ids: Lista de números de legajo
            variantes_vacaciones: String con IDs de variantes de vacaciones (ej: '1,2,3')
            variantes_protecintegral: String con IDs de variantes de protección integral (ej: '4,5,6')

        Returns:
            DataFrame con las licencias encontradas
        """
        if not legajos_ids:
            return pd.DataFrame(
                columns=["nro_legaj", "inicio", "final", "es_legajo", "condicion"]
            )

        # Si no hay variantes configuradas, retornar DataFrame vacío
        if not variantes_vacaciones and not variantes_protecintegral:
            logger.debug("No hay variantes de licencias configuradas")
            return pd.DataFrame(
                columns=["nro_legaj", "inicio", "final", "es_legajo", "condicion"]
            )

        # Obtener fechas del periodo
        import calendar
        from datetime import date

        fecha_inicio = date(periodo.year, periodo.month, 1).strftime("%Y-%m-%d")
        ultimo_dia = calendar.monthrange(periodo.year, periodo.month)[1]
        fecha_fin = date(periodo.year, periodo.month, ultimo_dia).strftime("%Y-%m-%d")

        # Construir condiciones WHERE dinámicamente
        where_vacaciones = ""
        where_protecintegral = ""

        # Filtros de variantes
        filtro_variantes: List[str] = []
        if variantes_vacaciones:
            where_vacaciones = (
                f" WHEN dh05.nrovarlicencia IN ({variantes_vacaciones}) THEN 12 "
            )
            filtro_variantes.append(variantes_vacaciones)

        if variantes_protecintegral:
            where_protecintegral = (
                f" WHEN dh05.nrovarlicencia IN ({variantes_protecintegral}) THEN 51 "
            )
            filtro_variantes.append(variantes_protecintegral)

        legajos_str = ",".join(map(str, legajos_ids))
        variantes_str = ",".join(filtro_variantes)

        # Consulta SQL adaptada de la versión legacy
        sql = f"""
        SELECT
            dh01.nro_legaj,
            CASE
                WHEN dh05.fec_desde <= '{fecha_inicio}'::date THEN 1
                ELSE date_part('day', dh05.fec_desde::timestamp)::integer
            END AS inicio,
            CASE
                WHEN dh05.fec_hasta > '{fecha_fin}'::date OR dh05.fec_hasta IS NULL THEN {ultimo_dia}
                ELSE date_part('day', dh05.fec_hasta::timestamp)::integer
            END AS final,
            TRUE AS es_legajo,
            CASE
              WHEN dl02.es_maternidad THEN 5
              {where_vacaciones}
              {where_protecintegral}
              ELSE 13
            END AS condicion
        FROM
            mapuche.dh05
            LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
            LEFT OUTER JOIN mapuche.dh01 ON (dh05.nro_legaj = dh01.nro_legaj)
        WHERE
            dh05.nro_legaj IN ({legajos_str})
            AND (fec_desde <= '{fecha_fin}'::date AND (fec_hasta is null OR fec_hasta >= '{fecha_inicio}'::date))
            AND mapuche.map_es_licencia_vigente(dh05.nro_licencia)
            AND dl02.es_remunerada = TRUE
            AND dh05.nrovarlicencia IN ({variantes_str})

        UNION

        SELECT
            dh01.nro_legaj,
            CASE
                WHEN dh05.fec_desde <= '{fecha_inicio}'::date THEN 1
                ELSE date_part('day', dh05.fec_desde::timestamp)::integer
            END AS inicio,
            CASE
                WHEN dh05.fec_hasta > '{fecha_fin}'::date OR dh05.fec_hasta IS NULL THEN {ultimo_dia}
                ELSE date_part('day', dh05.fec_hasta::timestamp)::integer
            END AS final,
            FALSE AS es_legajo,
            CASE
              WHEN dl02.es_maternidad THEN 5
              {where_vacaciones}
              {where_protecintegral}
              ELSE 13
            END AS condicion
        FROM
            mapuche.dh05
            LEFT OUTER JOIN mapuche.dh03 ON (dh03.nro_cargo = dh05.nro_cargo)
            LEFT OUTER JOIN mapuche.dh01 ON (dh03.nro_legaj = dh01.nro_legaj)
            LEFT OUTER JOIN mapuche.dl02 ON (dh05.nrovarlicencia = dl02.nrovarlicencia)
        WHERE
            dh03.nro_legaj IN ({legajos_str})
            AND (fec_desde <= '{fecha_fin}'::date AND (fec_hasta is null OR fec_hasta >= '{fecha_inicio}'::date))
            AND mapuche.map_es_cargo_activo(dh05.nro_cargo)
            AND mapuche.map_es_licencia_vigente(dh05.nro_licencia)
            AND dl02.es_remunerada = TRUE
            AND dh05.nrovarlicencia IN ({variantes_str})
        """

        try:
            # Ejecutar consulta a través del manager de BD
            df: pd.DataFrame = self.db.execute_query(sql)

            if df.empty:
                return pd.DataFrame(
                    columns=["nro_legaj", "inicio", "final", "es_legajo", "condicion"]
                )

            # Convertir tipos de datos explícitos para pandas
            df["nro_legaj"] = df["nro_legaj"].astype("int32")
            df["inicio"] = df["inicio"].astype("int32")
            df["final"] = df["final"].astype("int32")
            df["condicion"] = df["condicion"].astype("int32")
            df["es_legajo"] = df["es_legajo"].astype("bool")

            logger.info(
                f"✅ Se extrajeron {len(df)} licencias para {len(legajos_ids)} legajos"
            )
            return df

        except Exception as e:
            logger.error(f"❌ Error extrayendo licencias: {e}")
            return pd.DataFrame(
                columns=["nro_legaj", "inicio", "final", "es_legajo", "condicion"]
            )
