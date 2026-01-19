import logging
from typing import Any, List

import pandas as pd

from config.sicoss_config import SicossConfig

from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class TopesProcessor(BaseProcessor):
    """Procesador especializado para aplicación de topes"""

    def __init__(self, config: SicossConfig):
        super().__init__(config)

    def process(self, df_legajos: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        Aplica topes jubilatorios con lógica completa del PHP legacy

        Args:
            df_legajos: DataFrame de legajos a procesar
            **kwargs: Parámetros adicionales

        Returns:
            pd.DataFrame: DataFrame con topes aplicados y campos recalculados
        """
        logger.info("🔧 Aplicando topes jubilatorios con lógica completa...")

        if df_legajos.empty:
            return df_legajos

        df = df_legajos.copy()

        if not getattr(self.config, "trunca_tope", True):
            logger.info("🚫 Topes desactivados en configuración")
            return df

        # Pipeline completo de topes (siguiendo orden PHP legacy)
        df = self._aplicar_topes_patronales(df)
        df = self._aplicar_topes_personales_complejos(df)
        df = self._aplicar_categorias_diferenciales(df)
        df = self._aplicar_topes_otra_actividad(df)
        df = self._aplicar_topes_otros_aportes(df)
        df = self._aplicar_casos_especiales(df)
        df = self._calcular_campos_finales(df)
        df = self._recalcular_importe_bruto(df)

        self._log_process_info("TopesProcessor", len(df_legajos), len(df))
        return df

    def _aplicar_topes_patronales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica topes patronales (SAC + Imponible sin SAC)

        Args:
            df: DataFrame con importes calculados

        Returns:
            pd.DataFrame: DataFrame con topes aplicados
        """
        # Asegurar que las columnas requeridas existan
        for campo in [
            "ImporteSAC",
            "ImporteImponibleSinSAC",
            "ImporteImponiblePatronal",
            "DiferenciaSACImponibleConTope",
            "DiferenciaImponibleConTope",
            "ImporteSACPatronal",
        ]:
            if campo not in df.columns:
                df[campo] = 0.0

        # 1. Tope SAC patronal
        tope_sac = self.config.tope_sac_jubilatorio_patr
        mask_excede_sac = df["ImporteSAC"] > tope_sac

        if mask_excede_sac.any():
            logger.info(
                f"Aplicando tope SAC patronal: {mask_excede_sac.sum()} legajos afectados"
            )

            df.loc[mask_excede_sac, "DiferenciaSACImponibleConTope"] = (
                df.loc[mask_excede_sac, "ImporteSAC"] - tope_sac
            )
            df.loc[mask_excede_sac, "ImporteImponiblePatronal"] -= df.loc[
                mask_excede_sac, "DiferenciaSACImponibleConTope"
            ]
            df.loc[mask_excede_sac, "ImporteSACPatronal"] = tope_sac

        # 2. Tope imponible sin SAC
        tope_imponible = self.config.tope_jubilatorio_patronal
        mask_excede_imponible = df["ImporteImponibleSinSAC"] > tope_imponible

        if mask_excede_imponible.any():
            logger.info(
                f"Aplicando tope imponible: {mask_excede_imponible.sum()} legajos afectados"
            )

            df.loc[mask_excede_imponible, "DiferenciaImponibleConTope"] = (
                df.loc[mask_excede_imponible, "ImporteImponibleSinSAC"] - tope_imponible
            )
            df.loc[mask_excede_imponible, "ImporteImponiblePatronal"] -= df.loc[
                mask_excede_imponible, "DiferenciaImponibleConTope"
            ]

        return df

    def _aplicar_topes_personales_complejos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica topes personales complejos siguiendo la lógica específica del PHP (líneas 1135-1165).
        Maneja legajos con y sin SAC de forma diferenciada.

        Args:
            df: DataFrame con importes brutos e imponibles base

        Returns:
            pd.DataFrame: DataFrame con IMPORTE_IMPON ajustado a topes personales
        """
        if not getattr(self.config, "trunca_tope", True):
            return df

        # Inicializar campos necesarios
        for campo in [
            "ImporteSACNoDocente",
            "IMPORTE_IMPON",
            "ImporteImponible_6",
            "ImporteNoRemun",
        ]:
            if campo not in df.columns:
                df[campo] = 0.0

        # Configurar ImporteSACNoDocente si no existe
        if df["ImporteSACNoDocente"].sum() == 0:
            df["ImporteSACNoDocente"] = df.get("ImporteSAC", 0.0)

        # Configurar IMPORTE_IMPON inicial si no existe
        if df["IMPORTE_IMPON"].sum() == 0:
            df["IMPORTE_IMPON"] = df.get("ImporteImponiblePatronal", 0.0)

        tope_jubil_personal_base = self.config.tope_jubilatorio_personal
        tope_sac_personal = self.config.tope_sac_jubilatorio_pers

        # Calcular tope personal dinámico basado en si tiene SAC (Vectorizado)
        df["tope_jubil_personal_dinamico"] = tope_jubil_personal_base
        mask_tiene_sac = df["ImporteSAC"] > 0
        df.loc[mask_tiene_sac, "tope_jubil_personal_dinamico"] = (
            tope_jubil_personal_base + tope_sac_personal
        )

        # Caso 1: ImporteSACNoDocente excede tope personal
        mask_excede_tope_personal = (
            df["ImporteSACNoDocente"] > df["tope_jubil_personal_dinamico"]
        )

        if mask_excede_tope_personal.any():
            logger.info(
                f"Aplicando tope personal SAC: {mask_excede_tope_personal.sum()} legajos"
            )

            df.loc[mask_excede_tope_personal, "DiferenciaSACImponibleConTope"] = (
                df.loc[mask_excede_tope_personal, "ImporteSACNoDocente"]
                - tope_sac_personal
            )
            df.loc[mask_excede_tope_personal, "IMPORTE_IMPON"] -= df.loc[
                mask_excede_tope_personal, "DiferenciaSACImponibleConTope"
            ]
            df.loc[mask_excede_tope_personal, "ImporteSACNoDocente"] = tope_sac_personal

        # Caso 2: Lógica compleja cuando NO excede tope personal (PHP líneas 1147-1160)
        mask_no_excede = ~mask_excede_tope_personal

        if mask_no_excede.any():
            # Calcular bruto_nodo_sin_sac
            bruto_nodo_sin_sac = (
                df.loc[mask_no_excede, "IMPORTE_BRUTO"]
                - df.loc[mask_no_excede, "ImporteImponible_6"]
                - df.loc[mask_no_excede, "ImporteSACNoDocente"]
            )

            sac = df.loc[mask_no_excede, "ImporteSACNoDocente"]

            # Aplicar fórmula compleja del PHP
            tope_sueldo = (
                bruto_nodo_sin_sac - df.loc[mask_no_excede, "ImporteNoRemun"]
            ).clip(upper=tope_jubil_personal_base)
            tope_sac = sac.clip(upper=tope_sac_personal)

            df.loc[mask_no_excede, "IMPORTE_IMPON"] = tope_sueldo + tope_sac

        # Limpiar columna temporal
        df.drop("tope_jubil_personal_dinamico", axis=1, inplace=True)

        return df

    def _aplicar_categorias_diferenciales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica categorías diferenciales (PHP líneas 1167-1170)
        Si un legajo tiene categoría diferencial, su importe imponible se setea en 0.
        """
        try:
            # 1. Obtener categorías diferenciales de configuración
            categorias_dif = getattr(self.config, "categorias_diferenciales", [])
            
            if not categorias_dif:
                categorias_dif = self._obtener_categorias_diferenciales()

            if not categorias_dif:
                logger.debug("No hay categorías diferenciales configuradas")
                return df

            # 2. Obtener lista de legajos afectados en una sola consulta (Optimización Vectorizada)
            legajos_diferenciales = self._obtener_legajos_diferenciales_bulk(
                df["nro_legaj"].tolist(), categorias_dif
            )

            if not legajos_diferenciales:
                return df

            # 3. Aplicar tope a los legajos identificados
            mask_diferencial = df["nro_legaj"].isin(legajos_diferenciales)

            if mask_diferencial.any():
                count = mask_diferencial.sum()
                logger.info(f"✅ Aplicando categorías diferenciales a {count} legajos")
                # Según PHP: si es categoría diferencial → IMPORTE_IMPON = 0
                df.loc[mask_diferencial, "IMPORTE_IMPON"] = 0.0

        except Exception as e:
            logger.warning(f"⚠️ Error aplicando categorías diferenciales: {e}")

        return df

    def _obtener_categorias_diferenciales(self) -> List[str]:
        """
        Obtiene las categorías diferenciales definidas en la configuración Mapuche.

        Returns:
            List[str]: Lista de códigos de categorías diferenciales.
        """
        try:
            # Intentar obtener desde MapucheConfig
            import configparser

            from config.mapuche_config import ConnectionParams, create_mapuche_config

            config_ini = configparser.ConfigParser()
            config_ini.read("database.ini")
            if "postgresql" not in config_ini:
                return []

            db_params = config_ini["postgresql"]

            connection_params: ConnectionParams = {
                "host": db_params.get("host", "localhost"),
                "database": db_params.get("database", ""),
                "user": db_params.get("user", ""),
                "password": db_params.get("password", ""),
                "port": db_params.get("port", "5432"),
            }

            config = create_mapuche_config(connection_params)
            categorias_str = config.get_categorias_diferencial()

            if categorias_str:
                return [cat.strip() for cat in categorias_str.split(",") if cat.strip()]

        except Exception as e:
            logger.warning(f"⚠️ No se pudieron obtener categorías diferenciales: {e}")

        return []

    def _obtener_legajos_diferenciales_bulk(
        self, lista_legajos: List[int], categorias_dif: List[str]
    ) -> List[int]:
        """
        Verifica qué legajos de la lista tienen categoría diferencial en una sola consulta.
        Optimización para eliminar el problema N+1.

        Args:
            lista_legajos: Lista de IDs de legajos a verificar
            categorias_dif: Lista de códigos de categorías a buscar

        Returns:
            List[int]: Lista de legajos que efectivamente tienen categoría diferencial
        """
        if not lista_legajos or not categorias_dif:
            return []

        try:
            from database.database_connection import DatabaseConnection

            db = DatabaseConnection()

            legajos_str = ",".join(map(str, lista_legajos))
            categorias_in_clause = "','".join(categorias_dif)

            query = f"""
            SELECT DISTINCT dh01.nro_legaj
            FROM mapuche.dh01 dh01
            INNER JOIN mapuche.dh03 dh03 ON dh01.nro_legaj = dh03.nro_legaj
            WHERE dh01.nro_legaj IN ({legajos_str})
              AND dh03.codc_categ IN ('{categorias_in_clause}')
              AND mapuche.map_es_cargo_activo(dh03.nro_cargo)
            """

            resultado = db.execute_query(query)
            db.close()

            if not resultado.empty:
                return resultado["nro_legaj"].tolist()

        except Exception as e:
            logger.warning(f"⚠️ Error en consulta bulk de categorías diferenciales: {e}")

        return []

    def _aplicar_topes_otra_actividad(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica topes de otra actividad (PHP líneas 1192-1220)
        Considera los ingresos externos para no exceder el tope máximo imponible sumado.

        Args:
            df: DataFrame con importes internos

        Returns:
            pd.DataFrame: DataFrame con IMPORTE_IMPON ajustado por actividad externa
        """
        # Inicializar campos de otra actividad si no existen
        for campo in ["ImporteBrutoOtraActividad", "ImporteSACOtraActividad"]:
            if campo not in df.columns:
                df[campo] = 0.0

        mask_tiene_otra_actividad = (df["ImporteBrutoOtraActividad"] != 0) | (
            df["ImporteSACOtraActividad"] != 0
        )

        if mask_tiene_otra_actividad.any():
            logger.info(
                f"Procesando otra actividad: {mask_tiene_otra_actividad.sum()} legajos"
            )

            tope_sac_pers = self.config.tope_sac_jubilatorio_pers
            tope_jubil_patronal = self.config.tope_jubilatorio_patronal
            tope_total = tope_sac_pers + tope_jubil_patronal

            # Suma de otra actividad
            suma_otra_actividad = (
                df.loc[mask_tiene_otra_actividad, "ImporteBrutoOtraActividad"]
                + df.loc[mask_tiene_otra_actividad, "ImporteSACOtraActividad"]
            )

            # Caso 1: Otra actividad excede topes totales → IMPORTE_IMPON = 0
            mask_excede_total = suma_otra_actividad >= tope_total
            indices_excede = (
                df.loc[mask_tiene_otra_actividad].loc[mask_excede_total].index
            )

            if len(indices_excede) > 0:
                logger.info(
                    f"Otra actividad excede total: {len(indices_excede)} legajos"
                )
                df.loc[indices_excede, "IMPORTE_IMPON"] = 0.0

            # Caso 2: Calcular topes proporcionales
            mask_no_excede = ~mask_excede_total
            indices_no_excede = (
                df.loc[mask_tiene_otra_actividad].loc[mask_no_excede].index
            )

            if len(indices_no_excede) > 0:
                # Tope disponible para sueldo
                tope_disponible_sueldo = (
                    tope_jubil_patronal
                    - df.loc[indices_no_excede, "ImporteBrutoOtraActividad"]
                ).clip(lower=0)

                # Tope disponible para SAC
                tope_disponible_sac = (
                    tope_sac_pers - df.loc[indices_no_excede, "ImporteSACOtraActividad"]
                ).clip(lower=0)

                # Aplicar topes
                importe_sueldo_limitado = df.loc[
                    indices_no_excede, "ImporteImponibleSinSAC"
                ].clip(upper=tope_disponible_sueldo)
                importe_sac_limitado = df.loc[
                    indices_no_excede, "ImporteSACPatronal"
                ].clip(upper=tope_disponible_sac)

                df.loc[indices_no_excede, "IMPORTE_IMPON"] = (
                    importe_sueldo_limitado + importe_sac_limitado
                )

        return df

    def _aplicar_topes_otros_aportes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica topes de otros aportes (PHP líneas 1226-1240)

        Args:
            df: DataFrame actual

        Returns:
            pd.DataFrame: DataFrame con ImporteImponible_4 ajustado
        """
        # Inicializar campos necesarios
        for campo in [
            "ImporteSACOtroAporte",
            "ImporteImponible_4",
            "DifSACImponibleConOtroTope",
            "DifImponibleConOtroTope",
        ]:
            if campo not in df.columns:
                df[campo] = 0.0

        if df["ImporteSACOtroAporte"].sum() == 0:
            df["ImporteSACOtroAporte"] = df.get("ImporteSAC", 0.0)

        if df["ImporteImponible_4"].sum() == 0:
            df["ImporteImponible_4"] = df.get("IMPORTE_IMPON", 0.0)

        # Tope SAC otros aportes
        tope_sac_otro_aporte = self.config.tope_sac_jubilatorio_otro_ap
        mask_excede_sac_otro = df["ImporteSACOtroAporte"] > tope_sac_otro_aporte

        if mask_excede_sac_otro.any():
            logger.info(
                f"Aplicando tope SAC otros aportes: {mask_excede_sac_otro.sum()} legajos"
            )

            df.loc[mask_excede_sac_otro, "DifSACImponibleConOtroTope"] = (
                df.loc[mask_excede_sac_otro, "ImporteSACOtroAporte"]
                - tope_sac_otro_aporte
            )
            df.loc[mask_excede_sac_otro, "ImporteImponible_4"] -= df.loc[
                mask_excede_sac_otro, "DifSACImponibleConOtroTope"
            ]
            df.loc[mask_excede_sac_otro, "ImporteSACOtroAporte"] = tope_sac_otro_aporte

        # Calcular OtroImporteImponibleSinSAC
        df["OtroImporteImponibleSinSAC"] = (
            df["ImporteImponible_4"] - df["ImporteSACOtroAporte"]
        )

        # Tope otros aportes sin SAC
        tope_otros_aportes = self.config.tope_otros_aportes_personales
        mask_excede_otros = df["OtroImporteImponibleSinSAC"] > tope_otros_aportes

        if mask_excede_otros.any():
            logger.info(
                f"Aplicando tope otros aportes: {mask_excede_otros.sum()} legajos"
            )

            df.loc[mask_excede_otros, "DifImponibleConOtroTope"] = (
                df.loc[mask_excede_otros, "OtroImporteImponibleSinSAC"]
                - tope_otros_aportes
            )
            df.loc[mask_excede_otros, "ImporteImponible_4"] -= df.loc[
                mask_excede_otros, "DifImponibleConOtroTope"
            ]

        return df

    def _aplicar_casos_especiales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica casos especiales definidos por combinaciones de campos (PHP líneas 1243-1246)

        Args:
            df: DataFrame con cálculos intermedios

        Returns:
            pd.DataFrame: DataFrame con ajustes por casos especiales aplicados
        """
        # Inicializar campos si no existen
        for campo in ["ImporteImponible_6", "TipoDeOperacion"]:
            if campo not in df.columns:
                df[campo] = 0.0 if campo.startswith("Importe") else 1

        # Caso especial: ImporteImponible_6 != 0 && TipoDeOperacion == 1 → IMPORTE_IMPON = 0
        mask_caso_especial = (df["ImporteImponible_6"] != 0) & (
            df["TipoDeOperacion"] == 1
        )

        if mask_caso_especial.any():
            logger.info(
                f"Aplicando caso especial ImporteImponible_6: {mask_caso_especial.sum()} legajos"
            )
            df.loc[mask_caso_especial, "IMPORTE_IMPON"] = 0.0

        return df

    def _calcular_campos_finales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula los importes imponibles finales tras aplicar todos los topes (PHP líneas 1248-1268)

        Args:
            df: DataFrame con todos los topes previos aplicados

        Returns:
            pd.DataFrame: DataFrame con ImporteImponibleSinSAC y IMPORTE_IMPON finales
        """
        # Recalcular ImporteImponibleSinSAC después de todos los ajustes
        df["ImporteImponibleSinSAC"] = df.get("IMPORTE_IMPON", 0.0) - df.get(
            "ImporteSACNoDocente", 0.0
        )

        # Aplicar tope final a ImporteImponibleSinSAC si es necesario
        tope_jubil_personal = self.config.tope_jubilatorio_personal
        tope_sac_pers = self.config.tope_sac_jubilatorio_pers

        # Calcular tope dinámico final (Vectorizado)
        df["tope_final"] = tope_jubil_personal
        mask_tiene_sac = df.get("ImporteSAC", 0.0) > 0
        df.loc[mask_tiene_sac, "tope_final"] = tope_jubil_personal + tope_sac_pers

        # Usamos máscara inversa explícita para evitar advertencia de linter sobre ~ en bool (confusión de tipo)
        mask_no_tiene_sac = df.get("ImporteSAC", 0.0) <= 0
        df.loc[mask_no_tiene_sac, "tope_final"] = tope_jubil_personal

        # Aplicar tope final
        mask_excede_final = df["ImporteImponibleSinSAC"] > df["tope_final"]

        if mask_excede_final.any():
            logger.info(
                f"Aplicando tope final ImporteImponibleSinSAC: {mask_excede_final.sum()} legajos"
            )

            df.loc[mask_excede_final, "DiferenciaImponibleConTope"] = (
                df.loc[mask_excede_final, "ImporteImponibleSinSAC"]
                - tope_jubil_personal
            )
            df.loc[mask_excede_final, "IMPORTE_IMPON"] -= df.loc[
                mask_excede_final, "DiferenciaImponibleConTope"
            ]

        # Limpiar columna temporal
        df.drop("tope_final", axis=1, inplace=True)

        return df

    def _recalcular_importe_bruto(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recalcula el importe bruto final para asegurar consistencia tras aplicaciones de topes

        Args:
            df: DataFrame final

        Returns:
            pd.DataFrame: DataFrame con IMPORTE_BRUTO actualizado
        """
        df["IMPORTE_BRUTO"] = df["ImporteImponiblePatronal"] + df.get(
            "ImporteNoRemun", 0.0
        )
        return df
