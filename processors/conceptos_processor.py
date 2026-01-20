import logging
from typing import Any, List, Optional, Union

import pandas as pd

from config.sicoss_config import SicossConfig

from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class ConceptosProcessor(BaseProcessor):
    """Procesador vectorizado de máximo rendimiento para conceptos SICOSS"""

    def __init__(self, config: SicossConfig):
        super().__init__(config)
        self._init_mapeos()

    def _init_mapeos(self):
        """Inicializa mapeos para optimización"""
        # Mapeo directo tipo_grupo -> campo_sicoss (casos simples)
        self.mapeo_simple = {
            # Mapeos originales
            6: "ImporteHorasExtras",
            7: "ImporteZonaDesfavorable",
            8: "ImporteVacaciones",
            21: "ImporteAdicionales",
            22: "ImportePremios",
            24: "CantidadHorasExtras",
            45: "ImporteNoRemun",
            46: "ImporteRectificacionRemun",
            47: "ImporteMaternidad",
            16: "AporteAdicionalObraSocial",
            67: "ImporteImponibleBecario",
            81: "ImporteSICOSS27430",
            83: "ImporteSICOSSDec56119",
            84: "NoRemun4y8",
            86: "IncrementoSolidario",
            91: "ImporteTipo91",
            96: "ImporteNoRemun96",
            # Mapeos adicionales para tipos encontrados en datos reales
            5: "ImportePremios",  # Otro tipo de premios
            25: "ImporteAdicionales",  # Adicionales tipo 25
            29: "ImporteAdicionales",  # Adicionales tipo 29
            30: "ImporteAdicionales",  # Adicionales tipo 30
            68: "ImporteZonaDesfavorable",  # Zona desfavorable tipo 68
            69: "ImporteZonaDesfavorable",  # Zona desfavorable tipo 69
            77: "ImporteNoRemun",  # No remunerativo tipo 77
            89: "ImporteAdicionales",  # Adicionales tipo 89
            50: "ImporteAdicionales",  # Adicionales tipo 50
            51: "ImporteAdicionales",  # Adicionales tipo 51
            36: "ImporteAdicionales",  # Adicionales tipo 36
        }

        # Tipos que van a ImporteImponible_6 (investigadores)
        self.tipos_investigador = {11, 12, 13, 14, 15, 48, 49}

        # Mapeo tipo_grupo -> prioridad para TipoDeActividad
        self.mapeo_prioridades = {
            11: 38,
            12: 34,
            13: 35,
            14: 36,
            15: 37,
            48: 87,
            49: 88,
        }

        # Tipos especiales que requieren procesamiento específico
        self.tipos_especiales = {
            4,  # Básico - contribuye a campos base pero no es directo
            33,  # Descuentos - procesamiento especial
            58,  # Seguro Vida Obligatorio - boolean
            9,  # SAC - procesamiento especial por escalafón
        }

        # Todos los campos SICOSS que necesitamos inicializar
        self.campos_sicoss = [
            "ImporteSAC",
            "ImporteHorasExtras",
            "ImporteVacaciones",
            "ImporteAdicionales",
            "ImportePremios",
            "ImporteNoRemun",
            "ImporteMaternidad",
            "ImporteZonaDesfavorable",
            "ImporteImponible_6",
            "AporteAdicionalObraSocial",
            "CantidadHorasExtras",
            "SeguroVidaObligatorio",
            "ImporteImponibleBecario",
            "ImporteSICOSS27430",
            "ImporteSICOSSDec56119",
            "NoRemun4y8",
            "IncrementoSolidario",
            "ImporteTipo91",
            "ImporteNoRemun96",
            "ImporteRectificacionRemun",
            "ImporteSACDoce",
            "ImporteSACAuto",
            "ImporteSACNodo",
            "AsignacionesFliaresPagadas",
            "PrioridadTipoDeActividad",
            "TipoDeActividad",
            "SACInvestigador",
            "IMPORTE_VOLUN",
            "IMPORTE_ADICI",
            "adherentes",
        ]

    def process(
        self, df_legajos: pd.DataFrame, df_conceptos: pd.DataFrame, **kwargs: Any
    ) -> pd.DataFrame:
        """Procesa conceptos con máximo rendimiento vectorizado"""
        logger.info("🚀 Procesando conceptos con vectorización completa...")

        if df_conceptos.empty:
            return self._inicializar_columnas_sicoss(df_legajos)

        # PIPELINE VECTORIZADO
        start_time = pd.Timestamp.now()

        # 1. Expandir tipos_grupos
        df_expandido = self._expandir_tipos_grupos(df_conceptos)
        logger.debug(f"Expandido: {len(df_conceptos)} → {len(df_expandido)} filas")

        # 2. Procesar casos simples (vectorizado puro)
        df_simples = self._procesar_casos_simples(df_expandido)

        # 3. Procesar SAC con escalafón (vectorizado)
        df_sac = self._procesar_sac_escalafon(df_expandido)

        # 4. Procesar investigadores (vectorizado)
        df_investigadores = self._procesar_investigadores(df_expandido)

        # 5. Procesar casos especiales
        df_especiales = self._procesar_casos_especiales(df_expandido)

        # 5.b Procesar Asignaciones Familiares (basado en tipo_conce 'F' del legacy)
        df_asig_fam = self._procesar_asignaciones_tipo_f(df_conceptos)

        # 6. Combinar todos los resultados
        df_combinado = self._combinar_resultados(
            [df_simples, df_sac, df_investigadores, df_especiales, df_asig_fam]
        )

        # 7. Agrupar por legajo
        df_agrupado = self._agrupar_por_legajo(df_combinado)

        # 8. Procesar conceptos específicos por código
        df_agrupado = self._procesar_conceptos_especificos(df_agrupado, df_conceptos)

        # 9. Merge con legajos
        result = df_legajos.merge(df_agrupado, on="nro_legaj", how="left")
        result = self._llenar_valores_faltantes(result)

        # 10. NUEVA FASE: Consolidación de campos calculados
        result = self._consolidar_campos_calculados(result)

        elapsed = (pd.Timestamp.now() - start_time).total_seconds()
        logger.info(f"⚡ Procesamiento vectorizado completado en {elapsed:.3f}s")

        self._log_process_info("ConceptosProcessor", len(df_legajos), len(result))
        return result

    def _expandir_tipos_grupos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expande array tipos_grupos en múltiples filas"""
        # Convertir tipos_grupos a list si es string
        df = df.copy()

        # Manejar diferentes formatos del campo tipos_grupos
        def parse_tipos_grupos(
            tipos: Optional[Union[List[Any], str, int, float]],
        ) -> List[int]:
            """
            Parsea y normaliza el campo tipos_grupos en una lista de enteros.

            Maneja diversos formatos de entrada (listas, strings, valores numéricos)
            provenientes de la base de datos o procesos previos.

            Args:
                tipos: El valor del campo tipos_grupos a procesar.

            Returns:
                List[int]: Una lista de IDs de tipos de grupos.
            """
            try:
                if tipos is None:
                    return []

                # Si es una lista de Python (caso más común)
                if isinstance(tipos, list):
                    if len(tipos) == 0:
                        return []
                    # Usar verificación simple para enteros en listas
                    result: List[int] = []
                    for t in tipos:
                        if t is not None and isinstance(t, (int, float)):
                            result.append(int(t))
                    return result

                # Si es string vacío o array vacío
                if tipos in ["{}", "", "[]"]:
                    return []

                if isinstance(tipos, str):
                    # Formato "{1,2,3}" -> [1,2,3]
                    tipos_clean = tipos.strip("{}[]").replace(" ", "")
                    if not tipos_clean:
                        return []
                    parts = tipos_clean.split(",")
                    return [int(t.strip()) for t in parts if t.strip().isdigit()]

                # Si es un número directamente, convertir a int y devolver como lista
                return [int(tipos)]  # type: ignore[arg-type]

                return []
            except (ValueError, AttributeError, TypeError) as e:
                logger.debug(
                    f"Error parseando tipos_grupos: {tipos} (tipo: {type(tipos)}) - {e}"
                )
                return []

        df["tipos_grupos_parsed"] = df["tipos_grupos"].apply(parse_tipos_grupos)

        # Filtrar filas con listas vacías antes de explode (evita errores de pandas)
        # También asegurar que todos los valores sean enteros válidos
        def tiene_tipos_validos(x):
            """Verifica si la lista tiene tipos válidos"""
            if not isinstance(x, list):
                return False
            if len(x) == 0:
                return False
            # Verificar que todos los elementos sean enteros válidos
            return all(isinstance(t, (int, float)) and pd.notna(t) for t in x)

        mask_no_vacias = df["tipos_grupos_parsed"].apply(tiene_tipos_validos)
        df_con_tipos = df[mask_no_vacias].copy()

        if df_con_tipos.empty:
            empty_df = pd.DataFrame()
            for col in [
                "nro_legaj",
                "impp_conce",
                "codn_conce",
                "codigoescalafon",
                "tipo_grupo",
            ]:
                empty_df[col] = pd.Series(dtype="object")
            return empty_df

        # Limpiar y normalizar las listas para asegurar que solo contengan enteros
        df_con_tipos["tipos_grupos_parsed"] = df_con_tipos["tipos_grupos_parsed"].apply(
            lambda x: [int(t) for t in x if isinstance(t, (int, float)) and pd.notna(t)]
        )
        # Filtrar nuevamente por si alguna lista quedó vacía después de la limpieza
        mask_no_vacias_final = df_con_tipos["tipos_grupos_parsed"].apply(
            lambda x: len(x) > 0
        )
        df_con_tipos = df_con_tipos[mask_no_vacias_final].copy()

        if df_con_tipos.empty:
            empty_df = pd.DataFrame()
            for col in [
                "nro_legaj",
                "impp_conce",
                "codn_conce",
                "codigoescalafon",
                "tipo_grupo",
            ]:
                empty_df[col] = pd.Series(dtype="object")
            return empty_df

        # OPTIMIZACIÓN DE RENDIMIENTO: Intentar usar explode() primero (más rápido)
        # Si falla, usar método manual con itertuples() como fallback
        # Para 1.6M filas, explode() debería ser ~50-100x más rápido que itertuples()
        try:
            # Intentar usar explode() - método vectorizado más rápido
            df_exploded = df_con_tipos.explode("tipos_grupos_parsed")

            # Filtrar filas válidas (None/NaN se convierten en NaN después de explode)
            mask_valid = df_exploded["tipos_grupos_parsed"].notna()
            df_exploded = df_exploded[mask_valid].copy()

            if df_exploded.empty:
                empty_df = pd.DataFrame()
                for col in [
                    "nro_legaj",
                    "impp_conce",
                    "codn_conce",
                    "codigoescalafon",
                    "tipo_grupo",
                ]:
                    empty_df[col] = pd.Series(dtype="object")
                return empty_df

            # Convertir a tipo_grupo (ya son enteros, pero asegurar tipo)
            df_exploded["tipo_grupo"] = pd.to_numeric(
                df_exploded["tipos_grupos_parsed"], errors="coerce"
            )
            df_exploded = df_exploded[pd.notna(df_exploded["tipo_grupo"])].copy()
            df_exploded["tipo_grupo"] = df_exploded["tipo_grupo"].astype("Int64")

        except (TypeError, ValueError, AttributeError) as e:
            # Fallback: usar método manual con itertuples() si explode() falla
            # itertuples() es ~10-50x más rápido que iterrows() para datasets grandes
            logger.warning(
                f"explode() falló ({type(e).__name__}: {e}), usando método manual. "
                "Esto puede ser más lento para datasets grandes."
            )
            rows_expanded = []
            for row in df_con_tipos.itertuples(index=False):
                tipos_list = row.tipos_grupos_parsed
                if isinstance(tipos_list, list) and len(tipos_list) > 0:
                    # Acceder a atributos de namedtuple (más rápido que dict lookup)
                    nro_legaj = row.nro_legaj
                    impp_conce = row.impp_conce
                    codn_conce = row.codn_conce
                    codigoescalafon = getattr(row, "codigoescalafon", "")

                    for tipo in tipos_list:
                        if isinstance(tipo, (int, float)) and pd.notna(tipo):
                            rows_expanded.append(
                                {
                                    "nro_legaj": nro_legaj,
                                    "impp_conce": impp_conce,
                                    "codn_conce": codn_conce,
                                    "codigoescalafon": codigoescalafon,
                                    "tipo_grupo": int(tipo),
                                }
                            )

            if not rows_expanded:
                empty_df = pd.DataFrame()
                for col in [
                    "nro_legaj",
                    "impp_conce",
                    "codn_conce",
                    "codigoescalafon",
                    "tipo_grupo",
                ]:
                    empty_df[col] = pd.Series(dtype="object")
                return empty_df

            df_exploded = pd.DataFrame(rows_expanded)
            # Asegurar que todas las columnas necesarias existen
            required_cols = [
                "nro_legaj",
                "impp_conce",
                "codn_conce",
                "codigoescalafon",
                "tipo_grupo",
            ]
            for col in required_cols:
                if col not in df_exploded.columns:
                    df_exploded[col] = pd.Series(
                        dtype="object" if col == "codigoescalafon" else "float64"
                    )

        # Retornar solo las columnas necesarias (evitar problemas con coverage)
        required_cols = [
            "nro_legaj",
            "impp_conce",
            "codn_conce",
            "codigoescalafon",
            "tipo_grupo",
        ]
        return df_exploded[required_cols].copy()

    def _procesar_casos_simples(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa casos simples con mapeo directo"""
        # Filtrar solo tipos simples
        mask_simples = df["tipo_grupo"].isin(list(self.mapeo_simple.keys()))
        df_simples = df[mask_simples].copy()

        if df_simples.empty:
            return pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])

        # Mapear tipo_grupo a campo_sicoss
        df_simples = df_simples.copy()
        df_simples["campo_sicoss"] = df_simples["tipo_grupo"].map(self.mapeo_simple)
        df_simples["valor"] = df_simples["impp_conce"]

        return df_simples[["nro_legaj", "campo_sicoss", "valor"]].copy()

    def _procesar_sac_escalafon(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa SAC (tipo 9) con lógica de escalafón"""
        mask_sac = df["tipo_grupo"] == 9
        df_sac = df[mask_sac].copy()

        if df_sac.empty:
            return pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])

        # Crear múltiples campos por cada fila SAC
        resultados: List[pd.DataFrame] = []

        # ImporteSAC principal
        resultados.append(
            df_sac[["nro_legaj"]].assign(
                campo_sicoss="ImporteSAC", valor=df_sac["impp_conce"]
            )
        )

        # SAC por escalafón
        for escalafon, campo in [
            ("NODO", "ImporteSACNodo"),
            ("AUTO", "ImporteSACAuto"),
            ("DOCE", "ImporteSACDoce"),
        ]:
            mask_escalafon = df_sac["codigoescalafon"] == escalafon
            if mask_escalafon.any():
                resultados.append(
                    df_sac[mask_escalafon][["nro_legaj"]].assign(
                        campo_sicoss=campo, valor=df_sac[mask_escalafon]["impp_conce"]
                    )
                )

        return (
            pd.concat(resultados, ignore_index=True)
            if resultados
            else pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])
        )

    def _procesar_investigadores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa tipos investigador (11-15, 48-49) vectorizado"""
        mask_inv = df["tipo_grupo"].isin(self.tipos_investigador)
        df_inv = df[mask_inv].copy()

        if df_inv.empty:
            return pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])

        resultados: List[pd.DataFrame] = []

        # ImporteImponible_6 para todos
        resultados.append(
            df_inv[["nro_legaj"]].assign(
                campo_sicoss="ImporteImponible_6", valor=df_inv["impp_conce"]
            )
        )

        # PrioridadTipoDeActividad - calcular máxima prioridad por legajo
        df_inv["prioridad"] = df_inv["tipo_grupo"].map(self.mapeo_prioridades)
        df_prioridad = df_inv.groupby("nro_legaj")["prioridad"].max().reset_index()
        df_prioridad["campo_sicoss"] = "PrioridadTipoDeActividad"
        df_prioridad["valor"] = df_prioridad["prioridad"]
        resultados.append(df_prioridad[["nro_legaj", "campo_sicoss", "valor"]])

        return pd.concat(resultados, ignore_index=True)

    def _procesar_casos_especiales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa casos especiales (tipo 58, etc.)"""
        resultados: List[pd.DataFrame] = []

        # Tipo 58 - Seguro Vida Obligatorio (booleano)
        mask_58 = df["tipo_grupo"] == 58
        if mask_58.any():
            df_58 = df[mask_58][["nro_legaj"]].drop_duplicates()
            df_58["campo_sicoss"] = "SeguroVidaObligatorio"
            df_58["valor"] = 1
            resultados.append(df_58)

        return (
            pd.concat(resultados, ignore_index=True)
            if resultados
            else pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])
        )

    def _procesar_asignaciones_tipo_f(self, df_conceptos: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa asignaciones familiares basadas en tipo_conce 'F' (Lógica legacy)
        """
        if "tipo_conce" not in df_conceptos.columns:
            return pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])

        mask_f = df_conceptos["tipo_conce"] == "F"
        df_f = df_conceptos[mask_f].copy()

        if df_f.empty:
            return pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])

        df_f["campo_sicoss"] = "AsignacionesFliaresPagadas"
        df_f["valor"] = df_f["impp_conce"]

        return df_f[["nro_legaj", "campo_sicoss", "valor"]].copy()

    def _combinar_resultados(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Combina todos los DataFrames de resultados"""
        dfs_validos = [df for df in dataframes if not df.empty]
        return (
            pd.concat(dfs_validos, ignore_index=True)
            if dfs_validos
            else pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])
        )

    def _agrupar_por_legajo(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrupa y suma por legajo y campo"""
        if df.empty:
            return pd.DataFrame(columns=["nro_legaj"])

        # Agrupar y sumar valores
        df_agrupado = (
            df.groupby(["nro_legaj", "campo_sicoss"])["valor"].sum().reset_index()
        )

        # Pivotar a formato wide
        df_pivot = df_agrupado.pivot(
            index="nro_legaj", columns="campo_sicoss", values="valor"
        )
        df_pivot = df_pivot.reset_index()

        # Llenar columnas faltantes con 0
        for campo in self.campos_sicoss:
            if campo not in df_pivot.columns:
                df_pivot[campo] = 0.0

        return df_pivot

    def _procesar_conceptos_especificos(
        self, df_agrupado: pd.DataFrame, df_conceptos: pd.DataFrame
    ) -> pd.DataFrame:
        """Procesa conceptos específicos por código (no por tipo_grupo)"""
        # Aquí iría la lógica para conceptos específicos como:
        # - self.config.codigo_os_aporte_adicional
        # - self.config.aportes_voluntarios
        # - self.config.codigo_obrasocial_fc

        # Por ahora retornamos sin cambios, pero se puede agregar fácilmente
        return df_agrupado

    def _inicializar_columnas_sicoss(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Inicializa todas las columnas SICOSS con ceros"""
        df = df_legajos.copy()
        for campo in self.campos_sicoss:
            df[campo] = 0.0
        return df

    def _llenar_valores_faltantes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Llena valores faltantes con ceros"""
        for campo in self.campos_sicoss:
            if campo in df.columns:
                df[campo] = df[campo].fillna(0.0)
        return df

    def _consolidar_campos_calculados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        NUEVA FASE: Consolida conceptos individuales en campos calculados que necesita TopesProcessor

        Implementa la lógica de consolidación que estaba dispersa en otros procesadores.
        """
        logger.info("🔄 Consolidando campos calculados...")

        # 1. Calcular Remuner78805 (conceptos remunerativos)
        df["Remuner78805"] = (
            df.get("ImporteSAC", 0)
            + df.get("ImporteHorasExtras", 0)
            + df.get("ImporteZonaDesfavorable", 0)
            + df.get("ImporteVacaciones", 0)
            + df.get("ImportePremios", 0)
            + df.get("ImporteAdicionales", 0)
            + df.get("ImporteImponibleBecario", 0)  # Si hay becarios, sumar también
        )

        # 2. Ajustar SAC si hay investigadores (restar SAC de investigadores)
        if "SACInvestigador" in df.columns:
            mask_investigadores = df["SACInvestigador"] > 0
            if mask_investigadores.any():
                df.loc[mask_investigadores, "ImporteSAC"] = (
                    df.loc[mask_investigadores, "ImporteSAC"]
                    - df.loc[mask_investigadores, "SACInvestigador"]
                )
                logger.info(
                    f"Ajustado SAC para {mask_investigadores.sum()} investigadores"
                )

        # 3. *** NUEVA LÓGICA: Calcular TipoDeActividad ***
        # Implementa exactamente la misma lógica que PHP (líneas 1866-1871)
        df = self._calcular_tipo_actividad(df)

        # 4. Calcular ImporteImponiblePatronal (base para topes)
        df["ImporteImponiblePatronal"] = df["Remuner78805"].copy()

        # 5. Calcular campos derivados principales
        df["ImporteSACPatronal"] = df.get("ImporteSAC", 0)
        df["ImporteImponibleSinSAC"] = (
            df["ImporteImponiblePatronal"] - df["ImporteSACPatronal"]
        )

        # 6. Calcular IMPORTE_BRUTO (imponible + no remunerativo)
        df["IMPORTE_BRUTO"] = df["ImporteImponiblePatronal"] + df.get(
            "ImporteNoRemun", 0
        )

        # 7. Calcular IMPORTE_IMPON inicial (base para procesos posteriores)
        df["IMPORTE_IMPON"] = df["Remuner78805"].copy()

        # 8. Inicializar campos adicionales que necesita TopesProcessor
        campos_adicionales = {
            "DiferenciaSACImponibleConTope": 0.0,
            "DiferenciaImponibleConTope": 0.0,
            "ImporteSACNoDocente": "ImporteSAC",  # Copia de ImporteSAC
            "ImporteImponible_4": "IMPORTE_IMPON",  # Copia de IMPORTE_IMPON
            "ImporteImponible_5": "IMPORTE_IMPON",  # Copia de IMPORTE_IMPON
            "TipoDeOperacion": 1,
            "ImporteSueldoMasAdicionales": 0.0,
            "ImporteSACOtraActividad": 0.0,
            "ImporteSACOtroAporte": "ImporteSAC",  # Copia de ImporteSAC
            "ImporteBrutoOtraActividad": 0.0,
        }

        for campo, valor in campos_adicionales.items():
            if isinstance(valor, str):
                # Es una referencia a otra columna
                df[campo] = df.get(valor, 0)
            else:
                # Es un valor constante
                df[campo] = valor

        # ImporteImponible_6: solo inicializar si no existe (puede venir de investigadores)
        # Si ya existe, solo llenar NaN con 0.0
        if "ImporteImponible_6" not in df.columns:
            df["ImporteImponible_6"] = 0.0
        else:
            df["ImporteImponible_6"] = df["ImporteImponible_6"].fillna(0.0)

        # 9. Calcular ImporteSueldoMasAdicionales (fórmula específica)
        df["ImporteSueldoMasAdicionales"] = (
            df["ImporteImponiblePatronal"]
            - df.get("ImporteSAC", 0)
            - df.get("ImporteHorasExtras", 0)
            - df.get("ImporteZonaDesfavorable", 0)
            - df.get("ImporteVacaciones", 0)
            - df.get("ImportePremios", 0)
            - df.get("ImporteAdicionales", 0)
        )

        # Si es positivo, restar incremento solidario
        mask_positivo = df["ImporteSueldoMasAdicionales"] > 0
        if mask_positivo.any() and "IncrementoSolidario" in df.columns:
            df.loc[mask_positivo, "ImporteSueldoMasAdicionales"] -= df.loc[
                mask_positivo, "IncrementoSolidario"
            ]

        # 10. Configurar trabajador convencionado si no está definido
        if (
            "trabajadorconvencionado" not in df.columns
            or df["trabajadorconvencionado"].isna().all()
        ):
            df["trabajadorconvencionado"] = "S"

        logger.info("✅ Campos calculados consolidados:")
        logger.info(f"  - Remuner78805: ${df['Remuner78805'].sum():,.2f}")
        logger.info(
            f"  - ImporteImponiblePatronal: ${df['ImporteImponiblePatronal'].sum():,.2f}"
        )
        logger.info(f"  - IMPORTE_BRUTO: ${df['IMPORTE_BRUTO'].sum():,.2f}")
        logger.info(f"  - IMPORTE_IMPON: ${df['IMPORTE_IMPON'].sum():,.2f}")

        return df

    def _calcular_tipo_actividad(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula TipoDeActividad basado en PrioridadTipoDeActividad y codigoactividad

        Implementa exactamente la misma lógica que PHP (SicossOptimizado.php líneas 1866-1871):

        if ($leg['PrioridadTipoDeActividad'] == 38 || $leg['PrioridadTipoDeActividad'] == 0)
            $leg['TipoDeActividad'] = $leg['codigoactividad'];
        elseif (($leg['PrioridadTipoDeActividad'] >= 34 && $leg['PrioridadTipoDeActividad'] <= 37) ||
            $leg['PrioridadTipoDeActividad'] == 87 || $leg['PrioridadTipoDeActividad'] == 88)
            $leg['TipoDeActividad'] = $leg['PrioridadTipoDeActividad'];
        """
        logger.info("🎯 Calculando TipoDeActividad según prioridades...")

        # Asegurar que existen los campos necesarios
        if "PrioridadTipoDeActividad" not in df.columns:
            df["PrioridadTipoDeActividad"] = 0

        if "codigoactividad" not in df.columns:
            logger.warning(
                "⚠️  Campo 'codigoactividad' no encontrado, usando 0 por defecto"
            )
            df["codigoactividad"] = 0

        # Inicializar TipoDeActividad en 0
        df["TipoDeActividad"] = 0

        # Convertir a enteros para comparación
        df["PrioridadTipoDeActividad"] = (
            pd.to_numeric(df["PrioridadTipoDeActividad"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        df["codigoactividad"] = (
            pd.to_numeric(df["codigoactividad"], errors="coerce").fillna(0).astype(int)
        )

        # LÓGICA EXACTA DE PHP:

        # Caso 1: Si prioridad es 38 o 0 → usar codigoactividad del cargo
        mask_codigo_actividad = (df["PrioridadTipoDeActividad"] == 38) | (
            df["PrioridadTipoDeActividad"] == 0
        )
        df.loc[mask_codigo_actividad, "TipoDeActividad"] = df.loc[
            mask_codigo_actividad, "codigoactividad"
        ]

        # Caso 2: Si prioridad está entre 34-37 o es 87/88 → usar la prioridad
        mask_prioridad = (
            (
                (df["PrioridadTipoDeActividad"] >= 34)
                & (df["PrioridadTipoDeActividad"] <= 37)
            )
            | (df["PrioridadTipoDeActividad"] == 87)
            | (df["PrioridadTipoDeActividad"] == 88)
        )
        df.loc[mask_prioridad, "TipoDeActividad"] = df.loc[
            mask_prioridad, "PrioridadTipoDeActividad"
        ]

        # Estadísticas para logging
        stats_actividad = df["TipoDeActividad"].value_counts().to_dict()
        stats_prioridad = df["PrioridadTipoDeActividad"].value_counts().to_dict()

        logger.info("✅ TipoDeActividad calculado:")
        logger.info(
            f"   - Legajos con codigoactividad (prioridad 38/0): {mask_codigo_actividad.sum()}"
        )
        logger.info(
            f"   - Legajos con prioridad investigador (34-37,87,88): {mask_prioridad.sum()}"
        )
        logger.info(f"   - Distribución TipoDeActividad: {stats_actividad}")
        logger.info(f"   - Distribución PrioridadTipoDeActividad: {stats_prioridad}")

        return df
