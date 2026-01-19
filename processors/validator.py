import logging
from typing import Any

import pandas as pd

from config.sicoss_config import SicossConfig

from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class LegajosValidator(BaseProcessor):
    """Validador especializado para legajos según criterios SICOSS"""

    def __init__(self, config: SicossConfig):
        super().__init__(config)

    def process(self, df_legajos: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        Implementación de la interfaz BaseProcessor.
        Delega a validate() para mantener compatibilidad.
        """
        return self.validate(df_legajos)

    def validate(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """
        Valida legajos según criterios de SICOSS.
        Filtra legajos que no tienen importes imponibles, a menos que tengan situaciones especiales.

        Args:
            df_legajos: DataFrame con datos procesados

        Returns:
            pd.DataFrame: DataFrame filtrado con solo registros válidos para exportar
        """
        logger.info("🔍 Validando legajos según criterios SICOSS...")

        if df_legajos.empty:
            return df_legajos

        # 1. Verificar importes (Bruto/Imponible)
        campos_importes = ["IMPORTE_BRUTO", "IMPORTE_IMPON", "ImporteImponiblePatronal"]

        # Asegurar que las columnas existen para evitar KeyError
        for campo in campos_importes:
            if campo not in df_legajos.columns:
                df_legajos[campo] = 0.0

        mask_importes_validos = df_legajos[campos_importes].sum(axis=1) > 0

        # 2. Situaciones especiales (maternidad: 5, excedencia: 11)
        if "codigosituacion" in df_legajos.columns:
            mask_situaciones_especiales = df_legajos["codigosituacion"].isin([5, 11])
        else:
            mask_situaciones_especiales = pd.Series(False, index=df_legajos.index)

        # 3. Licencias
        check_lic = getattr(self.config, "check_lic", False)
        if check_lic and "licencia" in df_legajos.columns:
            mask_licencias = df_legajos["licencia"] == 1
        else:
            mask_licencias = pd.Series(False, index=df_legajos.index)

        # 4. Situación reserva de puesto (14)
        if "codigosituacion" in df_legajos.columns:
            mask_reserva_puesto = df_legajos["codigosituacion"] == 14
        else:
            mask_reserva_puesto = pd.Series(False, index=df_legajos.index)

        # Combinar todas las condiciones
        mask_validos = (
            mask_importes_validos
            | mask_situaciones_especiales
            | mask_licencias
            | mask_reserva_puesto
        )

        df_validos = df_legajos[mask_validos].copy()

        self._log_process_info("LegajosValidator", len(df_legajos), len(df_validos))

        logger.info(
            f"✅ Validación completada: {len(df_validos)}/{len(df_legajos)} legajos válidos"
        )

        return df_validos  # type: ignore
