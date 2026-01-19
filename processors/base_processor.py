import logging
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from config.sicoss_config import SicossConfig

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """Clase base abstracta para todos los procesadores de datos.

    Esta clase proporciona la estructura común y los métodos base que deben
    implementar todos los procesadores de datos en el sistema. Define la
    interfaz estándar para el procesamiento de DataFrames de pandas y
    proporciona utilidades comunes como el registro de información de
    procesamiento.

    Attributes:
        config (SicossConfig): Configuración del procesador que contiene
            los parámetros necesarios para el procesamiento de datos.

    Example:
        Para crear un procesador personalizado, hereda de esta clase e
        implementa el método abstracto `process`:

        >>> class MiProcesador(BaseProcessor):
        ...     def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        ...         # Implementación del procesamiento
        ...         return df
    """

    def __init__(self, config: SicossConfig):
        """Inicializa el procesador base con la configuración proporcionada.

        Args:
            config (SicossConfig): Configuración del procesador que contiene
                los parámetros necesarios para el procesamiento. La estructura
                y contenido de este diccionario depende de la implementación
                específica de cada procesador.

        Raises:
            TypeError: Si `config` no es un diccionario o no es proporcionado.
        """
        self.config = config

    @abstractmethod
    def process(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Procesa datos según la lógica específica del procesador.

        Este método debe ser implementado por todas las clases que hereden de
        BaseProcessor. Permite firmas flexibles en las clases hijas para recibir
        uno o más DataFrames y otros parámetros.

        Args:
            *args: Argumentos posicionales (generalmente uno o más DataFrames).
            **kwargs: Argumentos de palabras clave adicionales.

        Returns:
            pd.DataFrame: DataFrame procesado con las transformaciones aplicadas.
        """
        pass

    def _log_process_info(
        self, process_name: str, input_rows: int, output_rows: int
    ) -> None:
        """Registra información sobre el procesamiento realizado.

        Este método de utilidad permite registrar de forma consistente la
        información sobre el procesamiento de datos, incluyendo el nombre del
        proceso y el número de filas antes y después del procesamiento.

        Args:
            process_name (str): Nombre descriptivo del proceso que se está
                ejecutando. Debe ser claro y conciso para facilitar la
                identificación en los logs.
            input_rows (int): Número de filas en el DataFrame antes del
                procesamiento. Debe ser un número entero no negativo.
            output_rows (int): Número de filas en el DataFrame después del
                procesamiento. Debe ser un número entero no negativo.

        Returns:
            None: Este método no retorna ningún valor.

        Example:
            >>> processor._log_process_info("Filtrado de duplicados", 1000, 950)
            # Log: "Filtrado de duplicados: 1000 → 950 registros"

        Note:
            Este método utiliza el logger configurado a nivel de módulo. Asegúrate
            de que el logging esté configurado correctamente para ver los mensajes.
        """
        logger.info(f"{process_name}: {input_rows} → {output_rows} registros")
