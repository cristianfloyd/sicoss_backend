"""
Script de Validación Legacy - Comparación TXT Refactor vs PHP Original

Compara la salida TXT del sistema refactorizado (Python) con la salida TXT
del sistema PHP original para validar que los cálculos son equivalentes.

Uso:
    python scripts/validacion_legacy_txt.py --periodo 202501 --legajo 1001
    python scripts/validacion_legacy_txt.py --archivo-python output_python.txt --archivo-php output_php.txt
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from extractors.data_extractor_manager import DataExtractorManager
from processors.sicoss_processor import SicossDataProcessor
from value_objects.periodo_fiscal import PeriodoFiscal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ExportadorTXT:
    """Exportador de datos SICOSS a formato TXT (compatible con PHP legacy)"""

    def __init__(self):
        """Inicializa el exportador TXT"""
        logger.info("ExportadorTXT inicializado")

    def exportar_legajos_a_txt(
        self, df_legajos: pd.DataFrame, archivo_salida: str
    ) -> str:
        """
        Exporta legajos procesados a archivo TXT en formato SICOSS

        Args:
            df_legajos: DataFrame con legajos procesados
            archivo_salida: Ruta del archivo de salida

        Returns:
            str: Ruta del archivo generado
        """
        logger.info(f"Exportando {len(df_legajos)} legajos a {archivo_salida}")

        # Crear directorio si no existe
        Path(archivo_salida).parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(archivo_salida, "w", encoding="latin1") as f:
                for _, legajo in df_legajos.iterrows():
                    linea = self._formatear_linea_sicoss(legajo)
                    f.write(linea + "\r\n")

            logger.info(f"Archivo TXT generado: {archivo_salida}")
            return archivo_salida

        except Exception as e:
            logger.error(f"Error exportando TXT: {e}")
            raise

    def _formatear_linea_sicoss(self, legajo: pd.Series) -> str:
        """
        Formatea una línea del archivo TXT SICOSS según especificaciones

        Formato esperado (similar al PHP legacy):
        - CUIL: 11 caracteres
        - Apellido y Nombre: 30 caracteres (rellenado con blancos)
        - Cónyuge: 1 carácter
        - Cantidad de Hijos: 2 caracteres (rellenado con ceros)
        - Código de Situación: 2 caracteres
        - ... (resto de campos según especificación SICOSS)
        """
        # Mapeo de campos a formato TXT
        cuit = str(legajo.get("cuit", legajo.get("cuil", "00000000000"))).zfill(11)[:11]
        apyno = self._llenar_blancos(
            str(legajo.get("apyno", legajo.get("apnom", ""))), 30
        )
        conyuge = str(legajo.get("conyuge", 0))[:1]
        hijos = str(int(legajo.get("cant_hijos", legajo.get("hijos", 0)))).zfill(2)[:2]
        cod_situacion = str(
            int(legajo.get("codigosituacion", legajo.get("cod_situacion", 0)))
        ).zfill(2)[:2]
        cod_cond = str(int(legajo.get("cod_cond", 0))).zfill(2)[:2]
        cod_act = str(
            int(legajo.get("TipoDeActividad", legajo.get("cod_act", 0)))
        ).zfill(2)[:2]
        cod_zona = str(int(legajo.get("cod_zona", 0))).zfill(2)[:2]
        porc_aporte = str(int(legajo.get("porc_aporte", 0))).zfill(3)[:3]
        cod_mod_cont = str(int(legajo.get("cod_mod_cont", 0))).zfill(2)[:2]
        cod_os = str(legajo.get("codigo_os", legajo.get("cod_os", "000000"))).zfill(6)[
            :6
        ]
        cant_adh = str(int(legajo.get("adherentes", legajo.get("cant_adh", 0)))).zfill(
            2
        )[:2]

        # Importes (formato con ceros a la izquierda, sin decimales)
        rem_total = str(int(legajo.get("IMPORTE_BRUTO", 0))).zfill(10)[:10]
        rem_impo1 = str(int(legajo.get("ImporteImponible_4", 0))).zfill(10)[:10]
        rem_impo2 = str(int(legajo.get("ImporteImponible_5", 0))).zfill(10)[:10]
        rem_impo3 = str(int(legajo.get("ImporteImponible_6", 0))).zfill(10)[:10]
        rem_impo4 = str(int(legajo.get("ImporteImponible_4", 0))).zfill(10)[
            :10
        ]  # Puede variar según lógica
        rem_impo5 = str(int(legajo.get("ImporteImponible_5", 0))).zfill(10)[
            :10
        ]  # Puede variar según lógica
        sac = str(int(legajo.get("ImporteSAC", 0))).zfill(10)[:10]

        # Construir línea completa
        linea = (
            cuit
            + apyno
            + conyuge
            + hijos
            + cod_situacion
            + cod_cond
            + cod_act
            + cod_zona
            + porc_aporte
            + cod_mod_cont
            + cod_os
            + cant_adh
            + rem_total
            + rem_impo1
            + rem_impo2
            + rem_impo3
            + rem_impo4
            + rem_impo5
            + sac
        )

        return linea

    def _llenar_blancos(self, texto: str, longitud: int) -> str:
        """Llena con blancos a la derecha hasta la longitud especificada"""
        return texto.ljust(longitud)[:longitud]


class ComparadorTXT:
    """Comparador de archivos TXT SICOSS"""

    def __init__(self, tolerancia_caracteres: int = 0):
        """
        Inicializa el comparador

        Args:
            tolerancia_caracteres: Número de caracteres de diferencia permitidos por línea
        """
        self.tolerancia_caracteres = tolerancia_caracteres
        logger.info("ComparadorTXT inicializado")

    def comparar_archivos(
        self, archivo_python: str, archivo_php: str
    ) -> Dict[str, Optional[str]]:
        """
        Compara dos archivos TXT línea por línea

        Args:
            archivo_python: Ruta del archivo TXT generado por Python
            archivo_php: Ruta del archivo TXT generado por PHP

        Returns:
            Dict con resultados de la comparación
        """
        logger.info("Comparando archivos:")
        logger.info(f"   Python: {archivo_python}")
        logger.info(f"   PHP:    {archivo_php}")

        try:
            # Leer archivos
            with open(archivo_python, "r", encoding="latin1") as f:
                lineas_python = [linea.rstrip("\r\n") for linea in f.readlines()]

            with open(archivo_php, "r", encoding="latin1") as f:
                lineas_php = [linea.rstrip("\r\n") for linea in f.readlines()]

            # Comparar
            total_lineas = max(len(lineas_python), len(lineas_php))
            lineas_iguales = 0
            lineas_diferentes = []
            lineas_faltantes_python = []
            lineas_faltantes_php = []

            for i in range(total_lineas):
                if i >= len(lineas_python):
                    lineas_faltantes_python.append((i + 1, lineas_php[i]))
                    continue

                if i >= len(lineas_php):
                    lineas_faltantes_php.append((i + 1, lineas_python[i]))
                    continue

                linea_py = lineas_python[i]
                linea_php = lineas_php[i]

                if linea_py == linea_php:
                    lineas_iguales += 1
                else:
                    diferencias = self._analizar_diferencias_linea(
                        linea_py, linea_php, i + 1
                    )
                    lineas_diferentes.append(diferencias)

            # Calcular estadísticas
            porcentaje_coincidencia = (
                (lineas_iguales / total_lineas * 100) if total_lineas > 0 else 0
            )

            resultado = {
                "total_lineas": total_lineas,
                "lineas_iguales": lineas_iguales,
                "lineas_diferentes": len(lineas_diferentes),
                "lineas_faltantes_python": len(lineas_faltantes_python),
                "lineas_faltantes_php": len(lineas_faltantes_php),
                "porcentaje_coincidencia": porcentaje_coincidencia,
                "detalles_diferencias": lineas_diferentes[
                    :100
                ],  # Primeras 100 diferencias
                "lineas_faltantes_python_detalle": lineas_faltantes_python[:50],
                "lineas_faltantes_php_detalle": lineas_faltantes_php[:50],
            }

            logger.info("Comparación completada:")
            logger.info(f"   Total líneas: {total_lineas}")
            logger.info(f"   Líneas iguales: {lineas_iguales}")
            logger.info(f"   Líneas diferentes: {len(lineas_diferentes)}")
            logger.info(f"   Coincidencia: {porcentaje_coincidencia:.2f}%")

            return resultado

        except FileNotFoundError as e:
            logger.error(f"Archivo no encontrado: {e}")
            raise
        except Exception as e:
            logger.error(f"Error comparando archivos: {e}")
            raise

    def _analizar_diferencias_linea(
        self, linea_py: str, linea_php: str, numero_linea: int
    ) -> Dict:
        """Analiza las diferencias entre dos líneas"""
        diferencias_posiciones = []
        longitud_min = min(len(linea_py), len(linea_php))

        for i in range(longitud_min):
            if linea_py[i] != linea_php[i]:
                diferencias_posiciones.append(i)

        # Si las longitudes son diferentes
        if len(linea_py) != len(linea_php):
            diferencias_posiciones.append(
                f"Longitud: Python={len(linea_py)}, PHP={len(linea_php)}"
            )

        return {
            "numero_linea": numero_linea,
            "linea_python": linea_py[:100],  # Primeros 100 caracteres
            "linea_php": linea_php[:100],
            "posiciones_diferentes": diferencias_posiciones[
                :20
            ],  # Primeras 20 posiciones
            "total_diferencias": len(diferencias_posiciones),
        }


def generar_txt_refactorizado(
    periodo: PeriodoFiscal,
    legajo: Optional[int] = None,
    archivo_salida: Optional[str] = None,
) -> str:
    """
    Genera archivo TXT desde el sistema refactorizado

    Args:
        periodo: Período fiscal a procesar
        legajo: Número de legajo específico (opcional)
        archivo_salida: Ruta del archivo de salida (opcional)

    Returns:
        str: Ruta del archivo generado
    """
    logger.info(f"Generando TXT refactorizado para período {periodo}")

    # Configuración
    config = SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
    )

    # Conexión BD
    db = DatabaseConnection()

    # Extraer datos
    extractor = DataExtractorManager(db)
    datos = extractor.extraer_datos_completos(
        config=config,
        per_anoct=periodo.year,
        per_mesct=periodo.month,
        nro_legajo=legajo,
    )

    # Procesar
    processor = SicossDataProcessor(config)
    resultado = processor.procesar_datos_extraidos(datos, validate_input=True)

    # Exportar a TXT
    exportador = ExportadorTXT()
    if archivo_salida is None:
        archivo_salida = f"output_refactor_{periodo.periodo_str}.txt"

    return exportador.exportar_legajos_a_txt(
        resultado["legajos_procesados"], archivo_salida
    )


def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(
        description="Compara salida TXT del refactor vs PHP original"
    )
    parser.add_argument(
        "--periodo",
        type=str,
        help="Período en formato YYYYMM (ej: 202501)",
    )
    parser.add_argument(
        "--legajo",
        type=int,
        help="Número de legajo específico (opcional)",
    )
    parser.add_argument(
        "--archivo-python",
        type=str,
        help="Ruta del archivo TXT generado por Python",
    )
    parser.add_argument(
        "--archivo-php",
        type=str,
        help="Ruta del archivo TXT generado por PHP",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="comparacion_legacy.txt",
        help="Archivo de salida para el reporte",
    )

    args = parser.parse_args()

    try:
        # Modo 1: Generar TXT desde refactor y comparar con PHP existente
        if args.periodo and args.archivo_php:
            periodo = PeriodoFiscal.from_string(args.periodo)
            archivo_python = generar_txt_refactorizado(
                periodo, args.legajo, f"output_refactor_{periodo.periodo_str}.txt"
            )

            comparador = ComparadorTXT()
            resultado = comparador.comparar_archivos(archivo_python, args.archivo_php)

        # Modo 2: Comparar dos archivos TXT existentes
        elif args.archivo_python and args.archivo_php:
            comparador = ComparadorTXT()
            resultado = comparador.comparar_archivos(
                args.archivo_python, args.archivo_php
            )

        else:
            parser.print_help()
            return 1

        # Generar reporte
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("REPORTE DE COMPARACIÓN LEGACY - TXT REFACTOR vs PHP\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Total de líneas: {resultado['total_lineas']}\n")
            f.write(f"Líneas iguales: {resultado['lineas_iguales']}\n")
            f.write(f"Líneas diferentes: {resultado['lineas_diferentes']}\n")
            f.write(
                f"Porcentaje de coincidencia: {resultado['porcentaje_coincidencia']:.2f}%\n\n"
            )

            if resultado["lineas_diferentes"] > 0:
                f.write("DIFERENCIAS ENCONTRADAS:\n")
                f.write("-" * 80 + "\n")
                for diff in resultado["detalles_diferencias"]:
                    f.write(f"\nLínea {diff['numero_linea']}:\n")
                    f.write(f"  Python: {diff['linea_python']}\n")
                    f.write(f"  PHP:    {diff['linea_php']}\n")
                    f.write(
                        f"  Diferencias en posiciones: {diff['posiciones_diferentes']}\n"
                    )

            if resultado["lineas_faltantes_python"] > 0:
                f.write(
                    f"\n⚠️  {resultado['lineas_faltantes_python']} líneas faltantes en Python\n"
                )

            if resultado["lineas_faltantes_php"] > 0:
                f.write(
                    f"\n⚠️  {resultado['lineas_faltantes_php']} líneas faltantes en PHP\n"
                )

        logger.info(f"✅ Reporte generado: {args.output}")

        # Retornar código de salida según resultado
        if resultado["porcentaje_coincidencia"] == 100.0:
            logger.info("🎉 ¡ÉXITO TOTAL! Los archivos son idénticos")
            return 0
        elif resultado["porcentaje_coincidencia"] >= 99.0:
            logger.warning("⚠️  Coincidencia alta pero con diferencias menores")
            return 0
        else:
            logger.error("❌ Se encontraron diferencias significativas")
            return 1

    except Exception as e:
        logger.error(f"❌ Error en validación legacy: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
