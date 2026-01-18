#!/usr/bin/env python3
"""
test_topes_processor.py

Script de prueba para validar el TopesProcessor
Uso: python test_topes_processor.py --legajo 123456 [--periodo 2024/12]
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import Any, Dict

import pandas as pd

from config.sicoss_config import SicossConfig

# Imports del proyecto
from database.database_connection import DatabaseConnection
from extractors.conceptos_extractor import ConceptosExtractor
from extractors.legajos_extractor import LegajosExtractor
from processors.conceptos_processor import ConceptosProcessor
from processors.topes_processor import TopesProcessor

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TopesProcessorTester:
    """Tester para validar TopesProcessor"""

    def __init__(self):
        self.db = None
        self.config = None
        self.legajos_extractor = None
        self.conceptos_extractor = None
        self.conceptos_processor = None
        self.topes_processor = None

    def setup(self):
        """Inicializa conexiones y componentes"""
        try:
            logger.info("🔧 Inicializando conexiones...")

            # Conexión a base de datos
            self.db = DatabaseConnection()

            # Obtener topes desde la base de datos usando MapucheConfig
            logger.info("📊 Obteniendo topes desde la base de datos...")

            # Leer configuración de BD
            import configparser

            config_ini = configparser.ConfigParser()
            config_ini.read("database.ini")
            db_params = config_ini["postgresql"]

            # Crear parámetros de conexión con el tipo correcto
            from mapuche_config import ConnectionParams, create_mapuche_config

            connection_params: ConnectionParams = {
                "host": db_params.get("host", "localhost"),
                "database": db_params.get("database", ""),
                "user": db_params.get("user", ""),
                "password": db_params.get("password", ""),
                "port": db_params.get("port", "5432"),
            }

            # Crear instancia de MapucheConfig para obtener topes
            mapuche_config = create_mapuche_config(connection_params)

            # Obtener topes desde BD
            tope_jubilatorio_patronal = float(
                mapuche_config.get_topes_jubilatorio_patronal() or 0
            )
            tope_jubilatorio_personal = float(
                mapuche_config.get_topes_jubilatorio_personal() or 0
            )
            tope_otros_aportes_personales = float(
                mapuche_config.get_topes_otros_aportes_personales() or 0
            )

            logger.info(
                f"   💰 Tope jubilatorio patronal: ${tope_jubilatorio_patronal:,.2f}"
            )
            logger.info(
                f"   💰 Tope jubilatorio personal: ${tope_jubilatorio_personal:,.2f}"
            )
            logger.info(
                f"   💰 Tope otros aportes: ${tope_otros_aportes_personales:,.2f}"
            )

            # Configuración con topes reales
            self.config = SicossConfig(
                tope_jubilatorio_patronal=tope_jubilatorio_patronal,
                tope_jubilatorio_personal=tope_jubilatorio_personal,
                tope_otros_aportes_personales=tope_otros_aportes_personales,
                trunca_tope=True,
                check_lic=False,
                check_retro=False,
                check_sin_activo=False,
                asignacion_familiar=False,
                trabajador_convencionado="S",
            )

            # Extractores
            self.legajos_extractor = LegajosExtractor(self.db)
            self.conceptos_extractor = ConceptosExtractor(self.db)

            # Procesadores
            self.conceptos_processor = ConceptosProcessor(self.config)
            self.topes_processor = TopesProcessor(self.config)

            logger.info("✅ Setup completado con topes reales desde BD")

        except Exception as e:
            logger.error(f"❌ Error en setup: {e}")
            raise

    def test_legajo(
        self, nro_legaj: int, per_anoct: int, per_mesct: int
    ) -> Dict[str, Any]:
        """Prueba el procesamiento de topes para un legajo específico"""
        logger.info(
            f"🧪 Probando topes para legajo {nro_legaj} - período {per_anoct}/{per_mesct}"
        )

        try:
            # 1. EXTRAER DATOS DEL LEGAJO
            logger.info("📥 Extrayendo datos del legajo...")
            where_legajo = f"dh01.nro_legaj = {nro_legaj}"

            df_legajos = self.legajos_extractor.extract(
                per_anoct, per_mesct, where_legajo
            )

            if df_legajos.empty:
                return {
                    "error": f"No se encontró el legajo {nro_legaj}",
                    "legajo_encontrado": False,
                }

            logger.info(f"✅ Legajo encontrado: {df_legajos.iloc[0]['apyno']}")

            # 2. EXTRAER CONCEPTOS DEL LEGAJO
            logger.info("📥 Extrayendo conceptos liquidados...")
            where_conceptos = f"dh31.nro_legaj = {nro_legaj}"

            df_conceptos = self.conceptos_extractor.extract(
                per_anoct, per_mesct, where_conceptos
            )

            logger.info(f"📊 Conceptos encontrados: {len(df_conceptos)}")

            # 3. PROCESAR CONCEPTOS PRIMERO (necesario para topes)
            logger.info("⚡ Procesando conceptos...")
            start_time = pd.Timestamp.now()

            df_procesado = self.conceptos_processor.process(df_legajos, df_conceptos)

            elapsed = (pd.Timestamp.now() - start_time).total_seconds()
            logger.info(f"⏱️ Conceptos procesados en {elapsed:.4f}s")

            # 4. CREAR DATOS SIMULADOS PARA TOPES (campos que necesita TopesProcessor)
            self._preparar_datos_para_topes(df_procesado)

            # 5. APLICAR TOPES
            logger.info("🔢 Aplicando topes...")
            start_time = pd.Timestamp.now()

            df_con_topes = self.topes_processor.process(df_procesado)

            elapsed = (pd.Timestamp.now() - start_time).total_seconds()
            logger.info(f"⏱️ Topes aplicados en {elapsed:.4f}s")

            return {
                "legajo_encontrado": True,
                "legajo_data": df_legajos.iloc[0].to_dict(),
                "conceptos_count": len(df_conceptos),
                "datos_antes_topes": df_procesado.iloc[0].to_dict(),
                "datos_despues_topes": df_con_topes.iloc[0].to_dict(),
                "topes_aplicados": self._analizar_topes_aplicados(
                    df_procesado.iloc[0], df_con_topes.iloc[0]
                ),
            }

        except Exception as e:
            logger.error(f"❌ Error procesando legajo: {e}")
            return {"error": str(e), "legajo_encontrado": True}

    def _preparar_datos_para_topes(self, df: pd.DataFrame):
        """Prepara datos simulados necesarios para TopesProcessor"""
        # Inicializar campos necesarios que TopesProcessor espera
        campos_necesarios = [
            "ImporteImponiblePatronal",
            "ImporteImponibleSinSAC",
            "ImporteSACPatronal",
            "DiferenciaSACImponibleConTope",
            "DiferenciaImponibleConTope",
            "IMPORTE_BRUTO",
        ]

        for campo in campos_necesarios:
            if campo not in df.columns:
                df[campo] = 0.0

        # Simular cálculos básicos (esto normalmente lo haría CalculosProcessor)
        df["ImporteImponiblePatronal"] = df.get("ImporteSAC", 0) + df.get(
            "ImporteAdicionales", 0
        )
        df["ImporteSACPatronal"] = df.get("ImporteSAC", 0)
        df["ImporteImponibleSinSAC"] = (
            df["ImporteImponiblePatronal"] - df["ImporteSACPatronal"]
        )
        df["IMPORTE_BRUTO"] = df["ImporteImponiblePatronal"] + df.get(
            "ImporteNoRemun", 0
        )

    def _analizar_topes_aplicados(
        self, antes: pd.Series, despues: pd.Series
    ) -> Dict[str, Any]:
        """Analiza qué topes se aplicaron comparando antes vs después"""
        analisis = {
            "tope_sac_aplicado": False,
            "tope_imponible_aplicado": False,
            "diferencia_sac": 0.0,
            "diferencia_imponible": 0.0,
            "importe_bruto_cambio": 0.0,
        }

        # Tope SAC
        if despues.get("DiferenciaSACImponibleConTope", 0) > 0:
            analisis["tope_sac_aplicado"] = True
            analisis["diferencia_sac"] = despues.get("DiferenciaSACImponibleConTope", 0)

        # Tope Imponible
        if despues.get("DiferenciaImponibleConTope", 0) > 0:
            analisis["tope_imponible_aplicado"] = True
            analisis["diferencia_imponible"] = despues.get(
                "DiferenciaImponibleConTope", 0
            )

        # Cambio en IMPORTE_BRUTO
        bruto_antes = antes.get("IMPORTE_BRUTO", 0)
        bruto_despues = despues.get("IMPORTE_BRUTO", 0)
        analisis["importe_bruto_cambio"] = bruto_despues - bruto_antes

        return analisis

    def cleanup(self):
        """Limpia recursos"""
        if self.db:
            self.db.close()
            logger.info("🧹 Cleanup completado")


def mostrar_resultados(resultados: Dict[str, Any]):
    """Muestra los resultados del test de manera organizada"""
    print("=" * 80)
    print("🧪 RESULTADOS DE PRUEBA - TOPES PROCESSOR")
    print("=" * 80)

    if not resultados.get("legajo_encontrado", False):
        print(f"❌ {resultados.get('error', 'Error desconocido')}")
        return

    # Información del legajo
    legajo_data = resultados["legajo_data"]
    print(f"👤 LEGAJO: {legajo_data['nro_legaj']}")
    print(f"   Nombre: {legajo_data['apyno']}")
    print(f"   CUIT: {legajo_data.get('cuit', 'N/A')}")
    print(f"   Estado: {legajo_data.get('estado', 'N/A')}")
    print()

    # Estadísticas
    print(f"📊 ESTADÍSTICAS:")
    print(f"   Conceptos procesados: {resultados['conceptos_count']}")
    print()

    # Datos antes y después de topes
    antes = resultados["datos_antes_topes"]
    despues = resultados["datos_despues_topes"]

    print("💰 IMPORTES ANTES DE TOPES:")
    print("-" * 60)
    print(f"   ImporteSAC                 | ${antes.get('ImporteSAC', 0):>15,.2f}")
    print(
        f"   ImporteImponiblePatronal   | ${antes.get('ImporteImponiblePatronal', 0):>15,.2f}"
    )
    print(
        f"   ImporteImponibleSinSAC     | ${antes.get('ImporteImponibleSinSAC', 0):>15,.2f}"
    )
    print(f"   IMPORTE_BRUTO              | ${antes.get('IMPORTE_BRUTO', 0):>15,.2f}")
    print()

    print("🔢 IMPORTES DESPUÉS DE TOPES:")
    print("-" * 60)
    print(
        f"   ImporteSACPatronal         | ${despues.get('ImporteSACPatronal', 0):>15,.2f}"
    )
    print(
        f"   ImporteImponiblePatronal   | ${despues.get('ImporteImponiblePatronal', 0):>15,.2f}"
    )
    print(
        f"   ImporteImponibleSinSAC     | ${despues.get('ImporteImponibleSinSAC', 0):>15,.2f}"
    )
    print(f"   IMPORTE_BRUTO              | ${despues.get('IMPORTE_BRUTO', 0):>15,.2f}")
    print()

    # Análisis de topes aplicados
    topes = resultados["topes_aplicados"]
    print("🎯 ANÁLISIS DE TOPES APLICADOS:")
    print("-" * 60)

    if topes["tope_sac_aplicado"]:
        print(
            f"   ✅ Tope SAC aplicado      | Diferencia: ${topes['diferencia_sac']:>10,.2f}"
        )
    else:
        print(f"   ❌ Tope SAC NO aplicado   | Sin exceso")

    if topes["tope_imponible_aplicado"]:
        print(
            f"   ✅ Tope Imponible aplicado| Diferencia: ${topes['diferencia_imponible']:>10,.2f}"
        )
    else:
        print(f"   ❌ Tope Imponible NO aplicado| Sin exceso")

    if abs(topes["importe_bruto_cambio"]) > 0.01:
        print(
            f"   📊 Cambio IMPORTE_BRUTO   | ${topes['importe_bruto_cambio']:>+15,.2f}"
        )

    print("=" * 80)
    print("✅ PRUEBA COMPLETADA")
    print("=" * 80)


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description="Test TopesProcessor con legajo específico"
    )
    parser.add_argument(
        "--legajo", "-l", type=int, required=True, help="Número de legajo"
    )
    parser.add_argument(
        "--periodo", "-p", type=str, help="Período YYYY/MM (default: actual)"
    )
    parser.add_argument("--debug", "-d", action="store_true", help="Modo debug")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Período por defecto
    if args.periodo:
        try:
            year, month = map(int, args.periodo.split("/"))
        except ValueError:
            print("❌ Formato de período inválido. Use YYYY/MM")
            return 1
    else:
        now = datetime.now()
        year, month = now.year, now.month

    # Ejecutar test
    tester = TopesProcessorTester()

    try:
        tester.setup()
        resultados = tester.test_legajo(args.legajo, year, month)
        mostrar_resultados(resultados)
        return 0

    except Exception as e:
        logger.error(f"❌ Error en test: {e}")
        return 1

    finally:
        tester.cleanup()


if __name__ == "__main__":
    sys.exit(main())
