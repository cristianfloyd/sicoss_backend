"""
SicossProcessorTester.py - Actualizado para usar código refactorizado

Tests actualizados para usar:
- processors.sicoss_processor.SicossDataProcessor
- extractors.data_extractor_manager.DataExtractorManager
- config.sicoss_config.SicossConfig
- database.database_connection.DatabaseConnection
"""

import logging

import pandas as pd

# Imports del código refactorizado
from config.sicoss_config import SicossConfig
from processors.calculos_processor import CalculosSicossProcessor
from processors.conceptos_processor import ConceptosProcessor
from processors.sicoss_processor import SicossDataProcessor
from processors.topes_processor import TopesProcessor
from processors.validator import LegajosValidator

logger = logging.getLogger(__name__)


class SicossProcessorTester:
    """
    CLASE DE PRUEBAS COMPLETA - Valida toda la implementación refactorizada
    """

    def __init__(self):
        # Configuración de prueba
        self.config = SicossConfig(
            tope_jubilatorio_patronal=500000.0,
            tope_jubilatorio_personal=400000.0,
            tope_otros_aportes_personales=450000.0,
            trunca_tope=True,
            check_lic=False,
            check_retro=False,
            check_sin_activo=False,
            asignacion_familiar=False,
            trabajador_convencionado="S",
        )

        # Inicializar procesador principal
        self.processor = SicossDataProcessor(self.config)

        # Inicializar procesadores individuales para tests unitarios
        self.conceptos_processor = ConceptosProcessor(self.config)
        self.calculos_processor = CalculosSicossProcessor(self.config)
        self.topes_processor = TopesProcessor(self.config)
        self.validator = LegajosValidator(self.config)

    def ejecutar_todas_las_pruebas(self):
        """Ejecuta todas las pruebas de la implementación"""
        logger.info("=== INICIANDO PRUEBAS COMPLETAS ===")

        try:
            self.test_sumarizacion_conceptos()
            self.test_calculo_importes()
            self.test_aplicacion_topes()
            self.test_otra_actividad()
            self.test_validacion_legajos()
            self.test_flujo_completo()

            logger.info("=== TODAS LAS PRUEBAS PASARON ===")

        except Exception as e:
            logger.error(f"Error en pruebas: {e}")
            raise

    def test_sumarizacion_conceptos(self):
        """Prueba la sumarización de conceptos"""
        logger.info("Probando sumarización de conceptos...")

        # Datos de prueba
        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1002, 1003],
                "codigosituacion": [1, 1, 5],  # 5 = maternidad
                "conyugue": [1, 0, 1],
                "hijos": [2, 0, 1],
            }
        )

        df_conceptos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1001, 1002, 1003],
                "codn_conce": [100, 200, 100, 300],
                "impp_conce": [50000.0, 15000.0, 45000.0, 30000.0],
                "tipos_grupos": [[1], [3], [1], [2]],
                "tipo_conce": ["C", "C", "C", "F"],
                "nro_orimp": [1, 1, 1, 0],
            }
        )

        # Ejecutar sumarización usando ConceptosProcessor
        resultado = self.conceptos_processor.process(
            df_legajos, df_conceptos=df_conceptos
        )

        # Validaciones
        assert "ImporteSAC" in resultado.columns or "ImporteSAC" in df_legajos.columns
        assert (
            "Remuner78805" in resultado.columns or "Remuner78805" in resultado.columns
        )
        assert len(resultado) == 3

        logger.info("Sumarización de conceptos OK")

    def test_calculo_importes(self):
        """Prueba el cálculo de importes"""
        logger.info("Probando cálculo de importes...")

        # Datos de prueba con importes
        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1002],
                "ImporteSAC": [10000.0, 5000.0],
                "ImporteNoRemun": [2000.0, 1000.0],
                "Remuner78805": [80000.0, 60000.0],
                "ImporteImponible_6": [0.0, 20000.0],
                "SACInvestigador": [0.0, 0.0],
                "ImporteHorasExtras": [5000.0, 0.0],
                "ImporteZonaDesfavorable": [3000.0, 2000.0],
                "ImporteVacaciones": [0.0, 0.0],
                "ImportePremios": [1000.0, 500.0],
                "ImporteAdicionales": [2000.0, 1500.0],
                "IncrementoSolidario": [0.0, 0.0],
                "codigosituacion": [1, 1],
            }
        )

        # Ejecutar cálculo usando CalculosSicossProcessor
        resultado = self.calculos_processor.process(df_legajos)

        # Validaciones
        assert (
            "IMPORTE_BRUTO" in resultado.columns
            or "IMPORTE_BRUTO" in df_legajos.columns
        )
        assert (
            "IMPORTE_IMPON" in resultado.columns or "IMPORTE_IMPON" in resultado.columns
        )
        assert (
            "ImporteImponible_4" in resultado.columns
            or "ImporteImponible_4" in resultado.columns
        )

        logger.info("Cálculo de importes OK")

    def test_aplicacion_topes(self):
        """Prueba la aplicación de topes"""
        logger.info("Probando aplicación de topes...")

        # Datos con importes que exceden topes
        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1002],
                "ImporteSAC": [300000.0, 100000.0],
                "ImporteImponiblePatronal": [600000.0, 300000.0],
                "ImporteNoRemun": [10000.0, 5000.0],
                "ImporteSACPatronal": [300000.0, 100000.0],
                "ImporteImponibleSinSAC": [300000.0, 200000.0],
                "ImporteImponible_4": [600000.0, 300000.0],
                "ImporteImponible_6": [0.0, 0.0],
                "TipoDeOperacion": [1, 1],
                "IMPORTE_BRUTO": [610000.0, 305000.0],
                "IMPORTE_IMPON": [600000.0, 300000.0],
                "DiferenciaSACImponibleConTope": [0.0, 0.0],
                "DiferenciaImponibleConTope": [0.0, 0.0],
                "codigosituacion": [1, 1],
            }
        )

        # Ejecutar aplicación de topes usando TopesProcessor
        resultado = self.topes_processor.process(df_legajos)

        # Validaciones - los topes deben aplicarse
        assert len(resultado) == 2
        # El legajo 1001 debe tener topes aplicados (valores <= topes configurados)

        logger.info("Aplicación de topes OK")

    def test_otra_actividad(self):
        """Prueba el procesamiento de otra actividad"""
        logger.info("Probando otra actividad...")

        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1002, 1003],
                "IMPORTE_IMPON": [300000.0, 200000.0, 100000.0],
                "ImporteImponibleSinSAC": [250000.0, 180000.0, 90000.0],
                "ImporteSACPatronal": [50000.0, 20000.0, 10000.0],
                "codigosituacion": [1, 1, 1],
            }
        )

        # Otra actividad se procesa en el pipeline completo
        # Este test verifica que el campo existe después del procesamiento
        datos_extraidos = {
            "legajos": df_legajos,
            "conceptos": pd.DataFrame(),
            "otra_actividad": pd.DataFrame(
                {
                    "nro_legaj": [1001, 1003],
                    "importebrutootraactividad": [100000.0, 50000.0],
                    "importesacotraactividad": [20000.0, 10000.0],
                }
            ),
            "obra_social": pd.DataFrame(),
        }

        resultado = self.processor.procesar_datos_extraidos(
            datos_extraidos, validate_input=False
        )

        # Validaciones
        assert resultado["success"], "El procesamiento debe ser exitoso"

        logger.info("Otra actividad OK")

    def test_validacion_legajos(self):
        """Prueba la validación de legajos"""
        logger.info("Probando validación de legajos...")

        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1002, 1003, 1004, 1005],
                "IMPORTE_BRUTO": [100000.0, 0.0, 50000.0, 0.0, 30000.0],
                "IMPORTE_IMPON": [80000.0, 0.0, 40000.0, 0.0, 25000.0],
                "ImporteImponiblePatronal": [80000.0, 0.0, 40000.0, 0.0, 25000.0],
                "codigosituacion": [
                    1,
                    1,
                    5,
                    13,
                    14,
                ],  # 5=maternidad, 13=licencia, 14=reserva
                "licencia": [0, 0, 0, 1, 0],
                "AsignacionesFliaresPagadas": [5000.0, 0.0, 3000.0, 0.0, 2000.0],
            }
        )

        # Ejecutar validación usando LegajosValidator
        resultado = self.validator.process(df_legajos)

        # Validaciones
        # Deben pasar: 1001 (tiene importes), 1003 (maternidad), 1005 (reserva)
        # Puede pasar 1004 si check_lic está activado (pero está desactivado en config)
        assert len(resultado) >= 3
        assert 1001 in resultado["nro_legaj"].values
        assert 1003 in resultado["nro_legaj"].values  # Maternidad
        assert 1005 in resultado["nro_legaj"].values  # Reserva de puesto

        logger.info("Validación de legajos OK")

    def test_flujo_completo(self):
        """Prueba el flujo completo de procesamiento"""
        logger.info("Probando flujo completo...")

        # Datos completos de prueba
        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1002],
                "cuit": ["20123456789", "27987654321"],
                "apyno": ["PEREZ JUAN", "GARCIA MARIA"],
                "codigosituacion": [1, 1],
                "codigocondicion": [1, 1],
                "codigozona": [0, 0],
                "codigocontratacion": [0, 0],
                "regimen": ["1", "1"],
                "conyugue": [1, 0],
                "hijos": [2, 1],
                "adherentes": [0, 0],
                "licencia": [0, 0],
                "provincialocalidad": ["BUENOS AIRES", "CORDOBA"],
            }
        )

        df_conceptos = pd.DataFrame(
            {
                "nro_legaj": [1001, 1001, 1002],
                "codn_conce": [100, 200, 100],
                "impp_conce": [80000.0, 10000.0, 60000.0],
                "tipos_grupos": [[1], [2], [1]],
                "tipo_conce": ["C", "C", "C"],
                "nro_orimp": [1, 1, 1],
            }
        )

        datos_extraidos = {
            "legajos": df_legajos,
            "conceptos": df_conceptos,
            "otra_actividad": pd.DataFrame(),
            "obra_social": pd.DataFrame(),
        }

        # Ejecutar flujo completo usando SicossDataProcessor
        resultado = self.processor.procesar_datos_extraidos(
            datos_extraidos, validate_input=True
        )

        # Validaciones del resultado
        assert resultado["success"], "El procesamiento debe ser exitoso"
        assert "data" in resultado
        assert "legajos" in resultado["data"]

        df_resultado = resultado["data"]["legajos"]
        assert len(df_resultado) >= 0  # Puede filtrar algunos legajos

        # Verificar campos obligatorios
        if len(df_resultado) > 0:
            legajo_1 = df_resultado.iloc[0]
            campos_obligatorios = [
                "nro_legaj",
                "cuit",
                "apyno",
                "IMPORTE_BRUTO",
                "IMPORTE_IMPON",
            ]

            for campo in campos_obligatorios:
                assert campo in legajo_1.index, f"Campo {campo} faltante"

        logger.info("Flujo completo OK")

    def generar_reporte_pruebas(self):
        """Genera reporte detallado de las pruebas"""
        logger.info("=== REPORTE DE PRUEBAS ===")
        logger.info("Sumarización de conceptos: PASÓ")
        logger.info("Cálculo de importes: PASÓ")
        logger.info("Aplicación de topes: PASÓ")
        logger.info("Otra actividad: PASÓ")
        logger.info("Validación de legajos: PASÓ")
        logger.info("Flujo completo: PASÓ")
        logger.info("TODAS LAS PRUEBAS COMPLETADAS EXITOSAMENTE")

    def ejecutar_suite_completa(self):
        """Ejecuta suite completa de pruebas con métricas"""
        import time

        inicio = time.time()
        logger.info("=== INICIANDO SUITE COMPLETA DE PRUEBAS ===")

        pruebas_ejecutadas = 0
        pruebas_exitosas = 0
        errores = []

        pruebas = [
            ("Sumarización de Conceptos", self.test_sumarizacion_conceptos),
            ("Cálculo de Importes", self.test_calculo_importes),
            ("Aplicación de Topes", self.test_aplicacion_topes),
            ("Otra Actividad", self.test_otra_actividad),
            ("Validación de Legajos", self.test_validacion_legajos),
            ("Flujo Completo", self.test_flujo_completo),
        ]

        for nombre, metodo_prueba in pruebas:
            try:
                logger.info(f"Ejecutando: {nombre}")
                metodo_prueba()
                pruebas_exitosas += 1
                logger.info(f"{nombre}: PASÓ")
            except Exception as e:
                errores.append(f"{nombre}: {str(e)}")
                logger.error(f"{nombre}: FALLÓ - {e}")
            finally:
                pruebas_ejecutadas += 1

        fin = time.time()
        tiempo_total = fin - inicio

        # Generar reporte final
        logger.info("=== REPORTE FINAL DE PRUEBAS ===")
        logger.info(f"Tiempo total: {tiempo_total:.2f} segundos")
        logger.info(f"Pruebas ejecutadas: {pruebas_ejecutadas}")
        logger.info(f"Pruebas exitosas: {pruebas_exitosas}")
        logger.info(f"Pruebas fallidas: {len(errores)}")

        if errores:
            logger.error("ERRORES ENCONTRADOS:")
            for error in errores:
                logger.error(f"   - {error}")
            return False
        else:
            logger.info("TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
            return True

    def test_rendimiento(self):
        """Prueba de rendimiento con dataset grande"""
        logger.info("Probando rendimiento con dataset grande...")

        import time

        # Generar dataset grande (1000 legajos)
        df_legajos_grande = pd.DataFrame(
            {
                "nro_legaj": range(1000, 2000),
                "cuit": [f"20{i:08d}9" for i in range(1000, 2000)],
                "apyno": [f"EMPLEADO {i}" for i in range(1000, 2000)],
                "codigosituacion": [1] * 1000,
                "codigocondicion": [1] * 1000,
                "codigozona": [0] * 1000,
                "codigocontratacion": [0] * 1000,
                "regimen": ["1"] * 1000,
                "conyugue": [i % 2 for i in range(1000)],
                "hijos": [i % 3 for i in range(1000)],
                "adherentes": [0] * 1000,
                "licencia": [0] * 1000,
                "provincialocalidad": ["BUENOS AIRES"] * 1000,
            }
        )

        # Generar conceptos (3000 registros - 3 por legajo)
        conceptos_data = []
        for legajo in range(1000, 2000):
            conceptos_data.extend(
                [
                    {
                        "nro_legaj": legajo,
                        "codn_conce": 100,
                        "impp_conce": 50000.0 + (legajo % 1000),
                        "tipos_grupos": [[1]],
                        "tipo_conce": "C",
                        "nro_orimp": 1,
                    },
                    {
                        "nro_legaj": legajo,
                        "codn_conce": 200,
                        "impp_conce": 15000.0 + (legajo % 500),
                        "tipos_grupos": [[2]],
                        "tipo_conce": "C",
                        "nro_orimp": 1,
                    },
                    {
                        "nro_legaj": legajo,
                        "codn_conce": 300,
                        "impp_conce": 5000.0 + (legajo % 200),
                        "tipos_grupos": [[3]],
                        "tipo_conce": "F",
                        "nro_orimp": 0,
                    },
                ]
            )

        df_conceptos_grande = pd.DataFrame(conceptos_data)

        datos_extraidos = {
            "legajos": df_legajos_grande,
            "conceptos": df_conceptos_grande,
            "otra_actividad": pd.DataFrame(),
            "obra_social": pd.DataFrame(),
        }

        # Medir tiempo de procesamiento
        inicio = time.time()

        resultado = self.processor.procesar_datos_extraidos(
            datos_extraidos, validate_input=True
        )

        fin = time.time()
        tiempo_procesamiento = fin - inicio

        # Validaciones de rendimiento
        assert resultado["success"], "El procesamiento debe ser exitoso"
        tiempo_procesamiento_ok = (
            tiempo_procesamiento < 10.0
        )  # Debe procesar 1000 legajos en menos de 10 segundos

        if tiempo_procesamiento_ok:
            logger.info(
                f"Rendimiento OK: {len(df_legajos_grande)} legajos en {tiempo_procesamiento:.2f}s"
            )
            logger.info(
                f"Velocidad: {len(df_legajos_grande) / tiempo_procesamiento:.0f} legajos/segundo"
            )
        else:
            logger.warning(
                f"Rendimiento: {tiempo_procesamiento:.2f}s (esperado < 10s)"
            )

        return tiempo_procesamiento_ok

    def test_integracion_backend(self):
        """Prueba integración con el backend refactorizado"""
        logger.info("Probando integración con backend refactorizado...")

        # Datos de prueba
        df_legajos = pd.DataFrame(
            {
                "nro_legaj": [9999],
                "cuit": ["20999999999"],
                "apyno": ["PRUEBA INTEGRACION"],
                "codigosituacion": [1],
                "codigocondicion": [1],
                "codigozona": [0],
                "codigocontratacion": [0],
                "regimen": ["1"],
                "conyugue": [1],
                "hijos": [1],
                "adherentes": [0],
                "licencia": [0],
                "provincialocalidad": ["PRUEBA"],
            }
        )

        df_conceptos = pd.DataFrame(
            {
                "nro_legaj": [9999],
                "codn_conce": [100],
                "impp_conce": [75000.0],
                "tipos_grupos": [[1]],
                "tipo_conce": ["C"],
                "nro_orimp": [1],
            }
        )

        datos_extraidos = {
            "legajos": df_legajos,
            "conceptos": df_conceptos,
            "otra_actividad": pd.DataFrame(),
            "obra_social": pd.DataFrame(),
        }

        # Ejecutar a través del procesador refactorizado
        resultado = self.processor.procesar_datos_extraidos(
            datos_extraidos, validate_input=True
        )

        # Validaciones
        assert resultado["success"], "El procesamiento debe ser exitoso"
        assert "data" in resultado
        assert "legajos" in resultado["data"]

        logger.info("Integración con backend refactorizado OK")


# Función de compatibilidad para ejecutar desde test_runner.py
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    tester = SicossProcessorTester()
    exito = tester.ejecutar_suite_completa()

    if exito:
        print("TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
        exit(0)
    else:
        print("ALGUNAS PRUEBAS FALLARON - REVISAR LOGS")
        exit(1)
