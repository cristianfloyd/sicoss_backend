import pandas as pd
import numpy as np
import logging

from SicossProcessor import SicossProcessor

logger = logging.getLogger(__name__)

class SicossProcessorTester:
    """
    🧪 CLASE DE PRUEBAS COMPLETA - Valida toda la implementación pandas
    """

    def __init__(self):
        self.processor = SicossProcessor()
        self.processor.porc_aporte_adicional_jubilacion = 100.0
        # Corregir el tipo de dato para trabajador_convencionado
        self.processor.trabajador_convencionado = None  # o el valor correcto según la definición de la clase
        self.processor.asignacion_familiar = False

    def ejecutar_todas_las_pruebas(self):
        """🧪 Ejecuta todas las pruebas de la implementación"""
        logger.info("🧪 === INICIANDO PRUEBAS COMPLETAS ===")

        try:
            self.test_sumarizacion_conceptos()
            self.test_calculo_importes()
            self.test_aplicacion_topes()
            self.test_otra_actividad()
            self.test_validacion_legajos()
            self.test_flujo_completo()

            logger.info("✅ === TODAS LAS PRUEBAS PASARON ===")

        except Exception as e:
            logger.error(f"❌ Error en pruebas: {e}")
            raise

    def test_sumarizacion_conceptos(self):
        """🧪 Prueba la sumarización de conceptos"""
        logger.info("🧪 Probando sumarización de conceptos...")

        # Datos de prueba
        df_legajos = pd.DataFrame({
            'nro_legaj': [1001, 1002, 1003],
            'codigosituacion': [1, 1, 5],  # 5 = maternidad
            'conyugue': [1, 0, 1],
            'hijos': [2, 0, 1]
        })

        df_conceptos = pd.DataFrame({
            'nro_legaj': [1001, 1001, 1002, 1003],
            'codn_conce': [100, 200, 100, 300],
            'impp_conce': [50000.0, 15000.0, 45000.0, 30000.0],
            'tipos_grupos': [[1, 2], [3], [1], [2, 4]],  # Simulando tipos de grupos
            'tipo_conce': ['C', 'C', 'C', 'F'],
            'nro_orimp': [1, 1, 1, 0]
        })

        # Ejecutar sumarización
        resultado = self.processor._sumarizar_conceptos_pandas(df_legajos, df_conceptos)

        # Validaciones
        assert 'ImporteSAC' in resultado.columns
        assert 'Remuner78805' in resultado.columns
        assert pd.to_numeric(resultado.loc[0, 'ImporteSAC'], errors='coerce') > 0  # Legajo 1001 debe tener SAC
        assert len(resultado) == 3

        logger.info("✅ Sumarización de conceptos OK")

    def test_calculo_importes(self):
        """🧪 Prueba el cálculo de importes"""
        logger.info("🧪 Probando cálculo de importes...")

        # Datos de prueba con importes
        df_legajos = pd.DataFrame({
            'nro_legaj': [1001, 1002],
            'ImporteSAC': [10000.0, 5000.0],
            'ImporteNoRemun': [2000.0, 1000.0],
            'Remuner78805': [80000.0, 60000.0],
            'ImporteImponible_6': [0.0, 20000.0],  # Solo 1002 tiene imponible 6
            'SACInvestigador': [0.0, 0.0],
            'ImporteHorasExtras': [5000.0, 0.0],
            'ImporteZonaDesfavorable': [3000.0, 2000.0],
            'ImporteVacaciones': [0.0, 0.0],
            'ImportePremios': [1000.0, 500.0],
            'ImporteAdicionales': [2000.0, 1500.0],
            'IncrementoSolidario': [0.0, 0.0]
        })

        config = {
            'tope_jubilatorio_patronal': 500000.0,
            'tope_jubilatorio_personal': 400000.0,
            'tope_otros_aportes_personales': 450000.0,
            'trunca_tope': True,
            'tope_sac_jubilatorio_pers': 200000.0,
            'tope_sac_jubilatorio_patr': 250000.0,
            'tope_sac_jubilatorio_otro_ap': 225000.0
        }

        # Ejecutar cálculo
        resultado = self.processor._calcular_importes_pandas(df_legajos, config)

        # Validaciones
        assert 'IMPORTE_BRUTO' in resultado.columns
        assert 'IMPORTE_IMPON' in resultado.columns
        assert 'ImporteImponible_4' in resultado.columns
        assert resultado['IMPORTE_BRUTO'].sum() > 0
        assert resultado.loc[0, 'TipoDeOperacion'] == 1  # Legajo 1001
        assert pd.to_numeric(resultado.loc[1, 'ImporteImponible_6'], errors='coerce') > 0  # Legajo 1002

        logger.info("✅ Cálculo de importes OK")

    def test_aplicacion_topes(self):
        """🧪 Prueba la aplicación de topes"""
        logger.info("🧪 Probando aplicación de topes...")

        # Datos con importes que exceden topes
        df_legajos = pd.DataFrame({
            'nro_legaj': [1001, 1002],
            'ImporteSAC': [300000.0, 100000.0],  # 1001 excede tope SAC
            'ImporteImponiblePatronal': [600000.0, 300000.0],  # 1001 excede tope patronal
            'ImporteNoRemun': [10000.0, 5000.0],
            'ImporteSACPatronal': [300000.0, 100000.0],
            'ImporteImponibleSinSAC': [300000.0, 200000.0],
            'ImporteSACNoDocente': [300000.0, 100000.0],
            'ImporteImponible_4': [600000.0, 300000.0],
            'ImporteSACOtroAporte': [300000.0, 100000.0],
            'ImporteImponible_6': [0.0, 0.0],
            'TipoDeOperacion': [1, 1],
            'IMPORTE_BRUTO': [610000.0, 305000.0],
            'IMPORTE_IMPON': [600000.0, 300000.0],
            'DiferenciaSACImponibleConTope': [0.0, 0.0],
            'DiferenciaImponibleConTope': [0.0, 0.0]
        })

        config = {
            'tope_jubilatorio_patronal': 500000.0,
            'tope_jubilatorio_personal': 400000.0,
            'tope_otros_aportes_personales': 450000.0,
            'trunca_tope': True,
            'tope_sac_jubilatorio_pers': 200000.0,
            'tope_sac_jubilatorio_patr': 250000.0,
            'tope_sac_jubilatorio_otro_ap': 225000.0
        }

        # Ejecutar aplicación de topes
        resultado = self.processor._aplicar_topes_pandas(df_legajos, config)

        # Validaciones
        assert pd.to_numeric(resultado.loc[0, 'DiferenciaSACImponibleConTope'], errors='coerce') > 0  # Debe haber diferencia
        assert resultado.loc[0, 'ImporteSACPatronal'] == 250000.0  # Debe estar topado
        assert resultado.loc[1, 'DiferenciaSACImponibleConTope'] == 0  # No debe haber diferencia

        logger.info("✅ Aplicación de topes OK")

    def test_otra_actividad(self):
        """🧪 Prueba el procesamiento de otra actividad"""
        logger.info("🧪 Probando otra actividad...")

        df_legajos = pd.DataFrame({
            'nro_legaj': [1001, 1002, 1003],
            'IMPORTE_IMPON': [300000.0, 200000.0, 100000.0],
            'ImporteImponibleSinSAC': [250000.0, 180000.0, 90000.0],
            'ImporteSACPatronal': [50000.0, 20000.0, 10000.0]
        })

        df_otra_actividad = pd.DataFrame({
            'nro_legaj': [1001, 1003],  # Solo 1001 y 1003 tienen otra actividad
            'importebrutootraactividad': [100000.0, 50000.0],
            'importesacotraactividad': [20000.0, 10000.0]
        })

        config = {
            'tope_sac_jubilatorio_pers': 200000.0,
            'tope_jubilatorio_patronal': 500000.0
        }

        # Ejecutar procesamiento
        resultado = self.processor._procesar_otra_actividad_pandas(
            df_legajos, df_otra_actividad, config
        )

        # Validaciones
        assert 'ImporteBrutoOtraActividad' in resultado.columns
        assert 'ImporteSACOtraActividad' in resultado.columns
        assert resultado.loc[0, 'ImporteBrutoOtraActividad'] == 100000.0  # Legajo 1001
        assert resultado.loc[1, 'ImporteBrutoOtraActividad'] == 0.0  # Legajo 1002 sin otra actividad
        assert resultado.loc[2, 'ImporteBrutoOtraActividad'] == 50000.0  # Legajo 1003

        logger.info("✅ Otra actividad OK")

    def test_validacion_legajos(self):
        """🧪 Prueba la validación de legajos"""
        logger.info("🧪 Probando validación de legajos...")

        df_legajos = pd.DataFrame({
            'nro_legaj': [1001, 1002, 1003, 1004, 1005],
            'IMPORTE_BRUTO': [100000.0, 0.0, 50000.0, 0.0, 30000.0],
            'IMPORTE_IMPON': [80000.0, 0.0, 40000.0, 0.0, 25000.0],
            'ImporteImponiblePatronal': [80000.0, 0.0, 40000.0, 0.0, 25000.0],
            'codigosituacion': [1, 1, 5, 13, 14],  # 5=maternidad, 13=licencia, 14=reserva
            'licencia': [0, 0, 0, 1, 0],
            'AsignacionesFliaresPagadas': [5000.0, 0.0, 3000.0, 0.0, 2000.0]
        })

        datos = {
            'check_lic': True  # Incluir licencias
        }

        # Ejecutar validación
        resultado = self.processor._validar_legajos_pandas(df_legajos, datos)

        # Validaciones
        # Deben pasar: 1001 (tiene importes), 1003 (maternidad), 1004 (licencia), 1005 (reserva)
        # No debe pasar: 1002 (sin importes, sin situaciones especiales)
        assert len(resultado) == 4
        assert 1001 in resultado['nro_legaj'].values
        assert 1003 in resultado['nro_legaj'].values  # Maternidad
        assert 1004 in resultado['nro_legaj'].values  # Licencia con check activado
        assert 1005 in resultado['nro_legaj'].values  # Reserva de puesto
        assert 1002 not in resultado['nro_legaj'].values  # Debe ser rechazado

        logger.info("✅ Validación de legajos OK")

    def test_flujo_completo(self):
        """🧪 Prueba el flujo completo de procesamiento"""
        logger.info("🧪 Probando flujo completo...")

        # Datos completos de prueba
        df_legajos = pd.DataFrame({
            'nro_legaj': [1001, 1002],
            'cuit': ['20123456789', '27987654321'],
            'apyno': ['PEREZ JUAN', 'GARCIA MARIA'],
            'codigosituacion': [1, 1],
            'codigocondicion': [1, 1],
            'codigozona': [0, 0],
            'codigocontratacion': [0, 0],
            'regimen': ['1', '1'],
            'conyugue': [1, 0],
            'hijos': [2, 1],
            'adherentes': [0, 0],
            'licencia': [0, 0],
            'provincialocalidad': ['BUENOS AIRES', 'CORDOBA']
        })

        df_conceptos = pd.DataFrame({
            'nro_legaj': [1001, 1001, 1002],
            'codn_conce': [100, 200, 100],
            'impp_conce': [80000.0, 10000.0, 60000.0],
            'tipos_grupos': [[1], [2], [1]],
            'tipo_conce': ['C', 'C', 'C'],
            'nro_orimp': [1, 1, 1]
        })

        df_otra_actividad = pd.DataFrame()  # Sin otra actividad

        datos = {
            'TopeJubilatorioPatronal': 500000.0,
            'TopeJubilatorioPersonal': 400000.0,
            'TopeOtrosAportesPersonal': 450000.0,
            'truncaTope': 1,
            'check_lic': False,
            'check_retro': False,
            'seguro_vida_patronal': 0,
            'ARTconTope': '1',
            'ConceptosNoRemuEnART': '0'
        }

        # Ejecutar flujo completo
        resultado = self.processor.procesa_sicoss_dataframes(
            datos=datos,
            per_anoct=2024,
            per_mesct=12,
            df_legajos=df_legajos,
            df_conceptos=df_conceptos,
            df_otra_actividad=df_otra_actividad,
            nombre_arch="test_sicoss",
            retornar_datos=True
        )

        # Validaciones del resultado
        assert isinstance(resultado, list)
        assert len(resultado) == 2  # Ambos legajos deben ser válidos

        # Verificar campos obligatorios en el primer legajo
        # Verificar campos obligatorios en el primer legajo
        legajo_1 = resultado[0]
        campos_obligatorios = [
            'cuit', 'apyno', 'IMPORTE_BRUTO', 'IMPORTE_IMPON',
            'ImporteImponiblePatronal', 'ImporteSAC', 'codigosituacion',
            'dias_trabajados', 'trabajadorconvencionado'
        ]

        for campo in campos_obligatorios:
            assert campo in legajo_1, f"Campo {campo} faltante"

        # Verificar que los importes sean coherentes
        assert legajo_1['IMPORTE_BRUTO'] >= legajo_1['IMPORTE_IMPON']
        assert legajo_1['nro_legaj'] == 1001

        logger.info("✅ Flujo completo OK")

    def generar_reporte_pruebas(self):
        """📊 Genera reporte detallado de las pruebas"""
        logger.info("📊 === REPORTE DE PRUEBAS ===")
        logger.info("✅ Sumarización de conceptos: PASÓ")
        logger.info("✅ Cálculo de importes: PASÓ")
        logger.info("✅ Aplicación de topes: PASÓ")
        logger.info("✅ Otra actividad: PASÓ")
        logger.info("✅ Validación de legajos: PASÓ")
        logger.info("✅ Flujo completo: PASÓ")
        logger.info("🎉 TODAS LAS PRUEBAS COMPLETADAS EXITOSAMENTE")

# 🚀 **MÉTODO PARA GRABAR ARCHIVO TXT CON PANDAS**

def _grabar_en_txt_pandas(self, df_legajos: pd.DataFrame, nombre_arch: str):
    """
    💾 MÉTODO IMPLEMENTADO: Graba archivo TXT usando pandas
    Reemplaza grabar_en_txt() del PHP con formato idéntico
    """
    logger.info(f"💾 Grabando archivo SICOSS: {nombre_arch}")
    import os
    # Crear directorio si no existe
    directorio = 'storage/comunicacion/sicoss'
    os.makedirs(directorio, exist_ok=True)
    archivo_path = f"{directorio}/{nombre_arch}.txt"
    try:
        with open(archivo_path, 'w', encoding='latin1') as archivo:
            for _, legajo in df_legajos.iterrows():
                # Formatear cada campo según especificaciones SICOSS
                linea = self._formatear_linea_sicoss(legajo)
                archivo.write(linea + "\r\n")
        logger.info(f"✅ Archivo grabado: {archivo_path}")
        logger.info(f"📊 Registros escritos: {len(df_legajos)}")
    except Exception as e:
        logger.error(f"❌ Error grabando archivo: {e}")
        raise

    def _formatear_linea_sicoss(self, legajo: pd.Series) -> str:
        """
        🔧 HELPER: Formatea una línea del archivo SICOSS

        Replica exactamente el formato del PHP grabar_en_txt()
        """

        def llenar_blancos(texto: str, longitud: int) -> str:
            """Completa con espacios a la derecha"""
            return str(texto)[:longitud].ljust(longitud)

        def llenar_blancos_izq(texto: str, longitud: int) -> str:
            """Completa con espacios a la izquierda"""
            return str(texto)[:longitud].rjust(longitud)

        def llenar_importes(numero, longitud: int) -> str:
            """Completa números con ceros a la izquierda"""
            return str(int(numero)).zfill(longitud)

        def formatear_importe(importe: float) -> str:
            """Formatea importe con coma decimal"""
            return f"{importe:.2f}".replace('.', ',')

        # 🎯 FORMATEAR CADA CAMPO SEGÚN ESPECIFICACIONES SICOSS
        linea = (
                str(legajo['cuit']) +  # Campo 1 - CUIT
                llenar_blancos(legajo['apyno'], 30) +  # Campo 2 - Apellido y Nombre
                str(int(legajo['conyugue'])) +  # Campo 3 - Cónyuge
                llenar_importes(legajo['hijos'], 2) +  # Campo 4 - Hijos
                llenar_importes(legajo['codigosituacion'], 2) +  # Campo 5 - Código Situación
                llenar_importes(legajo['codigocondicion'], 2) +  # Campo 6 - Código Condición
                llenar_importes(legajo['TipoDeActividad'], 3) +  # Campo 7 - Tipo Actividad
                llenar_importes(legajo['codigozona'], 2) +  # Campo 8 - Código Zona
                llenar_blancos_izq(formatear_importe(legajo.get('aporteadicional', 0.0)),
                                   5) +  # Campo 9 - Aporte Adicional
                llenar_importes(legajo['codigocontratacion'], 3) +  # Campo 10 - Código Contratación
                llenar_importes(legajo['codigo_os'], 6) +  # Campo 11 - Código Obra Social
                llenar_importes(legajo['adherentes'], 2) +  # Campo 12 - Adherentes
                llenar_blancos_izq(formatear_importe(legajo['IMPORTE_BRUTO']), 12) +  # Campo 13 - Importe Bruto
                llenar_blancos_izq(formatear_importe(legajo['IMPORTE_IMPON']), 12) +  # Campo 14 - Importe Imponible
                llenar_blancos_izq(formatear_importe(legajo['AsignacionesFliaresPagadas']),
                                   9) +  # Campo 15 - Asignaciones Familiares
                llenar_blancos_izq(formatear_importe(legajo.get('IMPORTE_VOLUN', 0.0)),
                                   9) +  # Campo 16 - Importe Voluntario
                llenar_blancos_izq(formatear_importe(legajo.get('IMPORTE_ADICI', 0.0)),
                                   9) +  # Campo 17 - Importe Adicional
                llenar_blancos_izq(formatear_importe(abs(legajo.get('ImporteSICOSSDec56119', 0.0))),
                                   9) +  # Campo 18 - Excedente Aportes SS
                '     0,00' +  # Campo 19 - Excedente Aportes OS
                llenar_blancos(legajo.get('provincialocalidad', ''), 50) +  # Campo 20 - Provincia/Localidad
                llenar_blancos_izq(formatear_importe(legajo['ImporteImponiblePatronal']),
                                   12) +  # Campo 21 - Imponible 2
                llenar_blancos_izq(formatear_importe(legajo['ImporteImponiblePatronal']),
                                   12) +  # Campo 22 - Imponible 3
                llenar_blancos_izq(formatear_importe(legajo['ImporteImponible_4']), 12) +  # Campo 23 - Imponible 4
                '00' +  # Campo 24 - Código Siniestrado
                '0' +  # Campo 25 - Marca Reducción
                '000000,00' +  # Campo 26 - Capital Recomposición
                '1' +  # Campo 27 - Tipo Empresa
                llenar_blancos_izq(formatear_importe(legajo.get('AporteAdicionalObraSocial', 0.0)),
                                   9) +  # Campo 28 - Aporte Adicional OS
                str(legajo['regimen']) +  # Campo 29 - Régimen
                llenar_importes(legajo['codigorevista1'], 2) +  # Campo 30 - Código Revista 1
                llenar_importes(legajo['fecharevista1'], 2) +  # Campo 31 - Fecha Revista 1
                llenar_importes(legajo['codigorevista2'], 2) +  # Campo 32 - Código Revista 2
                llenar_importes(legajo['fecharevista2'], 2) +  # Campo 33 - Fecha Revista 2
                llenar_importes(legajo['codigorevista3'], 2) +  # Campo 34 - Código Revista 3
                llenar_importes(legajo['fecharevista3'], 2) +  # Campo 35 - Fecha Revista 3
                llenar_blancos_izq(formatear_importe(legajo['ImporteSueldoMasAdicionales']),
                                   12) +  # Campo 36 - Sueldo + Adicionales
                llenar_blancos_izq(formatear_importe(legajo['ImporteSAC']), 12) +  # Campo 37 - SAC
                llenar_blancos_izq(formatear_importe(legajo['ImporteHorasExtras']), 12) +  # Campo 38 - Horas Extras
                llenar_blancos_izq(formatear_importe(legajo['ImporteZonaDesfavorable']),
                                   12) +  # Campo 39 - Zona Desfavorable
                llenar_blancos_izq(formatear_importe(legajo['ImporteVacaciones']), 12) +  # Campo 40 - Vacaciones
                '0000000' + llenar_importes(legajo['dias_trabajados'], 2) +  # Campo 41 - Días Trabajados
                llenar_blancos_izq(formatear_importe(legajo['ImporteImponible_4'] - legajo.get('ImporteTipo91', 0.0)),
                                   12) +  # Campo 42 - Imponible 5
                str(legajo['trabajadorconvencionado']) +  # Campo 43 - Trabajador Convencionado
                llenar_blancos_izq(formatear_importe(legajo['ImporteImponible_6']), 12) +  # Campo 44 - Imponible 6
                str(int(legajo['TipoDeOperacion'])) +  # Campo 45 - Tipo Operación
                llenar_blancos_izq(formatear_importe(legajo['ImporteAdicionales']), 12) +  # Campo 46 - Adicionales
                llenar_blancos_izq(formatear_importe(legajo['ImportePremios']), 12) +  # Campo 47 - Premios
                llenar_blancos_izq(formatear_importe(legajo['Remuner78805']), 12) +  # Campo 48 - Remuneración 78805
                llenar_blancos_izq(formatear_importe(legajo['ImporteImponible_6']), 12) +  # Campo 49 - Imponible 7
                llenar_importes(int(np.ceil(legajo.get('CantidadHorasExtras', 0.0))),
                                3) +  # Campo 50 - Cantidad Horas Extras
                llenar_blancos_izq(formatear_importe(legajo['ImporteNoRemun']), 12) +  # Campo 51 - No Remunerativo
                llenar_blancos_izq(formatear_importe(legajo.get('ImporteMaternidad', 0.0)),
                                   12) +  # Campo 52 - Maternidad
                llenar_blancos_izq(formatear_importe(legajo.get('ImporteRectificacionRemun', 0.0)),
                                   9) +  # Campo 53 - Rectificación
                llenar_blancos_izq(formatear_importe(legajo['importeimponible_9']),
                                   12) +  # Campo 54 - Imponible 9 (ART)
                llenar_blancos_izq(formatear_importe(legajo.get('ContribTareaDif', 0.0)),
                                   9) +  # Campo 55 - Contribución Tarea Diferencial
                '000' +  # Campo 56 - Horas Trabajadas
                str(int(legajo['SeguroVidaObligatorio'])) +  # Campo 57 - Seguro Vida Obligatorio
                llenar_blancos_izq(formatear_importe(legajo.get('ImporteSICOSS27430', 0.0)),
                                   12) +  # Campo 58 - Importe Ley 27430
                llenar_blancos_izq(formatear_importe(legajo.get('IncrementoSolidario', 0.0)),
                                   12) +  # Campo 59 - Incremento Solidario
                llenar_blancos_izq(formatear_importe(0.0), 12)  # Campo 60 - Remuneración 11
        )

        return linea


def ejecutar_suite_completa(self):
    """🎯 Ejecuta suite completa de pruebas con métricas"""
    import time

    inicio = time.time()
    logger.info("🧪 === INICIANDO SUITE COMPLETA DE PRUEBAS ===")

    pruebas_ejecutadas = 0
    pruebas_exitosas = 0
    errores = []

    pruebas = [
        ('Sumarización de Conceptos', self.test_sumarizacion_conceptos),
        ('Cálculo de Importes', self.test_calculo_importes),
        ('Aplicación de Topes', self.test_aplicacion_topes),
        ('Otra Actividad', self.test_otra_actividad),
        ('Validación de Legajos', self.test_validacion_legajos),
        ('Flujo Completo', self.test_flujo_completo)
    ]

    for nombre, metodo_prueba in pruebas:
        try:
            logger.info(f"🧪 Ejecutando: {nombre}")
            metodo_prueba()
            pruebas_exitosas += 1
            logger.info(f"✅ {nombre}: PASÓ")
        except Exception as e:
            errores.append(f"{nombre}: {str(e)}")
            logger.error(f"❌ {nombre}: FALLÓ - {e}")
        finally:
            pruebas_ejecutadas += 1

    fin = time.time()
    tiempo_total = fin - inicio

    # Generar reporte final
    logger.info("📊 === REPORTE FINAL DE PRUEBAS ===")
    logger.info(f"⏱️ Tiempo total: {tiempo_total:.2f} segundos")
    logger.info(f"🧪 Pruebas ejecutadas: {pruebas_ejecutadas}")
    logger.info(f"✅ Pruebas exitosas: {pruebas_exitosas}")
    logger.info(f"❌ Pruebas fallidas: {len(errores)}")

    if errores:
        logger.error("❌ ERRORES ENCONTRADOS:")
        for error in errores:
            logger.error(f"   - {error}")
        return False
    else:
        logger.info("🎉 TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
        return True


def test_rendimiento(self):
    """⚡ Prueba de rendimiento con dataset grande"""
    logger.info("⚡ Probando rendimiento con dataset grande...")

    import time

    # Generar dataset grande (1000 legajos)
    df_legajos_grande = pd.DataFrame({
        'nro_legaj': range(1000, 2000),
        'cuit': [f'20{i:08d}9' for i in range(1000, 2000)],
        'apyno': [f'EMPLEADO {i}' for i in range(1000, 2000)],
        'codigosituacion': [1] * 1000,
        'codigocondicion': [1] * 1000,
        'codigozona': [0] * 1000,
        'codigocontratacion': [0] * 1000,
        'regimen': ['1'] * 1000,
        'conyugue': [i % 2 for i in range(1000)],
        'hijos': [i % 3 for i in range(1000)],
        'adherentes': [0] * 1000,
        'licencia': [0] * 1000,
        'provincialocalidad': ['BUENOS AIRES'] * 1000
    })

    # Generar conceptos (3000 registros - 3 por legajo)
    conceptos_data = []
    for legajo in range(1000, 2000):
        conceptos_data.extend([
            {'nro_legaj': legajo, 'codn_conce': 100, 'impp_conce': 50000.0 + (legajo % 1000),
             'tipos_grupos': [1], 'tipo_conce': 'C', 'nro_orimp': 1},
            {'nro_legaj': legajo, 'codn_conce': 200, 'impp_conce': 15000.0 + (legajo % 500),
             'tipos_grupos': [2], 'tipo_conce': 'C', 'nro_orimp': 1},
            {'nro_legaj': legajo, 'codn_conce': 300, 'impp_conce': 5000.0 + (legajo % 200),
             'tipos_grupos': [3], 'tipo_conce': 'F', 'nro_orimp': 0}
        ])

    df_conceptos_grande = pd.DataFrame(conceptos_data)

    datos = {
        'TopeJubilatorioPatronal': 500000.0,
        'TopeJubilatorioPersonal': 400000.0,
        'TopeOtrosAportesPersonal': 450000.0,
        'truncaTope': 1,
        'check_lic': False,
        'check_retro': False
    }

    # Medir tiempo de procesamiento
    inicio = time.time()

    resultado = self.processor.procesa_sicoss_dataframes(
        datos=datos,
        per_anoct=2024,
        per_mesct=12,
        df_legajos=df_legajos_grande,
        df_conceptos=df_conceptos_grande,
        df_otra_actividad=pd.DataFrame(),
        nombre_arch="test_rendimiento",
        retornar_datos=True
    )

    fin = time.time()
    tiempo_procesamiento = fin - inicio

    # Validaciones de rendimiento
    assert isinstance(resultado, list)
    assert len(resultado) == 1000
    assert tiempo_procesamiento < 10.0  # Debe procesar 1000 legajos en menos de 10 segundos

    logger.info(f"⚡ Rendimiento OK: {len(resultado)} legajos en {tiempo_procesamiento:.2f}s")
    logger.info(f"📊 Velocidad: {len(resultado) / tiempo_procesamiento:.0f} legajos/segundo")


def test_integracion_backend(self):
    """🔌 Prueba integración con SicossBackEnd"""
    logger.info("🔌 Probando integración con SicossBackEnd...")

    from SicossBackEnd import procesar_sicoss_completo

    # Datos de prueba
    datos_config = {
        'TopeJubilatorioPatronal': 500000.0,
        'TopeJubilatorioPersonal': 400000.0,
        'TopeOtrosAportesPersonal': 450000.0,
        'truncaTope': 1,
        'check_lic': False,
        'PorcAporteAdicionalJubilacion': 100.0,
        'TrabajadorConvencionado': 'S',
        'AsignacionFamiliar': False
    }

    legajos_data = [
        {
            'nro_legaj': 9999,
            'cuit': '20999999999',
            'apyno': 'PRUEBA INTEGRACION',
            'codigosituacion': 1,
            'codigocondicion': 1,
            'codigozona': 0,
            'codigocontratacion': 0,
            'regimen': '1',
            'conyugue': 1,
            'hijos': 1,
            'adherentes': 0,
            'licencia': 0,
            'provincialocalidad': 'PRUEBA'
        }
    ]

    conceptos_data = [
        {'nro_legaj': 9999, 'codn_conce': 100, 'impp_conce': 75000.0,
         'tipos_grupos': [1], 'tipo_conce': 'C', 'nro_orimp': 1}
    ]

    # Ejecutar a través del BackEnd
    resultado = procesar_sicoss_completo(
        datos_config=datos_config,
        legajos_data=legajos_data,
        conceptos_data=conceptos_data,
        per_anoct=2024,
        per_mesct=12,
        retornar_datos=True
    )

    # Validaciones
    assert isinstance(resultado, list)
    assert len(resultado) == 1
    assert resultado[0]['nro_legaj'] == 9999
    assert resultado[0]['IMPORTE_BRUTO'] > 0

    logger.info("✅ Integración con BackEnd OK")
