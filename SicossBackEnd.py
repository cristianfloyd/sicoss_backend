import pandas as pd
from SicossProcessor import SicossProcessor
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# 🎯 **FUNCIÓN PRINCIPAL PARA USO EXTERNO**
def procesar_sicoss_completo(datos_config: Dict, legajos_data: List[Dict],
                             conceptos_data: List[Dict], otra_actividad_data: List[Dict] = None,
                             **kwargs) -> Dict:
    """
    🚀 FUNCIÓN PRINCIPAL PÚBLICA - Interfaz simplificada para usar desde otros módulos
    *** VERSIÓN ACTUALIZADA CON PANDAS ***
    """
    logger.info("🚀 === INICIANDO PROCESAMIENTO SICOSS COMPLETO ===")

    # Convertir datos a DataFrames
    df_legajos = pd.DataFrame(legajos_data)
    df_conceptos = pd.DataFrame(conceptos_data)
    df_otra_actividad = pd.DataFrame(otra_actividad_data or [])

    # Crear procesador
    processor = SicossProcessor()

    # Configurar parámetros del procesador
    processor.porc_aporte_adicional_jubilacion = datos_config.get('PorcAporteAdicionalJubilacion', 100.0)
    processor.categoria_diferencial = datos_config.get('CategoriaDiferencial', '')
    processor.trabajador_convencionado = datos_config.get('TrabajadorConvencionado', 'S')
    processor.asignacion_familiar = datos_config.get('AsignacionFamiliar', False)

    # Parámetros por defecto
    parametros = {
        'per_anoct': kwargs.get('per_anoct', datetime.now().year),
        'per_mesct': kwargs.get('per_mesct', datetime.now().month),
        'nombre_arch': kwargs.get('nombre_arch', f'sicoss_{datetime.now().strftime("%Y_%m")}'),
        'licencias': kwargs.get('licencias'),
        'retro': kwargs.get('retro', False),
        'check_sin_activo': kwargs.get('check_sin_activo', False),
        'retornar_datos': kwargs.get('retornar_datos', False)
    }

    # 🚀 EJECUTAR PROCESAMIENTO CON PANDAS (CAMBIO PRINCIPAL)
    resultado = processor.procesa_sicoss_dataframes(
        datos=datos_config,
        df_legajos=df_legajos,
        df_conceptos=df_conceptos,
        df_otra_actividad=df_otra_actividad,
        **parametros
    )

    logger.info("✅ === PROCESAMIENTO SICOSS COMPLETO FINALIZADO ===")
    return resultado


def procesar_sicoss_desde_bd(config_bd: Dict, datos_config: Dict,
                             per_anoct: int, per_mesct: int,
                             nro_legajo: Optional[int] = None, **kwargs) -> Dict:
    """
    🗄️ NUEVA FUNCIÓN - Procesa SICOSS extrayendo datos directamente de BD

    Esta función integra SicossDataExtractor + SicossProcessor

    Args:
        config_bd: Configuración de conexión a BD
        datos_config: Configuración de topes y parámetros SICOSS
        per_anoct: Año del período
        per_mesct: Mes del período
        nro_legajo: Legajo específico (opcional)
        **kwargs: Parámetros adicionales

    Returns:
        Dict con resultados del procesamiento
    """
    logger.info("🗄️ === PROCESAMIENTO SICOSS DESDE BD ===")

    try:
        # 1. Inicializar extractor de datos
        from SicossDataExtractor import DatabaseConnection, SicossDataExtractor, SicossConfig

        # Crear configuración
        config = SicossConfig(
            tope_jubilatorio_patronal=datos_config['TopeJubilatorioPatronal'],
            tope_jubilatorio_personal=datos_config['TopeJubilatorioPersonal'],
            tope_otros_aportes_personales=datos_config['TopeOtrosAportesPersonal'],
            trunca_tope=bool(datos_config.get('truncaTope', 1)),
            check_lic=datos_config.get('check_lic', False),
            check_retro=datos_config.get('check_retro', False),
            check_sin_activo=datos_config.get('check_sin_activo', False),
            asignacion_familiar=datos_config.get('AsignacionFamiliar', False),
            trabajador_convencionado=datos_config.get('TrabajadorConvencionado', 'S')
        )

        # 2. Extraer datos de la BD
        logger.info("📊 Extrayendo datos de la base de datos...")
        db = DatabaseConnection(config_bd.get('config_file', 'database.ini'))
        extractor = SicossDataExtractor(db)

        datos_extraidos = extractor.extraer_datos_completos(
            config=config,
            per_anoct=per_anoct,
            per_mesct=per_mesct,
            nro_legajo=nro_legajo
        )

        # 3. Procesar con pandas
        logger.info("⚙️ Procesando datos con pandas...")
        resultado = procesar_sicoss_completo(
            datos_config=datos_config,
            legajos_data=datos_extraidos['legajos'].to_dict('records'),
            conceptos_data=datos_extraidos['conceptos'].to_dict('records'),
            otra_actividad_data=datos_extraidos['otra_actividad'].to_dict('records'),
            per_anoct=per_anoct,
            per_mesct=per_mesct,
            **kwargs
        )

        logger.info("✅ === PROCESAMIENTO DESDE BD COMPLETADO ===")
        return resultado

    except Exception as e:
        logger.error(f"❌ Error en procesamiento desde BD: {e}")
        raise


def procesar_sicoss_modo_hibrido(datos_config: Dict, per_anoct: int, per_mesct: int,
                                 usar_extractor: bool = True, **kwargs) -> Dict:
    """
    🔄 FUNCIÓN HÍBRIDA - Permite elegir entre extractor BD o datos manuales

    Args:
        datos_config: Configuración SICOSS
        per_anoct: Año
        per_mesct: Mes
        usar_extractor: True = usar BD, False = usar datos manuales
        **kwargs: Parámetros adicionales

    Returns:
        Dict con resultados
    """
    logger.info(f"🔄 === MODO HÍBRIDO - Extractor: {usar_extractor} ===")

    if usar_extractor:
        # Usar extractor de BD
        config_bd = kwargs.get('config_bd', {})
        return procesar_sicoss_desde_bd(
            config_bd=config_bd,
            datos_config=datos_config,
            per_anoct=per_anoct,
            per_mesct=per_mesct,
            **kwargs
        )
    else:
        # Usar datos manuales (modo actual)
        legajos_data = kwargs.get('legajos_data', [])
        conceptos_data = kwargs.get('conceptos_data', [])
        otra_actividad_data = kwargs.get('otra_actividad_data', [])

        return procesar_sicoss_completo(
            datos_config=datos_config,
            legajos_data=legajos_data,
            conceptos_data=conceptos_data,
            otra_actividad_data=otra_actividad_data,
            per_anoct=per_anoct,
            per_mesct=per_mesct,
            **kwargs
        )


# 🧪 **EJEMPLO DE USO COMPLETO**

def ejemplo_uso_completo():
    """
    🧪 EJEMPLO COMPLETO - Demuestra cómo usar toda la implementación
    """
    logger.info("🧪 === EJECUTANDO EJEMPLO COMPLETO ===")

    # 📊 DATOS DE CONFIGURACIÓN
    datos_config = {
        'TopeJubilatorioPatronal': 800000.0,
        'TopeJubilatorioPersonal': 600000.0,
        'TopeOtrosAportesPersonal': 700000.0,
        'truncaTope': 1,
        'check_lic': False,
        'check_retro': False,
        'seguro_vida_patronal': 0,
        'ARTconTope': '1',
        'ConceptosNoRemuEnART': '0',
        'PorcAporteAdicionalJubilacion': 100.0,
        'TrabajadorConvencionado': 'S',
        'AsignacionFamiliar': False
    }

    # 📊 DATOS DE LEGAJOS (simulados)
    legajos_data = [
        {
            'nro_legaj': 12345,
            'cuit': '20123456789',
            'apyno': 'PEREZ JUAN CARLOS',
            'codigosituacion': 1,
            'codigocondicion': 1,
            'codigozona': 0,
            'TipoDeActividad': 0,
            'codigocontratacion': 0,
            'regimen': '1',
            'conyugue': 1,
            'hijos': 2,
            'adherentes': 0,
            'licencia': 0,
            'provincialocalidad': 'BUENOS AIRES'
        },
        {
            'nro_legaj': 67890,
            'cuit': '27987654321',
            'apyno': 'GARCIA MARIA ELENA',
            'codigosituacion': 5,  # Maternidad
            'codigocondicion': 1,
            'codigozona': 0,
            'TipoDeActividad': 0,
            'codigocontratacion': 0,
            'regimen': '1',
            'conyugue': 0,
            'hijos': 1,
            'adherentes': 0,
            'licencia': 0,
            'provincialocalidad': 'CORDOBA'
        },
        {
            'nro_legaj': 11111,
            'cuit': '23111111119',
            'apyno': 'RODRIGUEZ CARLOS ALBERTO',
            'codigosituacion': 1,
            'codigocondicion': 1,
            'codigozona': 0,
            'TipoDeActividad': 0,
            'codigocontratacion': 0,
            'regimen': '1',
            'conyugue': 1,
            'hijos': 0,
            'adherentes': 0,
            'licencia': 0,
            'provincialocalidad': 'MENDOZA'
        }
    ]

    # 📊 DATOS DE CONCEPTOS LIQUIDADOS (simulados)
    conceptos_data = [
        # Legajo 12345
        {'nro_legaj': 12345, 'codn_conce': 100, 'impp_conce': 150000.0, 'tipos_grupos': [1], 'tipo_conce': 'C',
         'nro_orimp': 1},
        {'nro_legaj': 12345, 'codn_conce': 200, 'impp_conce': 25000.0, 'tipos_grupos': [2], 'tipo_conce': 'C',
         'nro_orimp': 1},
        {'nro_legaj': 12345, 'codn_conce': 300, 'impp_conce': 8000.0, 'tipos_grupos': [3], 'tipo_conce': 'F',
         'nro_orimp': 0},

        # Legajo 67890 (maternidad)
        {'nro_legaj': 67890, 'codn_conce': 100, 'impp_conce': 120000.0, 'tipos_grupos': [1], 'tipo_conce': 'C',
         'nro_orimp': 1},
        {'nro_legaj': 67890, 'codn_conce': 500, 'impp_conce': 15000.0, 'tipos_grupos': [5], 'tipo_conce': 'C',
         'nro_orimp': 1},

        # Legajo 11111
        {'nro_legaj': 11111, 'codn_conce': 100, 'impp_conce': 200000.0, 'tipos_grupos': [1], 'tipo_conce': 'C',
         'nro_orimp': 1},
        {'nro_legaj': 11111, 'codn_conce': 400, 'impp_conce': 30000.0, 'tipos_grupos': [4], 'tipo_conce': 'C',
         'nro_orimp': 1},
        {'nro_legaj': 11111, 'codn_conce': 600, 'impp_conce': 12000.0, 'tipos_grupos': [6], 'tipo_conce': 'C',
         'nro_orimp': 1}
    ]

    # 📊 DATOS DE OTRA ACTIVIDAD (simulados)
    otra_actividad_data = [
        {'nro_legaj': 11111, 'importebrutootraactividad': 50000.0, 'importesacotraactividad': 8000.0}
        # Solo el legajo 11111 tiene otra actividad
    ]

    try:
        # 🚀 EJECUTAR PROCESAMIENTO COMPLETO
        resultado = procesar_sicoss_completo(
            datos_config=datos_config,
            legajos_data=legajos_data,
            conceptos_data=conceptos_data,
            otra_actividad_data=otra_actividad_data,
            per_anoct=2024,
            per_mesct=12,
            nombre_arch="ejemplo_sicoss_2024_12",
            retornar_datos=True
        )

        # 📊 MOSTRAR RESULTADOS
        if isinstance(resultado, list):
            logger.info("🎉 === RESULTADOS DEL PROCESAMIENTO ===")
            logger.info(f"📊 Legajos procesados: {len(resultado)}")

            for i, legajo in enumerate(resultado):
                logger.info(f"👤 Legajo {i + 1}: {legajo['nro_legaj']}")
                logger.info(f"   - Nombre: {legajo['apyno']}")
                logger.info(f"   - Situación: {legajo['codigosituacion']}")
                logger.info(f"   - Bruto: ${legajo['IMPORTE_BRUTO']:,.2f}")
                logger.info(f"   - Imponible: ${legajo['IMPORTE_IMPON']:,.2f}")
                logger.info(f"   - SAC: ${legajo['ImporteSAC']:,.2f}")
                logger.info(f"   - ART: ${legajo['importeimponible_9']:,.2f}")
        else:
            logger.info("📊 === TOTALES DEL PROCESAMIENTO ===")
            for concepto, valor in resultado.items():
                logger.info(f"   {concepto}: ${valor:,.2f}")

        logger.info("✅ === EJEMPLO COMPLETADO EXITOSAMENTE ===")
        return resultado

    except Exception as e:
        logger.error(f"❌ Error en ejemplo: {e}")
        raise
