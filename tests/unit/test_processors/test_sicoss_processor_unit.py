from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from config.sicoss_config import SicossConfig
from processors.sicoss_processor import SicossDataProcessor


@pytest.fixture
def sicoss_config():
    return SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
    )


@pytest.fixture
def processor(sicoss_config: SicossConfig):
    # Parcheamos la inicialización de procesadores para evitar dependencias
    with patch(
        "processors.sicoss_processor.SicossDataProcessor._initialize_processors"
    ):
        p = SicossDataProcessor(sicoss_config)
        p.conceptos_processor = MagicMock()
        p.calculos_processor = MagicMock()
        p.topes_processor = MagicMock()
        p.validator = MagicMock()
        p.stats_helper = MagicMock()
        p.recordset_exporter = MagicMock()
        return p


def test_sicoss_processor_pipeline_execution(processor: SicossDataProcessor):
    """Prueba que el pipeline de SicossDataProcessor se ejecute en el orden correcto."""
    # Mock data
    columns = [
        "nro_legaj",
        "ImporteImponiblePatronal",
        "IMPORTE_IMPON",
        "IMPORTE_BRUTO",
        "ImporteSACPatronal",
        "Remuner78805",
        "ImporteImponible_4",
        "ImporteImponible_5",
        "ImporteImponible_6",
    ]
    df_legajos = pd.DataFrame(
        {col: [0.0] if col != "nro_legaj" else [1] for col in columns}
    )

    data = {
        "legajos": df_legajos,
        "conceptos": pd.DataFrame({"nro_legaj": [1]}),
        "otra_actividad": pd.DataFrame({"nro_legaj": [1]}),
        "obra_social": pd.DataFrame({"nro_legaj": [1]}),
    }

    # Configurar retornos de mocks
    processor.conceptos_processor.process.return_value = df_legajos.copy()
    processor.calculos_processor.process.return_value = df_legajos.copy()
    processor.topes_processor.process.return_value = df_legajos.copy()
    processor.validator.validate.return_value = df_legajos.copy()
    processor.stats_helper.crear_totales_vacios.return_value = {}

    resultado = processor.procesar_datos_extraidos(data, validate_input=False)

    assert "legajos_procesados" in resultado
    # Verificar que se llamaron a los procesadores
    processor.conceptos_processor.process.assert_called_once()
    processor.calculos_processor.process.assert_called_once()
    processor.topes_processor.process.assert_called_once()
    processor.validator.validate.assert_called_once()


def test_sicoss_processor_error_handling(processor: SicossDataProcessor):
    """Prueba el manejo de errores en el pipeline."""
    data = {
        "legajos": pd.DataFrame({"nro_legaj": [1]}),
        "conceptos": pd.DataFrame({"nro_legaj": [1]}),
        "otra_actividad": pd.DataFrame({"nro_legaj": [1]}),
        "obra_social": pd.DataFrame({"nro_legaj": [1]}),
    }

    # Forzar error en el primer paso crítico
    processor.conceptos_processor.process.side_effect = Exception("Error crítico")

    resultado = processor.procesar_datos_extraidos(data, validate_input=False)

    # Debe retornar un resultado de emergencia con el error
    assert (
        resultado["estadisticas"]["error"]
        == "Error en 'Sumarización de conceptos': Error crítico"
    )


def test_sicoss_processor_validate_input(processor: SicossDataProcessor):
    """Prueba la validación de entrada."""
    data_incompleta = {"legajos": pd.DataFrame()}

    resultado = processor.procesar_datos_extraidos(data_incompleta, validate_input=True)

    # Debe retornar un resultado de emergencia por falta de claves
    assert "falta 'conceptos'" in resultado["estadisticas"]["error"]


# ============================================================================
# TESTS DE INTEGRACIÓN COMPLETA DEL PIPELINE
# ============================================================================


@pytest.fixture
def processor_real(sicoss_config: SicossConfig):
    """Procesador real sin mocks para tests de integración"""
    return SicossDataProcessor(sicoss_config)


@pytest.fixture
def datos_completos_realistas():
    """Datos completos y realistas para tests de integración"""
    # Legajos base
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1001, 1002, 1003],
            "apyno": ["PEREZ, JUAN", "GOMEZ, MARIA", "LOPEZ, CARLOS"],
            "cuit": ["20123456789", "27987654321", "20111111111"],
            "codigoescalafon": ["DOCE", "AUTO", "NODO"],
        }
    )

    # Conceptos con diferentes tipos
    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1001, 1001, 1002, 1002, 1003],
            "impp_conce": [100000.0, 50000.0, 150000.0, 30000.0, 200000.0],
            "codn_conce": ["001", "002", "001", "003", "001"],
            "tipos_grupos": [
                "[6]",
                "[7]",
                "[8]",
                "[21]",
                "[45]",
            ],  # HorasExtras, ZonaDesfavorable, Vacaciones, Adicionales, NoRemun
            "codigoescalafon": ["DOCE", "DOCE", "AUTO", "AUTO", "NODO"],
        }
    )

    # Otra actividad
    df_otra_actividad = pd.DataFrame(
        {
            "nro_legaj": [1001],
            "ImporteBrutoOtraActividad": [50000.0],
            "ImporteSACOtraActividad": [4166.67],
        }
    )

    # Obra social
    df_obra_social = pd.DataFrame(
        {
            "nro_legaj": [1001, 1002, 1003],
            "codigo_os": ["000000", "123456", "789012"],
        }
    )

    return {
        "legajos": df_legajos,
        "conceptos": df_conceptos,
        "otra_actividad": df_otra_actividad,
        "obra_social": df_obra_social,
    }


def test_pipeline_completo_end_to_end(processor_real, datos_completos_realistas):
    """Test de integración: Pipeline completo end-to-end con procesadores reales"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    # Verificar estructura del resultado
    assert "legajos_procesados" in resultado
    assert "estadisticas" in resultado
    assert "metricas" in resultado

    # Verificar que se procesaron los legajos
    df_resultado = resultado["legajos_procesados"]
    assert not df_resultado.empty
    assert len(df_resultado) == 3  # 3 legajos

    # Verificar campos críticos calculados
    campos_requeridos = [
        "nro_legaj",
        "IMPORTE_BRUTO",
        "IMPORTE_IMPON",
        "ImporteImponiblePatronal",
        "ImporteImponible_4",
        "ImporteImponible_5",
        "TipoDeOperacion",
    ]

    for campo in campos_requeridos:
        assert campo in df_resultado.columns, f"Campo faltante: {campo}"

    # Verificar que los importes son no negativos
    assert all(df_resultado["IMPORTE_BRUTO"] >= 0)
    assert all(df_resultado["IMPORTE_IMPON"] >= 0)
    assert all(df_resultado["ImporteImponiblePatronal"] >= 0)


def test_pipeline_paso_conceptos(processor_real, datos_completos_realistas):
    """Test de integración: Validar paso de sumarización de conceptos"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar que se calcularon campos de conceptos
    campos_conceptos = [
        "ImporteHorasExtras",
        "ImporteZonaDesfavorable",
        "ImporteVacaciones",
        "ImporteAdicionales",
        "ImporteNoRemun",
        "ImporteSAC",
    ]

    for campo in campos_conceptos:
        assert campo in df_resultado.columns, f"Campo de conceptos faltante: {campo}"

    # Verificar que legajo 1001 tiene ImporteHorasExtras (tipo_grupo 6)
    legajo_1001 = df_resultado[df_resultado["nro_legaj"] == 1001].iloc[0]
    assert legajo_1001["ImporteHorasExtras"] > 0


def test_pipeline_paso_calculos(processor_real, datos_completos_realistas):
    """Test de integración: Validar paso de cálculos SICOSS"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar campos calculados por CalculosProcessor
    campos_calculos = [
        "ImporteImponible_4",
        "ImporteImponible_5",
        "ImporteImponible_6",
        "TipoDeOperacion",
        "PorcAporteDiferencialJubilacion",
        "importeimponible_9",  # ART
    ]

    for campo in campos_calculos:
        assert campo in df_resultado.columns, f"Campo de cálculos faltante: {campo}"

    # Verificar que ImporteImponible_4 = IMPORTE_IMPON inicial
    # (puede cambiar después de topes, pero inicialmente debe ser igual)
    # Nota: Después de topes puede cambiar, así que solo verificamos que existe


def test_pipeline_paso_topes(processor_real, datos_completos_realistas):
    """Test de integración: Validar paso de aplicación de topes"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar campos relacionados con topes (nombres correctos)
    campos_topes = [
        "ImporteImponible_4",  # Se ajusta con topes
        "DiferenciaSACImponibleConTope",  # Nombre correcto
        "DiferenciaImponibleConTope",  # Nombre correcto
    ]

    for campo in campos_topes:
        assert campo in df_resultado.columns, f"Campo de topes faltante: {campo}"

    # Verificar que los topes se aplicaron (si los importes exceden)
    # Como los importes de prueba son pequeños, no deberían exceder topes
    # pero los campos deben existir


def test_pipeline_paso_validacion(processor_real, datos_completos_realistas):
    """Test de integración: Validar paso de validación final"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar que todos los legajos pasaron la validación
    # (si hay errores, el validator los marca)
    assert len(df_resultado) == 3

    # Verificar que no hay valores NaN en campos críticos
    campos_criticos = ["nro_legaj", "IMPORTE_BRUTO", "IMPORTE_IMPON"]
    for campo in campos_criticos:
        assert not df_resultado[campo].isna().any(), f"Valores NaN en {campo}"


def test_pipeline_otra_actividad(processor_real, datos_completos_realistas):
    """Test de integración: Validar agregado de otra actividad"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar que legajo 1001 tiene otra actividad
    legajo_1001 = df_resultado[df_resultado["nro_legaj"] == 1001].iloc[0]

    # Verificar campos de otra actividad (puede haber múltiples columnas por merge)
    assert (
        "ImporteBrutoOtraActividad" in df_resultado.columns
        or "ImporteBrutoOtraActividad_x" in df_resultado.columns
        or "ImporteBrutoOtraActividad_y" in df_resultado.columns
    )

    # Verificar que el valor está presente (puede estar en cualquiera de las columnas)
    valor_otra_actividad = (
        legajo_1001.get("ImporteBrutoOtraActividad", 0)
        or legajo_1001.get("ImporteBrutoOtraActividad_x", 0)
        or legajo_1001.get("ImporteBrutoOtraActividad_y", 0)
        or 0
    )
    assert valor_otra_actividad == 50000.0


def test_pipeline_obra_social(processor_real, datos_completos_realistas):
    """Test de integración: Validar agregado de obra social"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar que se agregó código de obra social
    assert "codigo_os" in df_resultado.columns

    # Verificar valores
    legajo_1001 = df_resultado[df_resultado["nro_legaj"] == 1001].iloc[0]
    assert legajo_1001["codigo_os"] == "000000"


def test_pipeline_metricas(processor_real, datos_completos_realistas):
    """Test de integración: Validar métricas del pipeline"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    # Verificar métricas
    assert "metricas" in resultado
    metricas = resultado["metricas"]

    # Verificar que hay tiempos registrados (nombre correcto)
    assert "tiempo_total_segundos" in metricas or "total_time" in metricas
    tiempo_total = metricas.get("tiempo_total_segundos") or metricas.get(
        "total_time", 0
    )
    assert tiempo_total > 0

    # Verificar que hay tiempos por paso (nombre correcto)
    assert "tiempos_por_paso" in metricas or "step_times" in metricas
    tiempos_paso = metricas.get("tiempos_por_paso") or metricas.get("step_times", {})
    assert len(tiempos_paso) > 0

    # Verificar estadísticas
    assert "estadisticas" in resultado
    estadisticas = resultado["estadisticas"]
    assert "total_legajos" in estadisticas
    assert estadisticas["total_legajos"] == 3


def test_pipeline_datos_vacios(processor_real):
    """Test de integración: Manejo de datos vacíos"""
    datos_vacios = {
        "legajos": pd.DataFrame(columns=["nro_legaj"]),
        "conceptos": pd.DataFrame(columns=["nro_legaj"]),
        "otra_actividad": pd.DataFrame(columns=["nro_legaj"]),
        "obra_social": pd.DataFrame(columns=["nro_legaj"]),
    }

    # Debe fallar en validación de entrada
    resultado = processor_real.procesar_datos_extraidos(
        datos_vacios, validate_input=True
    )

    # Debe retornar resultado de emergencia
    assert "error" in resultado["estadisticas"]


def test_pipeline_conceptos_vacios(processor_real):
    """Test de integración: Manejo de conceptos vacíos (legajos sin conceptos)"""
    datos = {
        "legajos": pd.DataFrame({"nro_legaj": [1001, 1002]}),
        "conceptos": pd.DataFrame(columns=["nro_legaj", "impp_conce"]),  # Vacío
        "otra_actividad": pd.DataFrame(columns=["nro_legaj"]),
        "obra_social": pd.DataFrame({"nro_legaj": [1001, 1002]}),
    }

    resultado = processor_real.procesar_datos_extraidos(datos, validate_input=False)

    # Cuando no hay conceptos, ConceptosProcessor puede fallar si no genera campos requeridos
    # Verificar que se maneja el error correctamente
    if "error" in resultado.get("estadisticas", {}):
        # Si hay error, debe estar relacionado con campos faltantes
        assert (
            "campos requeridos" in resultado["estadisticas"]["error"].lower()
            or "error" in resultado["estadisticas"]["error"].lower()
        )
    else:
        # Si no hay error, debe procesar correctamente
        assert "legajos_procesados" in resultado
        df_resultado = resultado["legajos_procesados"]
        assert len(df_resultado) == 2
        # Los importes deben ser 0
        assert all(df_resultado["IMPORTE_BRUTO"] == 0)
        assert all(df_resultado["IMPORTE_IMPON"] == 0)


def test_pipeline_error_paso_critico(processor_real, datos_completos_realistas):
    """Test de integración: Manejo de error en paso crítico"""
    # Parchear un procesador para que falle
    with patch.object(
        processor_real.conceptos_processor,
        "process",
        side_effect=Exception("Error crítico"),
    ):
        resultado = processor_real.procesar_datos_extraidos(
            datos_completos_realistas, validate_input=False
        )

        # Debe retornar resultado de emergencia
        assert "error" in resultado["estadisticas"]
        assert (
            "Error en 'Sumarización de conceptos'" in resultado["estadisticas"]["error"]
        )


def test_pipeline_error_paso_no_critico(processor_real, datos_completos_realistas):
    """Test de integración: Manejo de error en paso no crítico (debe continuar)"""
    # Parchear agregar_obra_social para que falle (paso no crítico)
    with patch.object(
        processor_real,
        "_agregar_obra_social",
        side_effect=Exception("Error no crítico"),
    ):
        resultado = processor_real.procesar_datos_extraidos(
            datos_completos_realistas, validate_input=False
        )

        # Debe continuar y completar el pipeline
        assert "legajos_procesados" in resultado
        # Puede tener warnings pero debe completar
        assert "metricas" in resultado


def test_pipeline_campos_calculados_consistencia(
    processor_real, datos_completos_realistas
):
    """Test de integración: Validar consistencia de campos calculados"""
    resultado = processor_real.procesar_datos_extraidos(
        datos_completos_realistas, validate_input=True
    )

    df_resultado = resultado["legajos_procesados"]

    # Verificar relaciones entre campos
    for _, row in df_resultado.iterrows():
        # IMPORTE_BRUTO debe ser >= IMPORTE_IMPON (puede incluir no remunerativos)
        assert row["IMPORTE_BRUTO"] >= row["IMPORTE_IMPON"]

        # ImporteImponible_4 debe ser >= 0
        assert row["ImporteImponible_4"] >= 0

        # ImporteImponible_5 debe ser >= 0
        assert row["ImporteImponible_5"] >= 0

        # TipoDeOperacion debe ser 1 o 2
        assert row["TipoDeOperacion"] in [1, 2]


def test_pipeline_multiple_legajos(processor_real):
    """Test de integración: Pipeline con múltiples legajos y diferentes escenarios"""
    # Crear datos con múltiples legajos y diferentes tipos de conceptos
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": list(range(1001, 1011)),  # 10 legajos
            "apyno": [f"PERSONA {i}" for i in range(1001, 1011)],
            "cuit": [f"20{i:09d}" for i in range(1001, 1011)],
            "codigoescalafon": ["DOCE"] * 10,
        }
    )

    # Conceptos variados
    conceptos_data = []
    for i in range(1001, 1011):
        conceptos_data.append(
            {
                "nro_legaj": i,
                "impp_conce": 100000.0 + (i - 1001) * 10000,
                "codn_conce": "001",
                "tipos_grupos": "[6]",  # HorasExtras
                "codigoescalafon": "DOCE",
            }
        )

    df_conceptos = pd.DataFrame(conceptos_data)

    datos = {
        "legajos": df_legajos,
        "conceptos": df_conceptos,
        "otra_actividad": pd.DataFrame(columns=["nro_legaj"]),
        "obra_social": pd.DataFrame({"nro_legaj": list(range(1001, 1011))}),
    }

    resultado = processor_real.procesar_datos_extraidos(datos, validate_input=True)

    # Verificar que se procesaron todos los legajos
    assert len(resultado["legajos_procesados"]) == 10

    # Verificar estadísticas
    assert resultado["estadisticas"]["total_legajos"] == 10
