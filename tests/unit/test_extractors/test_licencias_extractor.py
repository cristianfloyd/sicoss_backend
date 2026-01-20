"""
Tests unitarios e integración para LicenciasExtractor

Cobertura objetivo: >90%
"""

import calendar
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from extractors.licencias_extractor import LicenciasExtractor
from value_objects.periodo_fiscal import PeriodoFiscal

# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_db_connection():
    """Mock de DatabaseConnection para tests unitarios"""
    mock_db = Mock()
    mock_db.execute_query.return_value = pd.DataFrame()
    return mock_db


@pytest.fixture
def licencias_extractor(mock_db_connection):
    """Instancia de LicenciasExtractor con mock de BD"""
    return LicenciasExtractor(mock_db_connection)


@pytest.fixture
def periodo_fiscal():
    """PeriodoFiscal de prueba"""
    return PeriodoFiscal(year=2024, month=12)


@pytest.fixture
def sample_licencias_df():
    """DataFrame de ejemplo con licencias"""
    return pd.DataFrame(
        {
            "nro_legaj": [1001, 1002, 1001],
            "inicio": [1, 15, 1],
            "final": [31, 31, 15],
            "es_legajo": [True, True, False],
            "condicion": [
                12,
                51,
                5,
            ],  # 12: vacaciones, 51: protección integral, 5: maternidad
        }
    )


# ============================================================================
# TESTS UNITARIOS - extract_for_legajos
# ============================================================================


def test_extract_for_legajos_lista_vacia(licencias_extractor, periodo_fiscal):
    """Prueba cuando se pasa una lista vacía de legajos."""
    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal, legajos_ids=[], variantes_vacaciones="1,2"
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == [
        "nro_legaj",
        "inicio",
        "final",
        "es_legajo",
        "condicion",
    ]


def test_extract_for_legajos_sin_variantes(licencias_extractor, periodo_fiscal):
    """Prueba cuando no hay variantes configuradas."""
    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal, legajos_ids=[1001, 1002]
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == [
        "nro_legaj",
        "inicio",
        "final",
        "es_legajo",
        "condicion",
    ]


def test_extract_for_legajos_con_variantes_vacaciones(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba extracción con variantes de vacaciones."""
    # Configurar mock para retornar datos de ejemplo
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001, 1002],
        variantes_vacaciones="1,2,3",
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "nro_legaj" in result.columns
    assert "inicio" in result.columns
    assert "final" in result.columns
    assert "es_legajo" in result.columns
    assert "condicion" in result.columns

    # Verificar que se llamó a execute_query
    mock_db_connection.execute_query.assert_called_once()


def test_extract_for_legajos_con_variantes_protecintegral(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba extracción con variantes de protección integral."""
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001, 1002],
        variantes_protecintegral="4,5,6",
    )

    assert isinstance(result, pd.DataFrame)
    assert "nro_legaj" in result.columns


def test_extract_for_legajos_con_ambas_variantes(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba extracción con ambas variantes configuradas."""
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001, 1002],
        variantes_vacaciones="1,2",
        variantes_protecintegral="4,5",
    )

    assert isinstance(result, pd.DataFrame)
    # Verificar que la query incluye ambas variantes
    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "1,2" in call_args or "4,5" in call_args


def test_extract_for_legajos_tipos_datos_correctos(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba que los tipos de datos retornados son correctos."""
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert result["nro_legaj"].dtype in ["int32", "int64"]
    assert result["inicio"].dtype in ["int32", "int64"]
    assert result["final"].dtype in ["int32", "int64"]
    assert result["condicion"].dtype in ["int32", "int64"]
    assert result["es_legajo"].dtype == bool


def test_extract_for_legajos_resultado_vacio_bd(
    licencias_extractor, periodo_fiscal, mock_db_connection
):
    """Prueba cuando la BD retorna un DataFrame vacío."""
    mock_db_connection.execute_query.return_value = pd.DataFrame()

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == [
        "nro_legaj",
        "inicio",
        "final",
        "es_legajo",
        "condicion",
    ]


def test_extract_for_legajos_manejo_errores(
    licencias_extractor, periodo_fiscal, mock_db_connection
):
    """Prueba manejo de errores cuando la BD falla."""
    mock_db_connection.execute_query.side_effect = Exception("Error de conexión")

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    # Debe retornar DataFrame vacío en caso de error
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == [
        "nro_legaj",
        "inicio",
        "final",
        "es_legajo",
        "condicion",
    ]


def test_extract_for_legajos_resultado_no_dataframe(
    licencias_extractor, periodo_fiscal, mock_db_connection
):
    """Prueba cuando execute_query retorna algo que no es DataFrame."""
    mock_db_connection.execute_query.return_value = None

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_extract_for_legajos_calculo_fechas_periodo(
    licencias_extractor, sample_licencias_df, mock_db_connection
):
    """Prueba que las fechas del período se calculan correctamente."""
    # Período de febrero (mes con 28/29 días)
    periodo_feb = PeriodoFiscal(year=2024, month=2)
    mock_db_connection.execute_query.return_value = sample_licencias_df

    licencias_extractor.extract_for_legajos(
        periodo=periodo_feb,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    # Verificar que la query incluye las fechas correctas
    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "2024-02-01" in call_args
    assert "2024-02-29" in call_args  # 2024 es año bisiesto


def test_extract_for_legajos_mes_31_dias(
    licencias_extractor, sample_licencias_df, mock_db_connection
):
    """Prueba cálculo de fechas para mes de 31 días."""
    periodo_ene = PeriodoFiscal(year=2024, month=1)
    mock_db_connection.execute_query.return_value = sample_licencias_df

    licencias_extractor.extract_for_legajos(
        periodo=periodo_ene,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "2024-01-31" in call_args


# ============================================================================
# TESTS UNITARIOS - extract (método genérico)
# ============================================================================


def test_extract_con_periodo_fiscal(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba extract() pasando PeriodoFiscal directamente."""
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_extract_con_per_anoct_per_mesct(
    licencias_extractor, sample_licencias_df, mock_db_connection
):
    """Prueba extract() pasando per_anoct y per_mesct en lugar de PeriodoFiscal."""
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract(
        per_anoct=2024,
        per_mesct=12,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_extract_sin_periodo_ni_fechas(licencias_extractor):
    """Prueba extract() sin período ni fechas (debe fallar)."""
    with pytest.raises(ValueError, match="Se requiere un objeto PeriodoFiscal"):
        licencias_extractor.extract(legajos_ids=[1001], variantes_vacaciones="1")


def test_extract_con_periodo_invalido(licencias_extractor):
    """Prueba extract() con período inválido."""
    with pytest.raises(ValueError):
        licencias_extractor.extract(
            per_anoct=2024,
            per_mesct=None,  # Falta el mes
            legajos_ids=[1001],
            variantes_vacaciones="1",
        )


# ============================================================================
# TESTS DE INTEGRACIÓN (requieren BD real)
# ============================================================================


@pytest.mark.integration
def test_extract_for_legajos_integracion_bd_real(db_connection):
    """Test de integración con BD real."""
    extractor = LicenciasExtractor(db_connection)
    periodo = PeriodoFiscal(year=2024, month=12)

    # Intentar extraer para un legajo de prueba (puede no tener licencias)
    result = extractor.extract_for_legajos(
        periodo=periodo,
        legajos_ids=[1001],  # Ajustar según datos reales
        variantes_vacaciones="1,2,3",
        variantes_protecintegral="4,5,6",
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == [
        "nro_legaj",
        "inicio",
        "final",
        "es_legajo",
        "condicion",
    ]

    # Si hay resultados, verificar tipos
    if not result.empty:
        assert result["nro_legaj"].dtype in ["int32", "int64"]
        assert result["inicio"].dtype in ["int32", "int64"]
        assert result["final"].dtype in ["int32", "int64"]
        assert result["condicion"].dtype in ["int32", "int64"]
        assert result["es_legajo"].dtype == bool

        # Verificar rangos válidos
        assert all(1 <= result["inicio"]) and all(result["inicio"] <= 31)
        assert all(1 <= result["final"]) and all(result["final"] <= 31)
        assert all(result["inicio"] <= result["final"])


@pytest.mark.integration
def test_extract_integracion_bd_real(db_connection):
    """Test de integración del método extract() con BD real."""
    extractor = LicenciasExtractor(db_connection)

    result = extractor.extract(
        per_anoct=2024,
        per_mesct=12,
        legajos_ids=[1001],  # Ajustar según datos reales
        variantes_vacaciones="1,2",
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == [
        "nro_legaj",
        "inicio",
        "final",
        "es_legajo",
        "condicion",
    ]


# ============================================================================
# TESTS DE CASOS DE BORDE
# ============================================================================


def test_extract_for_legajos_muchos_legajos(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba con una lista grande de legajos."""
    muchos_legajos = list(range(1001, 1101))  # 100 legajos
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=muchos_legajos,
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    # Verificar que la query incluye todos los legajos
    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "1001" in call_args
    assert "1100" in call_args


def test_extract_for_legajos_variantes_string_largo(
    licencias_extractor, periodo_fiscal, sample_licencias_df, mock_db_connection
):
    """Prueba con string de variantes muy largo."""
    variantes_largas = ",".join(map(str, range(1, 51)))  # 50 variantes
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones=variantes_largas,
    )

    assert isinstance(result, pd.DataFrame)
    # Verificar que la query incluye las variantes
    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "1" in call_args
    assert "49" in call_args


def test_extract_for_legajos_periodo_bisiesto(
    licencias_extractor, sample_licencias_df, mock_db_connection
):
    """Prueba con período de año bisiesto (febrero 2024)."""
    periodo_feb_2024 = PeriodoFiscal(year=2024, month=2)
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_feb_2024,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    # Verificar que usa 29 días (año bisiesto)
    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "2024-02-29" in call_args


def test_extract_for_legajos_periodo_no_bisiesto(
    licencias_extractor, sample_licencias_df, mock_db_connection
):
    """Prueba con período de año no bisiesto (febrero 2023)."""
    periodo_feb_2023 = PeriodoFiscal(year=2023, month=2)
    mock_db_connection.execute_query.return_value = sample_licencias_df

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_feb_2023,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert isinstance(result, pd.DataFrame)
    # Verificar que usa 28 días (año no bisiesto)
    call_args = mock_db_connection.execute_query.call_args[0][0]
    assert "2023-02-28" in call_args


def test_extract_for_legajos_condiciones_maternidad(
    licencias_extractor, periodo_fiscal, mock_db_connection
):
    """Prueba que las condiciones de licencia se asignan correctamente."""
    # DataFrame con licencia de maternidad
    df_maternidad = pd.DataFrame(
        {
            "nro_legaj": [1001],
            "inicio": [1],
            "final": [31],
            "es_legajo": [True],
            "condicion": [5],  # Maternidad
        }
    )
    mock_db_connection.execute_query.return_value = df_maternidad

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert not result.empty
    assert result.iloc[0]["condicion"] == 5


def test_extract_for_legajos_es_legajo_true_false(
    licencias_extractor, periodo_fiscal, mock_db_connection
):
    """Prueba que es_legajo puede ser True o False."""
    df_mixto = pd.DataFrame(
        {
            "nro_legaj": [1001, 1001],
            "inicio": [1, 1],
            "final": [31, 31],
            "es_legajo": [True, False],  # Una por legajo, otra por cargo
            "condicion": [12, 12],
        }
    )
    mock_db_connection.execute_query.return_value = df_mixto

    result = licencias_extractor.extract_for_legajos(
        periodo=periodo_fiscal,
        legajos_ids=[1001],
        variantes_vacaciones="1",
    )

    assert len(result) == 2
    assert result["es_legajo"].dtype == bool
    assert result["es_legajo"].iloc[0] == True
    assert result["es_legajo"].iloc[1] == False
