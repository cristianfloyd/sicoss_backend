"""
Tests unitarios completos para ConceptosProcessor

Cobertura objetivo: >90%
"""

import pandas as pd
import pytest

from config.sicoss_config import SicossConfig
from processors.conceptos_processor import ConceptosProcessor


@pytest.fixture
def sicoss_config():
    """Configuración SICOSS para tests"""
    return SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
    )


@pytest.fixture
def conceptos_processor(sicoss_config: SicossConfig):
    """Procesador de conceptos para tests"""
    return ConceptosProcessor(sicoss_config)


# ============================================================================
# TESTS BÁSICOS
# ============================================================================


def test_conceptos_processor_basic_sum(conceptos_processor: ConceptosProcessor):
    """Prueba la sumarización básica de conceptos por tipo."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [123],
            "sexo": ["M"],
            "f_nacim": ["1980-01-01"],
            "codigoactividad": [0],
        }
    )

    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [123, 123, 123],
            "codn_conce": [1, 2, 3],
            "impp_conce": [1000.0, 500.0, 200.0],
            "tipo_conce": ["R", "R", "N"],
            "tipos_grupos": [[9], [4], [45]],  # 9: SAC, 4: Básico, 45: No Remun
            "codigoescalafon": ["01", "01", "01"],
        }
    )

    result = conceptos_processor.process(df_legajos, df_conceptos)

    assert len(result) == 1
    assert result.iloc[0]["ImporteSAC"] == 1000.0
    assert result.iloc[0]["ImporteNoRemun"] == 200.0


def test_conceptos_processor_multiple_groups(conceptos_processor: ConceptosProcessor):
    """Prueba un concepto que pertenece a múltiples grupos SICOSS."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "codigoactividad": [0],
        }
    )
    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "codn_conce": [100],
            "impp_conce": [5000.0],
            "tipo_conce": ["R"],
            "tipos_grupos": [[9, 6]],  # 9: SAC, 6: Horas Extras
            "codigoescalafon": ["01"],
        }
    )

    result = conceptos_processor.process(df_legajos, df_conceptos)

    assert result.iloc[0]["ImporteSAC"] == 5000.0
    assert result.iloc[0]["ImporteHorasExtras"] == 5000.0


def test_conceptos_processor_empty_input(conceptos_processor: ConceptosProcessor):
    """Prueba el comportamiento con DataFrames vacíos."""
    df_legajos = pd.DataFrame(columns=["nro_legaj"])
    df_conceptos = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipos_grupos"])

    result = conceptos_processor.process(df_legajos, df_conceptos)
    assert result.empty


def test_conceptos_processor_empty_conceptos(conceptos_processor: ConceptosProcessor):
    """Prueba cuando hay legajos pero sin conceptos."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [100, 200],
            "codigoactividad": [0, 0],
        }
    )
    df_conceptos = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipos_grupos"])

    result = conceptos_processor.process(df_legajos, df_conceptos)

    assert len(result) == 2
    # Todos los campos SICOSS deben estar inicializados en 0
    assert result.iloc[0]["ImporteSAC"] == 0.0
    assert result.iloc[0]["ImporteHorasExtras"] == 0.0


# ============================================================================
# TESTS PARA _expandir_tipos_grupos
# ============================================================================


def test_expandir_tipos_grupos_lista(conceptos_processor: ConceptosProcessor):
    """Prueba expansión con listas de Python."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "impp_conce": [1000.0, 2000.0],
            "codn_conce": [10, 20],
            "codigoescalafon": ["01", "01"],
            "tipos_grupos": [[6, 7], [8, 9]],
        }
    )

    result = conceptos_processor._expandir_tipos_grupos(df)

    assert len(result) == 4  # 2 conceptos * 2 tipos cada uno
    assert set(result["tipo_grupo"].tolist()) == {6, 7, 8, 9}


def test_expandir_tipos_grupos_string(conceptos_processor: ConceptosProcessor):
    """Prueba expansión con formato string {1,2,3}."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [1000.0],
            "codn_conce": [10],
            "codigoescalafon": ["01"],
            "tipos_grupos": ["{6,7,8}"],
        }
    )

    result = conceptos_processor._expandir_tipos_grupos(df)

    assert len(result) == 3
    assert set(result["tipo_grupo"].tolist()) == {6, 7, 8}


def test_expandir_tipos_grupos_none(conceptos_processor: ConceptosProcessor):
    """Prueba expansión con valores None."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "impp_conce": [1000.0, 2000.0],
            "codn_conce": [10, 20],
            "codigoescalafon": ["01", "01"],
            "tipos_grupos": [None, [6]],
        }
    )

    result = conceptos_processor._expandir_tipos_grupos(df)

    # Solo el segundo concepto tiene tipos válidos
    assert len(result) == 1
    assert result.iloc[0]["tipo_grupo"] == 6


def test_expandir_tipos_grupos_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba expansión con listas vacías."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [1000.0],
            "codn_conce": [10],
            "codigoescalafon": ["01"],
            "tipos_grupos": [[]],
        }
    )

    result = conceptos_processor._expandir_tipos_grupos(df)

    # Debe retornar DataFrame vacío con columnas correctas
    assert result.empty
    assert "tipo_grupo" in result.columns


def test_expandir_tipos_grupos_string_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba expansión con strings vacíos."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [1000.0],
            "codn_conce": [10],
            "codigoescalafon": ["01"],
            "tipos_grupos": ["{}"],
        }
    )

    result = conceptos_processor._expandir_tipos_grupos(df)
    assert result.empty


# ============================================================================
# TESTS PARA _procesar_casos_simples
# ============================================================================


def test_procesar_casos_simples_horas_extras(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de horas extras (tipo 6)."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 1],
            "impp_conce": [5000.0, 3000.0],
            "tipo_grupo": [6, 6],
        }
    )

    result = conceptos_processor._procesar_casos_simples(df)

    assert len(result) == 2
    assert all(result["campo_sicoss"] == "ImporteHorasExtras")
    assert result["valor"].sum() == 8000.0


def test_procesar_casos_simples_vacaciones(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de vacaciones (tipo 8)."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [10000.0],
            "tipo_grupo": [8],
        }
    )

    result = conceptos_processor._procesar_casos_simples(df)

    assert len(result) == 1
    assert result.iloc[0]["campo_sicoss"] == "ImporteVacaciones"
    assert result.iloc[0]["valor"] == 10000.0


def test_procesar_casos_simples_multiple_tipos(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de múltiples tipos simples."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 1, 1],
            "impp_conce": [1000.0, 2000.0, 3000.0],
            "tipo_grupo": [6, 7, 8],  # Horas extras, Zona desfavorable, Vacaciones
        }
    )

    result = conceptos_processor._procesar_casos_simples(df)

    assert len(result) == 3
    campos = set(result["campo_sicoss"].tolist())
    assert "ImporteHorasExtras" in campos
    assert "ImporteZonaDesfavorable" in campos
    assert "ImporteVacaciones" in campos


def test_procesar_casos_simples_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento con DataFrame vacío."""
    df = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipo_grupo"])

    result = conceptos_processor._procesar_casos_simples(df)

    assert result.empty
    assert "campo_sicoss" in result.columns
    assert "valor" in result.columns


# ============================================================================
# TESTS PARA _procesar_sac_escalafon
# ============================================================================


def test_procesar_sac_escalafon_doce(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de SAC para escalafón DOCE."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [50000.0],
            "codigoescalafon": ["DOCE"],
            "tipo_grupo": [9],
        }
    )

    result = conceptos_processor._procesar_sac_escalafon(df)

    assert len(result) == 2  # ImporteSAC + ImporteSACDoce
    campos = set(result["campo_sicoss"].tolist())
    assert "ImporteSAC" in campos
    assert "ImporteSACDoce" in campos
    assert result[result["campo_sicoss"] == "ImporteSAC"]["valor"].iloc[0] == 50000.0
    assert (
        result[result["campo_sicoss"] == "ImporteSACDoce"]["valor"].iloc[0] == 50000.0
    )


def test_procesar_sac_escalafon_auto(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de SAC para escalafón AUTO."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [60000.0],
            "codigoescalafon": ["AUTO"],
            "tipo_grupo": [9],
        }
    )

    result = conceptos_processor._procesar_sac_escalafon(df)

    campos = set(result["campo_sicoss"].tolist())
    assert "ImporteSAC" in campos
    assert "ImporteSACAuto" in campos


def test_procesar_sac_escalafon_nodo(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de SAC para escalafón NODO."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [70000.0],
            "codigoescalafon": ["NODO"],
            "tipo_grupo": [9],
        }
    )

    result = conceptos_processor._procesar_sac_escalafon(df)

    campos = set(result["campo_sicoss"].tolist())
    assert "ImporteSAC" in campos
    assert "ImporteSACNodo" in campos


def test_procesar_sac_escalafon_otro(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de SAC para escalafón no especial."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [80000.0],
            "codigoescalafon": ["01"],
            "tipo_grupo": [9],
        }
    )

    result = conceptos_processor._procesar_sac_escalafon(df)

    # Solo debe tener ImporteSAC, no campos específicos de escalafón
    assert len(result) == 1
    assert result.iloc[0]["campo_sicoss"] == "ImporteSAC"


def test_procesar_sac_escalafon_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de SAC con DataFrame vacío."""
    df = pd.DataFrame(
        columns=["nro_legaj", "impp_conce", "codigoescalafon", "tipo_grupo"]
    )

    result = conceptos_processor._procesar_sac_escalafon(df)

    assert result.empty


# ============================================================================
# TESTS PARA _procesar_investigadores
# ============================================================================


def test_procesar_investigadores_tipo_11(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de investigador tipo 11."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [100000.0],
            "tipo_grupo": [11],
        }
    )

    result = conceptos_processor._procesar_investigadores(df)

    assert len(result) == 2  # ImporteImponible_6 + PrioridadTipoDeActividad
    campos = set(result["campo_sicoss"].tolist())
    assert "ImporteImponible_6" in campos
    assert "PrioridadTipoDeActividad" in campos

    importe = result[result["campo_sicoss"] == "ImporteImponible_6"]["valor"].iloc[0]
    assert importe == 100000.0

    prioridad = result[result["campo_sicoss"] == "PrioridadTipoDeActividad"][
        "valor"
    ].iloc[0]
    assert prioridad == 38  # Prioridad para tipo 11


def test_procesar_investigadores_tipo_12(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de investigador tipo 12."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [150000.0],
            "tipo_grupo": [12],
        }
    )

    result = conceptos_processor._procesar_investigadores(df)

    prioridad = result[result["campo_sicoss"] == "PrioridadTipoDeActividad"][
        "valor"
    ].iloc[0]
    assert prioridad == 34  # Prioridad para tipo 12


def test_procesar_investigadores_multiple(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de múltiples tipos de investigadores."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 1],
            "impp_conce": [100000.0, 200000.0],
            "tipo_grupo": [12, 13],  # Prioridades 34 y 35
        }
    )

    result = conceptos_processor._procesar_investigadores(df)

    # Debe tomar la máxima prioridad
    prioridad = result[result["campo_sicoss"] == "PrioridadTipoDeActividad"][
        "valor"
    ].iloc[0]
    assert prioridad == 35  # Máxima prioridad entre 34 y 35


def test_procesar_investigadores_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de investigadores con DataFrame vacío."""
    df = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipo_grupo"])

    result = conceptos_processor._procesar_investigadores(df)

    assert result.empty


# ============================================================================
# TESTS PARA _procesar_casos_especiales
# ============================================================================


def test_procesar_casos_especiales_seguro_vida(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de Seguro Vida Obligatorio (tipo 58)."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 1, 2],
            "impp_conce": [1000.0, 2000.0, 3000.0],
            "tipo_grupo": [
                58,
                6,
                58,
            ],  # Dos seguros vida para legajo 1, uno para legajo 2
        }
    )

    result = conceptos_processor._procesar_casos_especiales(df)

    # Debe haber solo 2 filas (uno por legajo único con tipo 58)
    assert len(result) == 2
    assert all(result["campo_sicoss"] == "SeguroVidaObligatorio")
    assert all(result["valor"] == 1)
    assert set(result["nro_legaj"].tolist()) == {1, 2}


def test_procesar_casos_especiales_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de casos especiales con DataFrame vacío."""
    df = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipo_grupo"])

    result = conceptos_processor._procesar_casos_especiales(df)

    assert result.empty


# ============================================================================
# TESTS PARA _procesar_asignaciones_tipo_f
# ============================================================================


def test_procesar_asignaciones_tipo_f(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de asignaciones familiares (tipo_conce 'F')."""
    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1, 1, 2],
            "impp_conce": [5000.0, 3000.0, 2000.0],
            "tipo_conce": ["F", "R", "F"],  # Dos asignaciones familiares
        }
    )

    result = conceptos_processor._procesar_asignaciones_tipo_f(df_conceptos)

    assert len(result) == 2
    assert all(result["campo_sicoss"] == "AsignacionesFliaresPagadas")
    assert result["valor"].sum() == 7000.0  # 5000 + 2000


def test_procesar_asignaciones_tipo_f_sin_campo(
    conceptos_processor: ConceptosProcessor,
):
    """Prueba cuando no existe el campo tipo_conce."""
    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "impp_conce": [1000.0],
        }
    )

    result = conceptos_processor._procesar_asignaciones_tipo_f(df_conceptos)

    assert result.empty


def test_procesar_asignaciones_tipo_f_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba con DataFrame vacío."""
    df_conceptos = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipo_conce"])

    result = conceptos_processor._procesar_asignaciones_tipo_f(df_conceptos)

    assert result.empty


# ============================================================================
# TESTS PARA _combinar_resultados
# ============================================================================


def test_combinar_resultados(conceptos_processor: ConceptosProcessor):
    """Prueba combinación de múltiples DataFrames de resultados."""
    df1 = pd.DataFrame(
        {
            "nro_legaj": [1],
            "campo_sicoss": ["ImporteSAC"],
            "valor": [1000.0],
        }
    )
    df2 = pd.DataFrame(
        {
            "nro_legaj": [1],
            "campo_sicoss": ["ImporteHorasExtras"],
            "valor": [500.0],
        }
    )
    df3 = pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])  # Vacío

    result = conceptos_processor._combinar_resultados([df1, df2, df3])

    assert len(result) == 2
    assert set(result["campo_sicoss"].tolist()) == {"ImporteSAC", "ImporteHorasExtras"}


def test_combinar_resultados_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba combinación con todos los DataFrames vacíos."""
    dfs = [
        pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"]),
        pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"]),
    ]

    result = conceptos_processor._combinar_resultados(dfs)

    assert result.empty
    assert "campo_sicoss" in result.columns


# ============================================================================
# TESTS PARA _agrupar_por_legajo
# ============================================================================


def test_agrupar_por_legajo(conceptos_processor: ConceptosProcessor):
    """Prueba agrupación y pivotado por legajo."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 1, 2],
            "campo_sicoss": ["ImporteSAC", "ImporteHorasExtras", "ImporteSAC"],
            "valor": [1000.0, 500.0, 2000.0],
        }
    )

    result = conceptos_processor._agrupar_por_legajo(df)

    assert len(result) == 2
    assert "nro_legaj" in result.columns
    assert "ImporteSAC" in result.columns
    assert "ImporteHorasExtras" in result.columns

    legajo1 = result[result["nro_legaj"] == 1].iloc[0]
    assert legajo1["ImporteSAC"] == 1000.0
    assert legajo1["ImporteHorasExtras"] == 500.0


def test_agrupar_por_legajo_suma_duplicados(conceptos_processor: ConceptosProcessor):
    """Prueba que suma valores duplicados del mismo campo."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 1],
            "campo_sicoss": ["ImporteSAC", "ImporteSAC"],
            "valor": [1000.0, 500.0],
        }
    )

    result = conceptos_processor._agrupar_por_legajo(df)

    assert len(result) == 1
    assert result.iloc[0]["ImporteSAC"] == 1500.0


def test_agrupar_por_legajo_vacio(conceptos_processor: ConceptosProcessor):
    """Prueba agrupación con DataFrame vacío."""
    df = pd.DataFrame(columns=["nro_legaj", "campo_sicoss", "valor"])

    result = conceptos_processor._agrupar_por_legajo(df)

    assert result.empty
    assert "nro_legaj" in result.columns


# ============================================================================
# TESTS PARA _inicializar_columnas_sicoss
# ============================================================================


def test_inicializar_columnas_sicoss(conceptos_processor: ConceptosProcessor):
    """Prueba inicialización de columnas SICOSS."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "nombre": ["Juan", "María"],
        }
    )

    result = conceptos_processor._inicializar_columnas_sicoss(df_legajos)

    assert "ImporteSAC" in result.columns
    assert "ImporteHorasExtras" in result.columns
    assert all(result["ImporteSAC"] == 0.0)
    assert all(result["ImporteHorasExtras"] == 0.0)
    assert len(result) == 2


# ============================================================================
# TESTS PARA _llenar_valores_faltantes
# ============================================================================


def test_llenar_valores_faltantes(conceptos_processor: ConceptosProcessor):
    """Prueba llenado de valores faltantes con ceros."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "ImporteSAC": [1000.0, None],
            "ImporteHorasExtras": [None, 500.0],
        }
    )

    result = conceptos_processor._llenar_valores_faltantes(df)

    assert pd.notna(result.iloc[0]["ImporteSAC"])
    assert pd.notna(result.iloc[1]["ImporteSAC"])
    assert result.iloc[1]["ImporteSAC"] == 0.0
    assert result.iloc[0]["ImporteHorasExtras"] == 0.0


# ============================================================================
# TESTS PARA _consolidar_campos_calculados
# ============================================================================


def test_consolidar_campos_calculados(conceptos_processor: ConceptosProcessor):
    """Prueba consolidación de campos calculados."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "ImporteSAC": [10000.0],
            "ImporteHorasExtras": [5000.0],
            "ImporteZonaDesfavorable": [2000.0],
            "ImporteVacaciones": [3000.0],
            "ImportePremios": [1000.0],
            "ImporteAdicionales": [500.0],
            "ImporteNoRemun": [2000.0],
            "codigoactividad": [0],
            "PrioridadTipoDeActividad": [0],
        }
    )

    result = conceptos_processor._consolidar_campos_calculados(df)

    # Remuner78805 debe ser la suma de conceptos remunerativos
    assert result.iloc[0]["Remuner78805"] == 21500.0  # 10000+5000+2000+3000+1000+500
    assert result.iloc[0]["ImporteImponiblePatronal"] == 21500.0
    assert result.iloc[0]["IMPORTE_BRUTO"] == 23500.0  # 21500 + 2000
    assert result.iloc[0]["IMPORTE_IMPON"] == 21500.0


def test_consolidar_campos_calculados_investigador(
    conceptos_processor: ConceptosProcessor,
):
    """Prueba consolidación con investigador (ajuste de SAC)."""
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "ImporteSAC": [10000.0],
            "SACInvestigador": [2000.0],
            "ImporteHorasExtras": [5000.0],
            "codigoactividad": [0],
            "PrioridadTipoDeActividad": [0],
        }
    )

    result = conceptos_processor._consolidar_campos_calculados(df)

    # SAC debe ajustarse restando SACInvestigador
    assert result.iloc[0]["ImporteSAC"] == 8000.0  # 10000 - 2000


def test_consolidar_campos_calculados_tipo_actividad(
    conceptos_processor: ConceptosProcessor,
):
    """Prueba cálculo de TipoDeActividad."""
    # Caso 1: Prioridad 38 o 0 → usar codigoactividad
    df = pd.DataFrame(
        {
            "nro_legaj": [1],
            "codigoactividad": [5],
            "PrioridadTipoDeActividad": [38],
            "ImporteSAC": [0],
        }
    )

    result = conceptos_processor._consolidar_campos_calculados(df)
    assert result.iloc[0]["TipoDeActividad"] == 5

    # Caso 2: Prioridad 34-37, 87, 88 → usar prioridad
    df2 = pd.DataFrame(
        {
            "nro_legaj": [2],
            "codigoactividad": [5],
            "PrioridadTipoDeActividad": [34],
            "ImporteSAC": [0],
        }
    )

    result2 = conceptos_processor._consolidar_campos_calculados(df2)
    assert result2.iloc[0]["TipoDeActividad"] == 34


# ============================================================================
# TESTS INTEGRADOS - FLUJO COMPLETO
# ============================================================================


def test_process_completo_multiple_legajos(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento completo con múltiples legajos."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "codigoactividad": [0, 0],
        }
    )

    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1, 1, 2],
            "codn_conce": [10, 20, 30],
            "impp_conce": [1000.0, 500.0, 2000.0],
            "tipo_conce": ["R", "R", "N"],
            "tipos_grupos": [[9], [6], [45]],  # SAC, Horas Extras, No Remun
            "codigoescalafon": ["01", "01", "01"],
        }
    )

    result = conceptos_processor.process(df_legajos, df_conceptos)

    assert len(result) == 2
    legajo1 = result[result["nro_legaj"] == 1].iloc[0]
    legajo2 = result[result["nro_legaj"] == 2].iloc[0]

    assert legajo1["ImporteSAC"] == 1000.0
    assert legajo1["ImporteHorasExtras"] == 500.0
    assert legajo2["ImporteNoRemun"] == 2000.0


def test_process_completo_todos_tipos(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento con todos los tipos de conceptos."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "codigoactividad": [0],
        }
    )

    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1] * 10,
            "codn_conce": list(range(10, 20)),
            "impp_conce": [1000.0] * 10,
            "tipo_conce": ["R"] * 10,
            "tipos_grupos": [
                [9],  # SAC
                [6],  # Horas Extras
                [7],  # Zona Desfavorable
                [8],  # Vacaciones
                [21],  # Adicionales
                [22],  # Premios
                [45],  # No Remun
                [47],  # Maternidad
                [58],  # Seguro Vida
                [11],  # Investigador
            ],
            "codigoescalafon": ["01"] * 10,
        }
    )

    result = conceptos_processor.process(df_legajos, df_conceptos)

    assert len(result) == 1
    legajo = result.iloc[0]

    assert legajo["ImporteSAC"] == 1000.0
    assert legajo["ImporteHorasExtras"] == 1000.0
    assert legajo["ImporteZonaDesfavorable"] == 1000.0
    assert legajo["ImporteVacaciones"] == 1000.0
    assert legajo["ImporteAdicionales"] == 1000.0
    assert legajo["ImportePremios"] == 1000.0
    assert legajo["ImporteNoRemun"] == 1000.0
    assert legajo["ImporteMaternidad"] == 1000.0
    assert legajo["SeguroVidaObligatorio"] == 1
    assert legajo["ImporteImponible_6"] == 1000.0


def test_process_completo_sac_escalafones(conceptos_processor: ConceptosProcessor):
    """Prueba procesamiento de SAC con diferentes escalafones."""
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1, 2, 3],
            "codigoactividad": [0, 0, 0],
        }
    )

    df_conceptos = pd.DataFrame(
        {
            "nro_legaj": [1, 2, 3],
            "codn_conce": [10, 20, 30],
            "impp_conce": [50000.0, 60000.0, 70000.0],
            "tipo_conce": ["R", "R", "R"],
            "tipos_grupos": [[9], [9], [9]],
            "codigoescalafon": ["DOCE", "AUTO", "NODO"],
        }
    )

    result = conceptos_processor.process(df_legajos, df_conceptos)

    assert len(result) == 3
    assert result[result["nro_legaj"] == 1].iloc[0]["ImporteSACDoce"] == 50000.0
    assert result[result["nro_legaj"] == 2].iloc[0]["ImporteSACAuto"] == 60000.0
    assert result[result["nro_legaj"] == 3].iloc[0]["ImporteSACNodo"] == 70000.0
