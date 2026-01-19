import pandas as pd
import pytest
from config.sicoss_config import SicossConfig
from processors.conceptos_processor import ConceptosProcessor

@pytest.fixture
def sicoss_config():
    return SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
    )

@pytest.fixture
def conceptos_processor(sicoss_config: SicossConfig):
    return ConceptosProcessor(sicoss_config)

def test_conceptos_processor_basic_sum(conceptos_processor: ConceptosProcessor):
    """Prueba la sumarización básica de conceptos por tipo."""
    df_legajos = pd.DataFrame({
        "nro_legaj": [123],
        "sexo": ["M"],
        "f_nacim": ["1980-01-01"]
    })
    
    df_conceptos = pd.DataFrame({
        "nro_legaj": [123, 123, 123],
        "codn_conce": [1, 2, 3],
        "impp_conce": [1000.0, 500.0, 200.0],
        "tipo_conce": ["R", "R", "N"],  # Remunerativo, No Remunerativo
        "tipos_grupos": [[9], [4], [45]],  # 9: SAC, 4: Básico/Bruto, 45: No Remun
        "codigoescalafon": ["01", "01", "01"],
        "codigoactividad": [0, 0, 0]
    })
    
    result = conceptos_processor.process(df_legajos, df_conceptos)
    
    assert len(result) == 1
    assert result.iloc[0]["ImporteSAC"] == 1000.0
    assert result.iloc[0]["ImporteNoRemun"] == 200.0

def test_conceptos_processor_multiple_groups(conceptos_processor: ConceptosProcessor):
    """Prueba un concepto que pertenece a múltiples grupos SICOSS."""
    df_legajos = pd.DataFrame({"nro_legaj": [1]})
    df_conceptos = pd.DataFrame({
        "nro_legaj": [1],
        "codn_conce": [100],
        "impp_conce": [5000.0],
        "tipo_conce": ["R"],
        "tipos_grupos": [[9, 6]], # 9: SAC, 6: Horas Extras
        "codigoescalafon": ["01"],
        "codigoactividad": [0]
    })
    
    result = conceptos_processor.process(df_legajos, df_conceptos)
    
    assert result.iloc[0]["ImporteSAC"] == 5000.0
    assert result.iloc[0]["ImporteHorasExtras"] == 5000.0

def test_conceptos_processor_empty_input(conceptos_processor: ConceptosProcessor):
    """Prueba el comportamiento con DataFrames vacíos."""
    df_legajos = pd.DataFrame(columns=["nro_legaj"])
    df_conceptos = pd.DataFrame(columns=["nro_legaj", "impp_conce", "tipos_grupos"])
    
    result = conceptos_processor.process(df_legajos, df_conceptos)
    assert result.empty
