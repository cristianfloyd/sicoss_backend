import pandas as pd
import pytest
from config.sicoss_config import SicossConfig
from processors.validator import LegajosValidator

@pytest.fixture
def sicoss_config():
    return SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
        check_lic=True
    )

@pytest.fixture
def validator(sicoss_config):
    return LegajosValidator(sicoss_config)

def test_validator_basic_filtering(validator):
    """Prueba el filtrado básico por importes."""
    df = pd.DataFrame({
        "nro_legaj": [1, 2, 3],
        "IMPORTE_BRUTO": [1000.0, 0.0, 0.0],
        "IMPORTE_IMPON": [500.0, 0.0, 0.0],
        "ImporteImponiblePatronal": [500.0, 0.0, 0.0]
    })
    
    result = validator.validate(df)
    
    # Solo el legajo 1 tiene importes > 0
    assert len(result) == 1
    assert result.iloc[0]["nro_legaj"] == 1

def test_validator_special_situations(validator):
    """Prueba que legajos con importes 0 pero situaciones especiales se mantengan."""
    df = pd.DataFrame({
        "nro_legaj": [1, 2, 3],
        "IMPORTE_BRUTO": [0.0, 0.0, 0.0],
        "IMPORTE_IMPON": [0.0, 0.0, 0.0],
        "ImporteImponiblePatronal": [0.0, 0.0, 0.0],
        "codigosituacion": [5, 14, 1]  # 5: Maternidad, 14: Reserva puesto, 1: Activo normal
    })
    
    result = validator.validate(df)
    
    # Legajos 1 (maternidad) y 2 (reserva puesto) deben mantenerse
    assert len(result) == 2
    assert set(result["nro_legaj"].tolist()) == {1, 2}

def test_validator_licencias(validator):
    """Prueba el filtro de licencias."""
    df = pd.DataFrame({
        "nro_legaj": [1, 2],
        "IMPORTE_BRUTO": [0.0, 0.0],
        "IMPORTE_IMPON": [0.0, 0.0],
        "ImporteImponiblePatronal": [0.0, 0.0],
        "licencia": [1, 0]
    })
    
    result = validator.validate(df)
    
    # Solo el legajo 1 tiene licencia == 1
    assert len(result) == 1
    assert result.iloc[0]["nro_legaj"] == 1
