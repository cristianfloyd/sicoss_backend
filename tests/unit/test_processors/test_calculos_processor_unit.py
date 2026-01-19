import pandas as pd
import pytest
from config.sicoss_config import SicossConfig
from processors.calculos_processor import CalculosSicossProcessor

@pytest.fixture
def sicoss_config():
    return SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
        asignacion_familiar=True,
    )

@pytest.fixture
def calculos_processor(sicoss_config):
    return CalculosSicossProcessor(sicoss_config)

def test_calculos_processor_imponible_4_6(calculos_processor):
    """Prueba el cálculo de ImporteImponible_4 y _6."""
    df_legajos = pd.DataFrame({
        "nro_legaj": [1, 2],
        "IMPORTE_IMPON": [10000.0, 20000.0],
        "IMPORTE_BRUTO": [12000.0, 25000.0],
        "ImporteImponiblePatronal": [10000.0, 20000.0],
        "ImporteSACPatronal": [1000.0, 2000.0],
        "Remuner78805": [9000.0, 18000.0],
        "ImporteImponible_6": [0.0, 5000.0],
        "SACInvestigador": [0.0, 500.0],
        "ImporteSAC": [1000.0, 2000.0]
    })
    
    result = calculos_processor.process(df_legajos)
    
    # ImporteImponible_4 debe ser igual a IMPORTE_IMPON inicial
    assert result.iloc[0]["ImporteImponible_4"] == 10000.0
    
    # Caso 2 tiene ImporteImponible_6 > 0
    # Como porc_aporte_adicional_jubilacion es 100.0, no debería cambiar mucho pero TipoDeOperacion puede cambiar
    assert "TipoDeOperacion" in result.columns
    assert result.iloc[1]["ImporteImponible_6"] == 5000.0

def test_calculos_processor_campos_base_emergencia(calculos_processor):
    """Prueba la creación de campos base si no existen."""
    df_legajos = pd.DataFrame({
        "nro_legaj": [1]
    })
    
    # Esto activará _aplicar_campos_base_emergencia ya que faltan campos base
    result = calculos_processor.process(df_legajos)
    
    assert "IMPORTE_BRUTO" in result.columns
    assert result.iloc[0]["IMPORTE_BRUTO"] == 0.0
    assert "ImporteSAC" in result.columns
