import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from config.sicoss_config import SicossConfig
from processors.topes_processor import TopesProcessor

@pytest.fixture
def sicoss_config():
    return SicossConfig(
        tope_jubilatorio_patronal=100000.0,
        tope_jubilatorio_personal=100000.0,
        tope_otros_aportes_personales=100000.0,
        trunca_tope=True
    )

@pytest.fixture
def topes_processor(sicoss_config: SicossConfig):
    return TopesProcessor(sicoss_config)

def test_topes_patronales(topes_processor: TopesProcessor):
    """Prueba la aplicación de topes patronales."""
    df = pd.DataFrame({
        "nro_legaj": [1],
        "ImporteSAC": [60000.0],  # Excede tope SAC (50k)
        "ImporteImponibleSinSAC": [120000.0],  # Excede tope imponible (100k)
        "ImporteImponiblePatronal": [180000.0],
    })
    
    result = topes_processor._aplicar_topes_patronales(df)
    
    # Diferencia SAC: 60k - 50k = 10k
    assert result.iloc[0]["DiferenciaSACImponibleConTope"] == 10000.0
    assert result.iloc[0]["ImporteSACPatronal"] == 50000.0
    
    # Diferencia Imponible: 120k - 100k = 20k
    assert result.iloc[0]["DiferenciaImponibleConTope"] == 20000.0
    
    # ImporteImponiblePatronal final: 180k - 10k - 20k = 150k
    # Espera... la lógica en _aplicar_topes_patronales es:
    # df.loc[mask_excede_sac, "ImporteImponiblePatronal"] -= df.loc[mask_excede_sac, "DiferenciaSACImponibleConTope"]
    # df.loc[mask_excede_imponible, "ImporteImponiblePatronal"] -= df.loc[mask_excede_imponible, "DiferenciaImponibleConTope"]
    assert result.iloc[0]["ImporteImponiblePatronal"] == 150000.0

@patch("processors.topes_processor.TopesProcessor._obtener_legajos_diferenciales_bulk")
@patch("processors.topes_processor.TopesProcessor._obtener_categorias_diferenciales")
def test_categorias_diferenciales(mock_obtener_cats, mock_bulk, topes_processor: TopesProcessor):
    """Prueba la aplicación de categorías diferenciales."""
    mock_obtener_cats.return_value = ["DIF1"]
    mock_bulk.return_value = [123]  # Solo legajo 123 es diferencial
    
    df = pd.DataFrame({
        "nro_legaj": [123, 456],
        "IMPORTE_IMPON": [50000.0, 50000.0]
    })
    
    result = topes_processor._aplicar_categorias_diferenciales(df)
    
    # Legajo 123 debe tener IMPORTE_IMPON = 0
    assert result.loc[result["nro_legaj"] == 123, "IMPORTE_IMPON"].values[0] == 0.0
    # Legajo 456 debe mantener su importe
    assert result.loc[result["nro_legaj"] == 456, "IMPORTE_IMPON"].values[0] == 50000.0

def test_truncar_tope_false(sicoss_config: SicossConfig):
    """Prueba que si trunca_tope es False, no se aplican cambios."""
    sicoss_config.trunca_tope = False
    processor = TopesProcessor(sicoss_config)
    
    df = pd.DataFrame({
        "nro_legaj": [1],
        "ImporteSAC": [200000.0],
        "ImporteImponiblePatronal": [200000.0]
    })
    
    result = processor.process(df)
    
    # No debería haber cambiado nada
    assert result.iloc[0]["ImporteImponiblePatronal"] == 200000.0
    assert "DiferenciaSACImponibleConTope" not in result.columns
