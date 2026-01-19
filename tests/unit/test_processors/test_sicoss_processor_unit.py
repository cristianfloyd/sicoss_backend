import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
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
    with patch("processors.sicoss_processor.SicossDataProcessor._initialize_processors"):
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
        "ImporteImponible_6"
    ]
    df_legajos = pd.DataFrame({col: [0.0] if col != "nro_legaj" else [1] for col in columns})
    
    data = {
        "legajos": df_legajos,
        "conceptos": pd.DataFrame({"nro_legaj": [1]}),
        "otra_actividad": pd.DataFrame({"nro_legaj": [1]}),
        "obra_social": pd.DataFrame({"nro_legaj": [1]})
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
        "obra_social": pd.DataFrame({"nro_legaj": [1]})
    }
    
    # Forzar error en el primer paso crítico
    processor.conceptos_processor.process.side_effect = Exception("Error crítico")
    
    resultado = processor.procesar_datos_extraidos(data, validate_input=False)
    
    # Debe retornar un resultado de emergencia con el error
    assert resultado["estadisticas"]["error"] == "Error en 'Sumarización de conceptos': Error crítico"

def test_sicoss_processor_validate_input(processor: SicossDataProcessor):
    """Prueba la validación de entrada."""
    data_incompleta = {"legajos": pd.DataFrame()}
    
    resultado = processor.procesar_datos_extraidos(data_incompleta, validate_input=True)
    
    # Debe retornar un resultado de emergencia por falta de claves
    assert "falta 'conceptos'" in resultado["estadisticas"]["error"]
