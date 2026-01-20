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
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "IMPORTE_IMPON": [10000.0, 20000.0],
            "IMPORTE_BRUTO": [12000.0, 25000.0],
            "ImporteImponiblePatronal": [10000.0, 20000.0],
            "ImporteSACPatronal": [1000.0, 2000.0],
            "Remuner78805": [9000.0, 18000.0],
            "ImporteImponible_6": [0.0, 5000.0],
            "SACInvestigador": [0.0, 500.0],
            "ImporteSAC": [1000.0, 2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # ImporteImponible_4 debe ser igual a IMPORTE_IMPON inicial
    assert result.iloc[0]["ImporteImponible_4"] == 10000.0

    # Caso 2 tiene ImporteImponible_6 > 0
    # Como porc_aporte_adicional_jubilacion es 100.0, no debería cambiar mucho pero TipoDeOperacion puede cambiar
    assert "TipoDeOperacion" in result.columns
    assert result.iloc[1]["ImporteImponible_6"] == 5000.0


def test_calculos_processor_campos_base_emergencia(calculos_processor):
    """Prueba la creación de campos base si no existen."""
    df_legajos = pd.DataFrame({"nro_legaj": [1]})

    # Esto activará _aplicar_campos_base_emergencia ya que faltan campos base
    result = calculos_processor.process(df_legajos)

    assert "IMPORTE_BRUTO" in result.columns
    assert result.iloc[0]["IMPORTE_BRUTO"] == 0.0
    assert "ImporteSAC" in result.columns


# ============================================================================
# TESTS CASOS DE BORDE - ImporteImponible_6
# ============================================================================


def test_importe_imponible_6_tipo_operacion_2_diferencia_mayor_5(calculos_processor):
    """
    Caso de borde: ImporteImponible_6 con diferencia > 5 y menor que IMPORTE_IMPON
    Debe cambiar TipoDeOperacion a 2 y ajustar IMPORTE_IMPON
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [
                10000.0
            ],  # Diferencia de 10000 > 5 y < IMPORTE_IMPON
            "SACInvestigador": [1000.0],
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # Debe cambiar TipoDeOperacion a 2
    assert result.iloc[0]["TipoDeOperacion"] == 2

    # IMPORTE_IMPON debe ajustarse: 20000 - 10000 = 10000
    # (ImporteImponible_6 se ajusta primero por porcentaje, pero con 100% queda igual)
    # ImporteImponible_6 = 10000 * 100 / 100 = 10000
    # IMPORTE_IMPON = 20000 - 10000 = 10000
    assert result.iloc[0]["IMPORTE_IMPON"] == 10000.0

    # ImporteSACNoDocente = ImporteSAC - SACInvestigador = 2000 - 1000 = 1000
    assert result.iloc[0]["ImporteSACNoDocente"] == 1000.0


def test_importe_imponible_6_tolerancia_menor_igual_5(calculos_processor):
    """
    Caso de borde: ImporteImponible_6 con diferencia <= 5
    Debe ajustar ImporteImponible_6 a IMPORTE_IMPON (no cambia TipoDeOperacion)
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [19998.0],  # Diferencia de 2 <= 5
            "SACInvestigador": [0.0],
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # TipoDeOperacion debe permanecer en 1
    assert result.iloc[0]["TipoDeOperacion"] == 1

    # ImporteImponible_6 debe ajustarse a IMPORTE_IMPON
    # Primero se ajusta por porcentaje: 19998 * 100 / 100 = 19998
    # Luego como diferencia <= 5, se ajusta a IMPORTE_IMPON = 20000
    assert result.iloc[0]["ImporteImponible_6"] == 20000.0

    # IMPORTE_IMPON no debe cambiar
    assert result.iloc[0]["IMPORTE_IMPON"] == 20000.0


def test_importe_imponible_6_diferencia_mayor_5_pero_mayor_que_impon(
    calculos_processor,
):
    """
    Caso de borde: ImporteImponible_6 con diferencia > 5 pero mayor que IMPORTE_IMPON
    NO debe cambiar TipoDeOperacion a 2 (solo cambia si es menor)
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [30000.0],  # Mayor que IMPORTE_IMPON
            "SACInvestigador": [0.0],
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # TipoDeOperacion debe permanecer en 1 (no cambia porque ImporteImponible_6 > IMPORTE_IMPON)
    assert result.iloc[0]["TipoDeOperacion"] == 1

    # IMPORTE_IMPON no debe cambiar
    assert result.iloc[0]["IMPORTE_IMPON"] == 20000.0


def test_importe_imponible_6_porcentaje_diferencial_50(calculos_processor):
    """
    Caso de borde: ImporteImponible_6 con porcentaje diferencial != 100%
    """
    # Crear config con porcentaje diferente
    config = SicossConfig(
        tope_jubilatorio_patronal=3245240.49,
        tope_jubilatorio_personal=3245240.49,
        tope_otros_aportes_personales=3245240.49,
        trunca_tope=True,
    )
    config.porc_aporte_adicional_jubilacion = 50.0  # 50% en lugar de 100%

    processor = CalculosSicossProcessor(config)

    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [10000.0],  # Con 50%, esto se convierte en 20000
            "SACInvestigador": [0.0],
            "ImporteSAC": [2000.0],
        }
    )

    result = processor.process(df_legajos)

    # ImporteImponible_6 debe ajustarse: 10000 * 100 / 50 = 20000
    assert result.iloc[0]["ImporteImponible_6"] == 20000.0

    # Como ahora es igual a IMPORTE_IMPON (diferencia = 0 <= 5), debe ajustarse
    # y TipoDeOperacion permanece en 1
    assert result.iloc[0]["TipoDeOperacion"] == 1


def test_importe_imponible_6_cero_no_procesa(calculos_processor):
    """
    Caso de borde: ImporteImponible_6 = 0 no debe procesarse
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [0.0],  # Cero, no debe procesarse
            "SACInvestigador": [0.0],
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # TipoDeOperacion debe permanecer en 1
    assert result.iloc[0]["TipoDeOperacion"] == 1

    # ImporteImponible_6 debe permanecer en 0
    assert result.iloc[0]["ImporteImponible_6"] == 0.0

    # IMPORTE_IMPON no debe cambiar
    assert result.iloc[0]["IMPORTE_IMPON"] == 20000.0


def test_importe_imponible_6_ajuste_impon_cuando_tipo_2(calculos_processor):
    """
    Caso de borde: Verificar que IMPORTE_IMPON se ajusta correctamente cuando TipoDeOperacion = 2
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [50000.0],
            "IMPORTE_BRUTO": [55000.0],
            "ImporteImponiblePatronal": [50000.0],
            "ImporteSACPatronal": [5000.0],
            "Remuner78805": [45000.0],
            "ImporteImponible_6": [
                20000.0
            ],  # Diferencia de 30000 > 5 y < IMPORTE_IMPON
            "SACInvestigador": [2000.0],
            "ImporteSAC": [5000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # Debe cambiar TipoDeOperacion a 2
    assert result.iloc[0]["TipoDeOperacion"] == 2

    # IMPORTE_IMPON debe ajustarse: 50000 - 20000 = 30000
    assert result.iloc[0]["IMPORTE_IMPON"] == 30000.0

    # ImporteImponible_6 debe permanecer en 20000 (después del ajuste por porcentaje)
    assert result.iloc[0]["ImporteImponible_6"] == 20000.0


# ============================================================================
# TESTS CASOS DE BORDE - SAC
# ============================================================================


def test_sac_no_docente_con_sac_investigador(calculos_processor):
    """
    Caso de borde: ImporteSACNoDocente cuando hay SACInvestigador
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [10000.0],  # Para activar TipoDeOperacion = 2
            "SACInvestigador": [800.0],  # Hay SAC de investigador
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # ImporteSACNoDocente = ImporteSAC - SACInvestigador = 2000 - 800 = 1200
    assert result.iloc[0]["ImporteSACNoDocente"] == 1200.0


def test_sac_no_docente_sin_sac_investigador(calculos_processor):
    """
    Caso de borde: ImporteSACNoDocente cuando NO hay SACInvestigador
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [
                0.0
            ],  # Sin ImporteImponible_6, no cambia TipoDeOperacion
            "SACInvestigador": [0.0],  # Sin SAC de investigador
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # ImporteSACNoDocente debe ser igual a ImporteSAC cuando no hay SACInvestigador
    # y no hay TipoDeOperacion = 2
    assert result.iloc[0]["ImporteSACNoDocente"] == 2000.0


def test_sac_no_docente_tipo_2_sin_sac_investigador(calculos_processor):
    """
    Caso de borde: ImporteSACNoDocente cuando TipoDeOperacion = 2 pero sin SACInvestigador
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [10000.0],  # Para activar TipoDeOperacion = 2
            "SACInvestigador": [0.0],  # Sin SAC de investigador
            "ImporteSAC": [2000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # ImporteSACNoDocente = ImporteSAC - SACInvestigador = 2000 - 0 = 2000
    assert result.iloc[0]["ImporteSACNoDocente"] == 2000.0
    assert result.iloc[0]["TipoDeOperacion"] == 2


def test_sac_otro_aporte(calculos_processor):
    """
    Caso de borde: ImporteSACOtroAporte debe ser igual a ImporteSAC
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1],
            "IMPORTE_IMPON": [20000.0],
            "IMPORTE_BRUTO": [25000.0],
            "ImporteImponiblePatronal": [20000.0],
            "ImporteSACPatronal": [2000.0],
            "Remuner78805": [18000.0],
            "ImporteImponible_6": [0.0],
            "SACInvestigador": [0.0],
            "ImporteSAC": [3500.0],  # Valor específico
        }
    )

    result = calculos_processor.process(df_legajos)

    # ImporteSACOtroAporte debe ser igual a ImporteSAC
    assert result.iloc[0]["ImporteSACOtroAporte"] == 3500.0


# ============================================================================
# TESTS INTEGRACIÓN - CASOS COMBINADOS
# ============================================================================


def test_caso_combinado_multiple_legajos(calculos_processor):
    """
    Test de integración: Múltiples legajos con diferentes casos de ImporteImponible_6 y SAC
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1, 2, 3, 4, 5],
            "IMPORTE_IMPON": [20000.0, 30000.0, 40000.0, 50000.0, 60000.0],
            "IMPORTE_BRUTO": [25000.0, 35000.0, 45000.0, 55000.0, 65000.0],
            "ImporteImponiblePatronal": [20000.0, 30000.0, 40000.0, 50000.0, 60000.0],
            "ImporteSACPatronal": [2000.0, 3000.0, 4000.0, 5000.0, 6000.0],
            "Remuner78805": [18000.0, 27000.0, 36000.0, 45000.0, 54000.0],
            "ImporteImponible_6": [
                10000.0,  # Caso 1: diferencia > 5, menor que IMPORTE_IMPON → TipoDeOperacion = 2
                29998.0,  # Caso 2: diferencia = 2 <= 5 → ajusta a IMPORTE_IMPON
                50000.0,  # Caso 3: diferencia > 5 pero mayor que IMPORTE_IMPON → no cambia
                0.0,  # Caso 4: cero → no procesa
                60000.0,  # Caso 5: igual a IMPORTE_IMPON → diferencia = 0 <= 5
            ],
            "SACInvestigador": [1000.0, 0.0, 2000.0, 0.0, 3000.0],
            "ImporteSAC": [2000.0, 3000.0, 4000.0, 5000.0, 6000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # Caso 1: TipoDeOperacion = 2, IMPORTE_IMPON ajustado, ImporteSACNoDocente ajustado
    assert result.iloc[0]["TipoDeOperacion"] == 2
    assert result.iloc[0]["IMPORTE_IMPON"] == 10000.0
    assert result.iloc[0]["ImporteSACNoDocente"] == 1000.0  # 2000 - 1000

    # Caso 2: TipoDeOperacion = 1, ImporteImponible_6 ajustado a IMPORTE_IMPON
    assert result.iloc[1]["TipoDeOperacion"] == 1
    assert result.iloc[1]["ImporteImponible_6"] == 30000.0

    # Caso 3: TipoDeOperacion = 1 (no cambia porque ImporteImponible_6 > IMPORTE_IMPON)
    assert result.iloc[2]["TipoDeOperacion"] == 1
    assert result.iloc[2]["IMPORTE_IMPON"] == 40000.0

    # Caso 4: TipoDeOperacion = 1, ImporteImponible_6 = 0
    assert result.iloc[3]["TipoDeOperacion"] == 1
    assert result.iloc[3]["ImporteImponible_6"] == 0.0

    # Caso 5: TipoDeOperacion = 1, ImporteImponible_6 ajustado a IMPORTE_IMPON
    assert result.iloc[4]["TipoDeOperacion"] == 1
    assert result.iloc[4]["ImporteImponible_6"] == 60000.0

    # Todos deben tener ImporteSACOtroAporte = ImporteSAC
    assert all(result["ImporteSACOtroAporte"] == result["ImporteSAC"])


def test_importe_imponible_4_5_consistencia(calculos_processor):
    """
    Test de integración: Verificar que ImporteImponible_4 y _5 son consistentes
    """
    df_legajos = pd.DataFrame(
        {
            "nro_legaj": [1, 2],
            "IMPORTE_IMPON": [20000.0, 30000.0],
            "IMPORTE_BRUTO": [25000.0, 35000.0],
            "ImporteImponiblePatronal": [20000.0, 30000.0],
            "ImporteSACPatronal": [2000.0, 3000.0],
            "Remuner78805": [18000.0, 27000.0],
            "ImporteImponible_6": [0.0, 0.0],
            "SACInvestigador": [0.0, 0.0],
            "ImporteSAC": [2000.0, 3000.0],
        }
    )

    result = calculos_processor.process(df_legajos)

    # ImporteImponible_4 debe ser igual a IMPORTE_IMPON inicial
    assert all(result["ImporteImponible_4"] == [20000.0, 30000.0])

    # ImporteImponible_5 debe ser igual a ImporteImponible_4
    assert all(result["ImporteImponible_5"] == result["ImporteImponible_4"])
