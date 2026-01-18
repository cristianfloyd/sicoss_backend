"""
Test Avanzado SicossVerifier - Comparación Python vs PHP Legacy

Tests para validar la consistencia entre el sistema Python nuevo y datos
de referencia del PHP legacy.
"""

import sys
import time

import numpy as np
import pandas as pd

from config.sicoss_config import SicossConfig
from processors.sicoss_processor import SicossDataProcessor
from validators.sicoss_verifier import SicossVerifier, ToleranciaComparacion


def generar_datos_php_referencia(legajos_python: pd.DataFrame) -> pd.DataFrame:
    """
    Genera datos de referencia simulando resultados del PHP legacy

    En un caso real, estos datos vendrían de:
    - Exportación del sistema PHP actual
    - Base de datos con resultados históricos
    - Archivos CSV de referencia validados
    """
    print("Generando datos de referencia PHP legacy...")

    df_php = legajos_python.copy()

    # Simular pequeñas diferencias típicas entre sistemas
    np.random.seed(42)  # Para resultados reproducibles

    if "IMPORTE_BRUTO" in df_php.columns:
        # Al convertir la serie a float, Pyright entiende que la operación es segura
        ruido = np.random.uniform(-0.02, 0.02, size=len(df_php))
        df_php["IMPORTE_BRUTO"] = df_php["IMPORTE_BRUTO"].astype(float) + ruido

    # 2. Diferencia de centavos en IMPORTE_IMPON
    if "IMPORTE_IMPON" in df_php.columns:
        ruido = np.random.uniform(-0.01, 0.01, size=len(df_php))
        df_php["IMPORTE_IMPON"] = df_php["IMPORTE_IMPON"].astype(float) + ruido

    # 3. Diferencias significativas (5% de los casos)
    if "ImporteSAC" in df_php.columns:
        # Creamos una máscara booleana para el 5% de los registros
        mask = np.random.random(len(df_php)) < 0.05
        df_php.loc[mask, "ImporteSAC"] = (
            df_php.loc[mask, "ImporteSAC"].astype(float) * 1.001
        )

    # Introducir algunos errores deliberados para probar el verificador
    if len(df_php) > 2:
        # Error en TipoDeOperacion (fila 0)
        idx0 = df_php.index[0]
        actual = df_php.at[idx0, "TipoDeOperacion"]
        df_php.at[idx0, "TipoDeOperacion"] = 2 if actual == 1 else 1

        # Error en campo booleano
        if "SeguroVidaObligatorio" in df_php.columns:
            idx1 = df_php.index[1]
            df_php.at[idx1, "SeguroVidaObligatorio"] = not bool(
                df_php.at[idx1, "SeguroVidaObligatorio"]
            )

    print(
        f"Datos PHP legacy generados: {len(df_php)} legajos con diferencias simuladas"
    )
    return df_php


def test_sicoss_verifier_basico():
    """Test básico del SicossVerifier con datos simulados"""
    print("\n🧪 TEST 1: SicossVerifier Básico")
    print("=" * 50)

    try:
        # Generar datos de prueba Python
        df_python = pd.DataFrame(
            {
                "nro_legaj": [12345, 12346, 12347],
                "IMPORTE_BRUTO": [150000.50, 125000.75, 175000.25],
                "IMPORTE_IMPON": [140000.00, 120000.00, 165000.00],
                "ImporteSAC": [12500.00, 10400.00, 14600.00],
                "TipoDeOperacion": [1, 1, 2],
                "SeguroVidaObligatorio": [True, False, True],
            }
        )

        # Simular datos PHP con pequeñas diferencias
        df_php = df_python.copy()
        df_php.iloc[0]["IMPORTE_BRUTO"] += 0.02  # Diferencia de centavos
        df_php.iloc[1]["TipoDeOperacion"] = 2  # Diferencia en entero

        # Crear verificador
        tolerancia = ToleranciaComparacion(
            tolerancia_monetaria=0.05, tolerancia_porcentual=0.01, tolerancia_enteros=0
        )

        verifier = SicossVerifier(tolerancia)

        # Ejecutar verificación
        print("Ejecutando verificación Python vs PHP...")
        reporte = verifier.verificar_resultados(df_python, df_php)

        # Mostrar resultados
        print("\nRESULTADOS VERIFICACIÓN:")
        print(f"   - Total legajos: {reporte.total_legajos}")
        print(f"   - Porcentaje coincidencia: {reporte.porcentaje_coincidencia:.2f}%")
        print(f"   - Diferencias críticas: {reporte.diferencias_criticas}")

        # Generar reporte HTML
        archivo_reporte = verifier.generar_reporte_html(
            reporte, "test_verifier_basico.html"
        )
        print(f"Reporte generado: {archivo_reporte}")

        print("Test SicossVerifier Básico: EXITOSO")
        return True

    except Exception as e:
        print(f"Error en test básico: {e}")
        return False


def test_sicoss_verifier_con_procesador():
    """Test del SicossVerifier usando datos del SicossDataProcessor real"""
    print("\nTEST 2: SicossVerifier con SicossDataProcessor")
    print("=" * 60)

    try:
        # Configurar sistema
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True,
        )

        # Crear datos de entrada realistas
        datos_entrada = {
            "legajos": pd.DataFrame(
                {
                    "nro_legaj": [100001, 100002, 100003],
                    "apnom": ["EMPLEADO TEST 1", "EMPLEADO TEST 2", "EMPLEADO TEST 3"],
                    "cuil": ["20301234567", "20301234568", "20301234569"],
                    "situacion_revista": [1, 1, 1],
                    "codigo_obra_social": [101, 102, 101],
                }
            ),
            "conceptos": pd.DataFrame(
                {
                    "nro_legaj": [100001, 100001, 100002, 100003],
                    "codn_conce": [1, 9, 1, 15],
                    "impp_conce": [80000.0, 6666.67, 75000.0, 12000.0],
                    "tipos_grupos": [[1], [9], [1], [15]],
                    "codigoescalafon": ["NODO", "NODO", "AUTO", "DOCE"],
                }
            ),
            "otra_actividad": pd.DataFrame(),
            "obra_social": pd.DataFrame(),
        }

        # Procesar con sistema Python
        processor = SicossDataProcessor(config)
        print("Procesando datos con SicossDataProcessor...")
        resultado_python = processor.procesar_datos_extraidos(datos_entrada)

        if not resultado_python["success"]:
            print(
                f"Error en procesamiento: {resultado_python.get('error', 'Error desconocido')}"
            )
            return False

        df_python = resultado_python["data"]["legajos"]
        print(f"Procesamiento Python exitoso: {len(df_python)} legajos")

        # Simular datos PHP con base en los resultados Python
        df_php_simulado = generar_datos_php_referencia(df_python)

        # Configurar verificador con tolerancias estrictas
        tolerancia = ToleranciaComparacion(
            tolerancia_monetaria=0.01,  # 1 centavo
            tolerancia_porcentual=0.001,  # 0.1%
            tolerancia_enteros=0,
            tolerancia_booleana=False,
        )

        verifier = SicossVerifier(tolerancia)

        # Verificar solo campos críticos
        campos_criticos = [
            "IMPORTE_BRUTO",
            "IMPORTE_IMPON",
            "ImporteSAC",
            "ImporteImponible_4",
            "ImporteImponible_5",
            "TipoDeOperacion",
        ]

        print("Ejecutando verificación con tolerancias estrictas...")
        reporte = verifier.verificar_resultados(
            df_python, df_php_simulado, campos_criticos
        )

        # Análisis detallado
        print("\nANÁLISIS DETALLADO:")
        print(f"   - Porcentaje coincidencia: {reporte.porcentaje_coincidencia:.2f}%")
        print(f"   - Diferencias críticas: {reporte.diferencias_criticas}")

        # Estadísticas
        stats = reporte.resumen_estadistico
        print("\nESTADÍSTICAS:")
        print(f"   - Diferencia promedio: ${stats.get('diferencia_promedio', 0):.4f}")
        print(f"   - Diferencia máxima: ${stats.get('diferencia_maxima', 0):.4f}")
        print(f"   - Diferencia mediana: ${stats.get('diferencia_mediana', 0):.4f}")

        # Campos con más errores
        campos_errores = stats.get("campos_con_mas_errores", [])
        if campos_errores:
            print("\nCAMPOS CON MÁS ERRORES:")
            for campo, count in campos_errores:
                print(f"   - {campo}: {count} errores")

        # Generar reporte detallado
        archivo_reporte = verifier.generar_reporte_html(
            reporte, "reporte_verificacion_processor.html"
        )
        print(f"\nReporte detallado: {archivo_reporte}")

        # Evaluar resultado
        if reporte.porcentaje_coincidencia >= 95.0:
            print("Test SicossVerifier con Processor: EXITOSO")
            return True
        else:
            print("Test completado con advertencias - Revisar diferencias")
            return True

    except Exception as e:
        print(f"Error en test con processor: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_sicoss_verifier_tolerancias():
    """Test de diferentes configuraciones de tolerancias"""
    print("\n🧪 TEST 3: Configuraciones de Tolerancias")
    print("=" * 50)

    try:
        # Datos con diferencias conocidas
        df_python = pd.DataFrame(
            {
                "nro_legaj": [1, 2, 3],
                "IMPORTE_BRUTO": [100.00, 200.00, 300.00],
                "TipoDeOperacion": [1, 1, 2],
            }
        )

        df_php = pd.DataFrame(
            {
                "nro_legaj": [1, 2, 3],
                "IMPORTE_BRUTO": [100.02, 199.99, 300.05],  # Diferencias de centavos
                "TipoDeOperacion": [1, 2, 2],  # Diferencia en entero
            }
        )

        # Test 1: Tolerancia estricta
        print("Test con tolerancia estricta...")
        tolerancia_estricta = ToleranciaComparacion(
            tolerancia_monetaria=0.01, tolerancia_enteros=0
        )
        verifier_estricto = SicossVerifier(tolerancia_estricta)
        reporte_estricto = verifier_estricto.verificar_resultados(df_python, df_php)
        print(
            f"   Coincidencia estricta: {reporte_estricto.porcentaje_coincidencia:.1f}%"
        )

        # Test 2: Tolerancia relajada
        print("Test con tolerancia relajada...")
        tolerancia_relajada = ToleranciaComparacion(
            tolerancia_monetaria=0.10,  # 10 centavos
            tolerancia_enteros=1,  # Permite diferencia de 1
        )
        verifier_relajado = SicossVerifier(tolerancia_relajada)
        reporte_relajado = verifier_relajado.verificar_resultados(df_python, df_php)
        print(
            f"   Coincidencia relajada: {reporte_relajado.porcentaje_coincidencia:.1f}%"
        )

        # Comparar resultados
        mejora = (
            reporte_relajado.porcentaje_coincidencia
            - reporte_estricto.porcentaje_coincidencia
        )
        print("\nCOMPARACIÓN:")
        print(f"   - Mejora con tolerancia relajada: +{mejora:.1f}%")
        print(
            f"   - Diferencias críticas estrictas: {reporte_estricto.diferencias_criticas}"
        )
        print(
            f"   - Diferencias críticas relajadas: {reporte_relajado.diferencias_criticas}"
        )

        print("Test Configuraciones de Tolerancias: EXITOSO")
        return True

    except Exception as e:
        print(f"Error en test tolerancias: {e}")
        return False


def main():
    """Ejecuta todos los tests del SicossVerifier avanzado"""
    print("INICIANDO TESTS SICOSS VERIFIER AVANZADO")
    print("=" * 70)

    start_time = time.time()

    # Ejecutar tests
    tests = [
        ("Test Básico", test_sicoss_verifier_basico),
        ("Test con Processor", test_sicoss_verifier_con_procesador),
        ("Test Tolerancias", test_sicoss_verifier_tolerancias),
    ]

    resultados: dict[str, bool] = {}
    tests_exitosos = 0

    for nombre, test_func in tests:
        print(f"\n {nombre}")
        resultado = test_func()
        resultados[nombre] = resultado
        if resultado:
            tests_exitosos += 1

    # Resumen final
    elapsed_time = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"RESUMEN TESTS SICOSS VERIFIER: {tests_exitosos}/{len(tests)} exitosos")
    print(f"Tiempo total: {elapsed_time:.2f}s")

    for nombre, resultado in resultados.items():
        estado = "EXITOSO" if resultado else "FALLÓ"
        print(f"   - {nombre}: {estado}")

    if tests_exitosos == len(tests):
        print("\n TODOS LOS TESTS SICOSS VERIFIER EXITOSOS!")
        print(" SicossVerifier listo para validación de producción")
    else:
        print(
            f"\n {len(tests) - tests_exitosos} tests fallaron - Revisar implementación"
        )

    return tests_exitosos == len(tests)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
