#!/usr/bin/env python3
"""
🧪 EJECUTOR DE PRUEBAS SICOSS
"""

if __name__ == "__main__":
    import logging

    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Ejecutar pruebas
    from SicossProcessorTester import SicossProcessorTester

    tester = SicossProcessorTester()

    print("🧪 === EJECUTANDO SUITE COMPLETA DE PRUEBAS SICOSS ===")

    exito = tester.ejecutar_suite_completa()

    if exito:
        print("🎉 TODAS LAS PRUEBAS PASARON - SISTEMA LISTO PARA PRODUCCIÓN")
        exit(0)
    else:
        print("❌ ALGUNAS PRUEBAS FALLARON - REVISAR LOGS")
        exit(1)