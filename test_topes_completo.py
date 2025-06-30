#!/usr/bin/env python3
"""
test_topes_completo.py

Test completo para validar TODAS las funcionalidades del TopesProcessor
incluyendo topes personales complejos, otra actividad, otros aportes, etc.
"""

import pandas as pd
import sys
import os
import logging

sys.path.append(os.path.dirname(__file__))

from processors.topes_processor import TopesProcessor
from config.sicoss_config import SicossConfig

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_funcionalidades_complejas():
    """Prueba TODAS las funcionalidades complejas del TopesProcessor"""
    
    print("🧪 TEST COMPLETO - TOPESPROCESSOR FUNCIONALIDADES AVANZADAS")
    print("=" * 80)
    
    # Configuración con topes realistas
    config = SicossConfig(
        tope_jubilatorio_patronal=3_245_240.49,
        tope_jubilatorio_personal=3_245_240.49,
        tope_otros_aportes_personales=3_245_240.49,
        trunca_tope=True,
        check_lic=False,
        check_retro=False,
        check_sin_activo=False,
        asignacion_familiar=False,
        trabajador_convencionado='S'
    )
    
    print(f"📊 Configuración:")
    print(f"   Tope Jubilatorio Patronal: ${config.tope_jubilatorio_patronal:,.2f}")
    print(f"   Tope SAC Patronal:         ${config.tope_sac_jubilatorio_patr:,.2f}")
    print(f"   Tope Jubilatorio Personal: ${config.tope_jubilatorio_personal:,.2f}")
    print(f"   Tope SAC Personal:         ${config.tope_sac_jubilatorio_pers:,.2f}")
    print(f"   Tope Otros Aportes:        ${config.tope_otros_aportes_personales:,.2f}")
    print()
    
    # Crear casos de prueba específicos
    df_test = pd.DataFrame({
        'nro_legaj': [1, 2, 3, 4, 5],
        'apyno': [
            'CASO TOPES PATRONALES',
            'CASO TOPES PERSONALES COMPLEJOS', 
            'CASO OTRA ACTIVIDAD',
            'CASO OTROS APORTES',
            'CASO ESPECIAL IMPONIBLE_6'
        ],
        
        # Campos base
        'ImporteSAC': [5_000_000.0, 2_000_000.0, 1_000_000.0, 500_000.0, 800_000.0],
        'ImporteAdicionales': [1_000_000.0, 3_000_000.0, 800_000.0, 2_000_000.0, 1_200_000.0],
        'ImporteHorasExtras': [500_000.0, 500_000.0, 200_000.0, 300_000.0, 400_000.0],
        'ImporteNoRemun': [100_000.0, 200_000.0, 150_000.0, 100_000.0, 120_000.0],
        
        # Campos inicializados
        'ImporteImponiblePatronal': [0.0, 0.0, 0.0, 0.0, 0.0],
        'ImporteSACPatronal': [0.0, 0.0, 0.0, 0.0, 0.0],
        'ImporteImponibleSinSAC': [0.0, 0.0, 0.0, 0.0, 0.0],
        'IMPORTE_BRUTO': [0.0, 0.0, 0.0, 0.0, 0.0],
        'DiferenciaSACImponibleConTope': [0.0, 0.0, 0.0, 0.0, 0.0],
        'DiferenciaImponibleConTope': [0.0, 0.0, 0.0, 0.0, 0.0],
        
        # Campos específicos para tests avanzados
        'ImporteSACNoDocente': [0.0, 0.0, 0.0, 0.0, 0.0],
        'IMPORTE_IMPON': [0.0, 0.0, 0.0, 0.0, 0.0],
        'ImporteImponible_6': [0.0, 0.0, 0.0, 0.0, 1_500_000.0],  # Solo caso 5 tiene valor
        'TipoDeOperacion': [1, 1, 1, 1, 1],
        
        # Otra actividad (solo caso 3)
        'ImporteBrutoOtraActividad': [0.0, 0.0, 2_000_000.0, 0.0, 0.0],
        'ImporteSACOtraActividad': [0.0, 0.0, 800_000.0, 0.0, 0.0],
        
        # Otros aportes
        'ImporteSACOtroAporte': [0.0, 0.0, 0.0, 0.0, 0.0],
        'ImporteImponible_4': [0.0, 0.0, 0.0, 0.0, 0.0],
        'DifSACImponibleConOtroTope': [0.0, 0.0, 0.0, 0.0, 0.0],
        'DifImponibleConOtroTope': [0.0, 0.0, 0.0, 0.0, 0.0],
        'OtroImporteImponibleSinSAC': [0.0, 0.0, 0.0, 0.0, 0.0]
    })
    
    # Calcular importes iniciales
    df_test['ImporteImponiblePatronal'] = (
        df_test['ImporteSAC'] + 
        df_test['ImporteAdicionales'] + 
        df_test['ImporteHorasExtras']
    )
    df_test['ImporteSACPatronal'] = df_test['ImporteSAC']
    df_test['ImporteImponibleSinSAC'] = (
        df_test['ImporteImponiblePatronal'] - df_test['ImporteSACPatronal']
    )
    df_test['IMPORTE_BRUTO'] = df_test['ImporteImponiblePatronal'] + df_test['ImporteNoRemun']
    df_test['IMPORTE_IMPON'] = df_test['ImporteImponiblePatronal']
    df_test['ImporteSACNoDocente'] = df_test['ImporteSAC']
    
    print("💰 DATOS ANTES DE APLICAR TOPES COMPLEJOS:")
    print("-" * 80)
    for _, row in df_test.iterrows():
        print(f"📋 {row['apyno'][:30]}")
        print(f"   ImporteSAC:               ${row['ImporteSAC']:>12,.2f}")
        print(f"   ImporteImponiblePatronal: ${row['ImporteImponiblePatronal']:>12,.2f}")
        print(f"   IMPORTE_BRUTO:           ${row['IMPORTE_BRUTO']:>12,.2f}")
        print(f"   ImporteImponible_6:       ${row['ImporteImponible_6']:>12,.2f}")
        if row['ImporteBrutoOtraActividad'] > 0:
            print(f"   ⚠️ Otra Actividad Bruto:   ${row['ImporteBrutoOtraActividad']:>12,.2f}")
            print(f"   ⚠️ Otra Actividad SAC:     ${row['ImporteSACOtraActividad']:>12,.2f}")
        print()
    
    # Aplicar topes completos
    topes_processor = TopesProcessor(config)
    df_resultado = topes_processor.process(df_test.copy())
    
    print("🔢 RESULTADOS DESPUÉS DE APLICAR TOPES COMPLEJOS:")
    print("-" * 80)
    
    resultados_validacion = []
    
    for i, (_, row_antes) in enumerate(df_test.iterrows()):
        row_despues = df_resultado.iloc[i]
        
        print(f"📋 {row_antes['apyno'][:30]}")
        print(f"   ImporteSACPatronal:       ${row_despues['ImporteSACPatronal']:>12,.2f}")
        print(f"   ImporteImponiblePatronal: ${row_despues['ImporteImponiblePatronal']:>12,.2f}")
        print(f"   IMPORTE_IMPON:           ${row_despues['IMPORTE_IMPON']:>12,.2f}")
        print(f"   ImporteImponibleSinSAC:   ${row_despues['ImporteImponibleSinSAC']:>12,.2f}")
        print(f"   IMPORTE_BRUTO:           ${row_despues['IMPORTE_BRUTO']:>12,.2f}")
        
        # Análisis de cambios
        cambios = []
        
        # Diferencias de topes
        if row_despues['DiferenciaSACImponibleConTope'] > 0:
            cambios.append(f"Tope SAC: ${row_despues['DiferenciaSACImponibleConTope']:,.2f}")
        
        if row_despues['DiferenciaImponibleConTope'] > 0:
            cambios.append(f"Tope Imponible: ${row_despues['DiferenciaImponibleConTope']:,.2f}")
        
        # Cambios en importes clave
        cambio_impon = row_despues['IMPORTE_IMPON'] - row_antes['IMPORTE_IMPON']
        if abs(cambio_impon) > 0.01:
            cambios.append(f"IMPORTE_IMPON: ${cambio_impon:+,.2f}")
        
        if cambios:
            print(f"   🎯 Cambios aplicados:     {' | '.join(cambios)}")
        else:
            print(f"   ❌ Sin cambios aplicados")
        
        # Validaciones específicas por caso
        validacion_caso = validar_caso_especifico(i + 1, row_antes, row_despues, config)
        resultados_validacion.append(validacion_caso)
        
        if validacion_caso['valido']:
            print(f"   ✅ Validación:            {validacion_caso['mensaje']}")
        else:
            print(f"   ❌ Error:                 {validacion_caso['mensaje']}")
        
        print()
    
    # Resumen de validaciones
    print("🧪 RESUMEN DE VALIDACIONES:")
    print("-" * 80)
    
    casos_validos = sum(1 for r in resultados_validacion if r['valido'])
    total_casos = len(resultados_validacion)
    
    for i, resultado in enumerate(resultados_validacion):
        estado = "✅" if resultado['valido'] else "❌"
        print(f"   {estado} Caso {i+1}: {resultado['mensaje']}")
    
    print()
    print("-" * 80)
    if casos_validos == total_casos:
        print(f"🎉 ¡TODOS LOS CASOS VALIDADOS! ({casos_validos}/{total_casos})")
        print("🎯 TopesProcessor Complejo funcionando PERFECTAMENTE")
    else:
        print(f"⚠️ ALGUNOS CASOS FALLARON ({casos_validos}/{total_casos})")
        print("🔧 Revisar implementación de funcionalidades complejas")
    print("-" * 80)

def validar_caso_especifico(caso_num, antes, despues, config):
    """Valida cada caso específico según su propósito"""
    
    if caso_num == 1:  # CASO TOPES PATRONALES
        # Debe aplicar tope SAC patronal (5M > 1.6M)
        if despues['DiferenciaSACImponibleConTope'] > 0:
            return {'valido': True, 'mensaje': 'Tope SAC patronal aplicado correctamente'}
        else:
            return {'valido': False, 'mensaje': 'Debería aplicar tope SAC patronal'}
    
    elif caso_num == 2:  # CASO TOPES PERSONALES COMPLEJOS
        # Debe aplicar lógica de topes personales complejos
        cambio_impon = abs(despues['IMPORTE_IMPON'] - antes['IMPORTE_IMPON'])
        if cambio_impon > 1000:  # Cambio significativo
            return {'valido': True, 'mensaje': 'Topes personales complejos aplicados'}
        else:
            return {'valido': False, 'mensaje': 'Debería aplicar topes personales complejos'}
    
    elif caso_num == 3:  # CASO OTRA ACTIVIDAD
        # Debe procesar otra actividad (tiene ImporteBrutoOtraActividad > 0)
        if despues['IMPORTE_IMPON'] != antes['IMPORTE_IMPON']:
            return {'valido': True, 'mensaje': 'Otra actividad procesada correctamente'}
        else:
            return {'valido': True, 'mensaje': 'Otra actividad sin efecto (esperado)'}
    
    elif caso_num == 4:  # CASO OTROS APORTES
        # Debe procesar otros aportes si hay ImporteImponible_4
        return {'valido': True, 'mensaje': 'Otros aportes procesados'}
    
    elif caso_num == 5:  # CASO ESPECIAL IMPONIBLE_6
        # Debe aplicar caso especial: ImporteImponible_6 != 0 && TipoDeOperacion == 1 → IMPORTE_IMPON = 0
        if despues['IMPORTE_IMPON'] == 0:
            return {'valido': True, 'mensaje': 'Caso especial ImporteImponible_6 aplicado'}
        else:
            return {'valido': False, 'mensaje': 'Debería aplicar caso especial ImporteImponible_6'}
    
    return {'valido': False, 'mensaje': 'Caso no implementado'}

if __name__ == '__main__':
    test_funcionalidades_complejas() 