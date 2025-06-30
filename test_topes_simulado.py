#!/usr/bin/env python3
"""
test_topes_simulado.py

Script de prueba para validar TopesProcessor con datos simulados
que exceden los topes para verificar la lógica de aplicación
"""

import pandas as pd
import sys
import os
import logging

# Agregar el directorio padre al path
sys.path.append(os.path.dirname(__file__))

from processors.topes_processor import TopesProcessor
from config.sicoss_config import SicossConfig

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_topes_simulados():
    """Prueba TopesProcessor con datos simulados que exceden topes"""
    
    print("🧪 PRUEBA TOPESPROCESSOR CON DATOS SIMULADOS")
    print("=" * 70)
    
    # 1. CONFIGURACIÓN CON TOPES REALISTAS
    config = SicossConfig(
        tope_jubilatorio_patronal=3_245_240.49,  # Tope real
        tope_jubilatorio_personal=3_245_240.49,
        tope_otros_aportes_personales=3_245_240.49,
        trunca_tope=True,
        check_lic=False,
        check_retro=False,
        check_sin_activo=False,
        asignacion_familiar=False,
        trabajador_convencionado='S'
    )
    
    print(f"📊 Configuración de Topes:")
    print(f"   Tope Jubilatorio Patronal: ${config.tope_jubilatorio_patronal:,.2f}")
    print(f"   Tope SAC Patronal:         ${config.tope_sac_jubilatorio_patr:,.2f}")
    print(f"   Truncar Topes:             {config.trunca_tope}")
    print()
    
    # 2. CREAR DATOS DE PRUEBA QUE EXCEDEN TOPES
    df_test = pd.DataFrame({
        'nro_legaj': [110830, 110831, 110832],
        'apyno': ['LEGAJO TOPE SAC', 'LEGAJO TOPE IMPONIBLE', 'LEGAJO SIN TOPES'],
        
        # Casos de prueba
        'ImporteSAC': [5_000_000.0, 1_000_000.0, 500_000.0],  # [Excede, Normal, Normal]
        'ImporteAdicionales': [500_000.0, 5_000_000.0, 800_000.0],  # [Normal, Excede, Normal]
        'ImporteHorasExtras': [200_000.0, 200_000.0, 200_000.0],
        'ImporteNoRemun': [100_000.0, 100_000.0, 100_000.0],
        
        # Campos inicializados que TopesProcessor espera
        'ImporteImponiblePatronal': [0.0, 0.0, 0.0],
        'ImporteSACPatronal': [0.0, 0.0, 0.0], 
        'ImporteImponibleSinSAC': [0.0, 0.0, 0.0],
        'DiferenciaSACImponibleConTope': [0.0, 0.0, 0.0],
        'DiferenciaImponibleConTope': [0.0, 0.0, 0.0],
        'IMPORTE_BRUTO': [0.0, 0.0, 0.0]
    })
    
    # 3. CALCULAR IMPORTES INICIALES (simular CalculosProcessor)
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
    
    print("💰 DATOS ANTES DE APLICAR TOPES:")
    print("-" * 70)
    for _, row in df_test.iterrows():
        print(f"📋 {row['apyno']}")
        print(f"   ImporteSAC:             ${row['ImporteSAC']:>12,.2f}")
        print(f"   ImporteImponiblePatronal: ${row['ImporteImponiblePatronal']:>12,.2f}")
        print(f"   ImporteImponibleSinSAC:   ${row['ImporteImponibleSinSAC']:>12,.2f}")
        print(f"   IMPORTE_BRUTO:           ${row['IMPORTE_BRUTO']:>12,.2f}")
        print()
    
    # 4. APLICAR TOPES
    topes_processor = TopesProcessor(config)
    df_con_topes = topes_processor.process(df_test.copy())
    
    # 5. MOSTRAR RESULTADOS
    print("🔢 RESULTADOS DESPUÉS DE APLICAR TOPES:")
    print("-" * 70)
    
    for i, (_, row_antes) in enumerate(df_test.iterrows()):
        row_despues = df_con_topes.iloc[i]
        
        print(f"📋 {row_antes['apyno']}")
        
        # Comparar importes
        print(f"   ImporteSACPatronal:       ${row_despues['ImporteSACPatronal']:>12,.2f}")
        print(f"   ImporteImponiblePatronal: ${row_despues['ImporteImponiblePatronal']:>12,.2f}")
        print(f"   ImporteImponibleSinSAC:   ${row_despues['ImporteImponibleSinSAC']:>12,.2f}")
        print(f"   IMPORTE_BRUTO:           ${row_despues['IMPORTE_BRUTO']:>12,.2f}")
        
        # Analizar diferencias de topes
        diff_sac = row_despues['DiferenciaSACImponibleConTope']
        diff_imponible = row_despues['DiferenciaImponibleConTope']
        
        if diff_sac > 0:
            print(f"   🎯 Tope SAC aplicado:     ${diff_sac:>12,.2f}")
        else:
            print(f"   ❌ Tope SAC NO aplicado")
            
        if diff_imponible > 0:
            print(f"   🎯 Tope Imponible aplicado: ${diff_imponible:>12,.2f}")
        else:
            print(f"   ❌ Tope Imponible NO aplicado")
        
        # Cambios en importes
        cambio_patronal = row_despues['ImporteImponiblePatronal'] - row_antes['ImporteImponiblePatronal']
        cambio_bruto = row_despues['IMPORTE_BRUTO'] - row_antes['IMPORTE_BRUTO']
        
        if abs(cambio_patronal) > 0.01:
            print(f"   📊 Cambio Imponible:      ${cambio_patronal:>+12,.2f}")
        if abs(cambio_bruto) > 0.01:
            print(f"   📊 Cambio Bruto:          ${cambio_bruto:>+12,.2f}")
            
        print()
    
    # 6. VALIDACIONES
    print("🧪 VALIDACIONES:")
    print("-" * 70)
    
    # Caso 1: Tope SAC aplicado
    caso1 = df_con_topes.iloc[0]
    sac_excede = df_test.iloc[0]['ImporteSAC'] > config.tope_sac_jubilatorio_patr
    tope_sac_aplicado = caso1['DiferenciaSACImponibleConTope'] > 0
    
    print(f"✅ Caso 1 (SAC excede): {sac_excede} → Tope aplicado: {tope_sac_aplicado}")
    
    # Caso 2: Tope Imponible aplicado  
    caso2 = df_con_topes.iloc[1]
    imponible_excede = df_test.iloc[1]['ImporteImponibleSinSAC'] > config.tope_jubilatorio_patronal
    tope_imponible_aplicado = caso2['DiferenciaImponibleConTope'] > 0
    
    print(f"✅ Caso 2 (Imponible excede): {imponible_excede} → Tope aplicado: {tope_imponible_aplicado}")
    
    # Caso 3: Sin topes
    caso3 = df_con_topes.iloc[2]
    sin_topes = (caso3['DiferenciaSACImponibleConTope'] == 0 and 
                caso3['DiferenciaImponibleConTope'] == 0)
    
    print(f"✅ Caso 3 (Sin excesos): → Sin topes aplicados: {sin_topes}")
    
    print()
    print("=" * 70)
    if tope_sac_aplicado and tope_imponible_aplicado and sin_topes:
        print("🎉 ¡TODAS LAS VALIDACIONES PASARON! TopesProcessor funciona correctamente")
    else:
        print("❌ ALGUNAS VALIDACIONES FALLARON")
    print("=" * 70)

if __name__ == '__main__':
    test_topes_simulados() 