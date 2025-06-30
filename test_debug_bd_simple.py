#!/usr/bin/env python3
"""Test simple para diagnosticar problema de inserción BD"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.sicoss_processor import SicossDataProcessor
from config.sicoss_config import SicossConfig
from value_objects.periodo_fiscal import PeriodoFiscal

def test_debug_bd():
    print("🧪 DEBUG: Test simple inserción BD")
    
    # Datos de prueba mínimos
    datos_prueba = {
        'legajos': pd.DataFrame([
            {'nro_legaj': 999999, 'apnom': 'TEST USER', 'cuil': '20999999999', 
             'situacion_revista': 1, 'codigo_obra_social': 101, 'escalafon': 'DOCE',
             'categoria': 15, 'dedicacion': 'EXC', 'fecha_ingreso': '2020-01-01', 'activo': True}
        ]),
        'conceptos': pd.DataFrame([
            {'nro_legaj': 999999, 'codn_conce': 1, 'impp_conce': 100000.0, 'tipos_grupos': [1], 'codigoescalafon': 'DOCE'}
        ]),
        'otra_actividad': pd.DataFrame(),
        'obra_social': pd.DataFrame()
    }
    
    config = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=400000.0,
        trunca_tope=True
    )
    processor = SicossDataProcessor(config)
    periodo = PeriodoFiscal(2025, 1)
    
    print("🔄 Procesando 1 legajo test...")
    try:
        resultado = processor.procesar_datos_extraidos(datos_prueba)
        print(f"📊 Procesamiento: {len(resultado['legajos_procesados'])} legajos")
        
        print("💾 Intentando inserción en BD...")
        resultado_bd = processor._guardar_en_bd_sicoss(resultado, periodo)
        print(f"✅ BD Result: {resultado_bd}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_debug_bd()
    sys.exit(0 if success else 1) 