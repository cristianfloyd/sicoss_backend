#!/usr/bin/env python3
"""
Test Debug Simple - Legajo Específico
====================================

Test de debug para el legajo 169138 que sabemos que tiene conceptos
"""

import pandas as pd
import sys
import os

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.sicoss_processor import SicossDataProcessor
from extractors.data_extractor_manager import DataExtractorManager
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection

def test_debug_legajo_especifico():
    """Test específico para el legajo 169138"""
    
    print("🔧 TEST DEBUG - LEGAJO 169138")
    print("=" * 40)
    
    # Configuración
    config = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=400000.0,
        trunca_tope=True
    )
    
    db = None
    try:
        # Conexión
        db = DatabaseConnection()
        extractor = DataExtractorManager(db)
        processor = SicossDataProcessor(config)
        
        # Extraer datos para legajo específico
        print("📥 Extrayendo datos para legajo 169138...")
        
        datos = extractor.extraer_datos_completos(
            config=config,
            per_anoct=2025,
            per_mesct=5,
            nro_legajo=169138
        )
        
        print(f"✅ Datos extraídos:")
        print(f"   - Legajos: {len(datos['legajos'])}")
        print(f"   - Conceptos: {len(datos['conceptos'])}")
        print(f"   - Otra actividad: {len(datos['otra_actividad'])}")
        print(f"   - Obra social: {len(datos['obra_social'])}")
        
        if datos['conceptos'].empty:
            print("❌ Error: No hay conceptos para este legajo")
            return False
        
        # Mostrar información de conceptos
        print(f"\n💰 Conceptos encontrados:")
        conceptos_por_tipo = datos['conceptos']['codn_conce'].value_counts().head(10)
        for concepto, cantidad in conceptos_por_tipo.items():
            print(f"   - Concepto {concepto}: {cantidad} registros")
        
        # Verificar estructura de legajos
        print(f"\n👤 Información del legajo:")
        if not datos['legajos'].empty:
            legajo_info = datos['legajos'].iloc[0]
            print(f"   - Legajo: {legajo_info['nro_legaj']}")
            print(f"   - Nombre: {legajo_info['apyno']}")
            print(f"   - CUIT: {legajo_info['cuit']}")
        
        # Intentar procesamiento paso a paso
        print(f"\n⚙️ Iniciando procesamiento...")
        
        try:
            resultado = processor.procesar_datos_extraidos(datos)
            
            print(f"🔍 Tipo de resultado: {type(resultado)}")
            print(f"🔍 Contenido del resultado: {resultado}")
            
            # Verificar si es un dict y tiene 'success'
            if isinstance(resultado, dict) and 'success' in resultado:
                if resultado['success']:
                    print("✅ Procesamiento exitoso!")
                    df_procesados = resultado['data']['legajos']
                    print(f"   - Legajos procesados: {len(df_procesados)}")
                    print(f"   - Columnas generadas: {len(df_procesados.columns)}")
                    
                    # Mostrar campos importantes
                    campos_importantes = ['IMPORTE_BRUTO', 'IMPORTE_IMPON', 'ImporteImponiblePatronal', 'ImporteSAC']
                    for campo in campos_importantes:
                        if campo in df_procesados.columns:
                            valor = df_procesados[campo].iloc[0] if not df_procesados.empty else 0
                            print(f"   - {campo}: ${valor:,.2f}")
                    
                    return True
                else:
                    print(f"❌ Error en procesamiento: {resultado.get('error', 'Error desconocido')}")
                    return False
            else:
                print(f"❌ Resultado inesperado del procesador")
                print(f"   - Esperaba dict con 'success', obtuvo: {type(resultado)}")
                if isinstance(resultado, dict):
                    print(f"   - Claves disponibles: {list(resultado.keys())}")
                return False
                
        except Exception as e:
            print(f"💥 Excepción en procesamiento: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    except Exception as e:
        print(f"💥 Error general: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    success = test_debug_legajo_especifico()
    if success:
        print("\n🎉 Test debug exitoso!")
    else:
        print("\n❌ Test debug falló")