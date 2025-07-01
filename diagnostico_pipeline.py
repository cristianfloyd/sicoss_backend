#!/usr/bin/env python3
"""
Diagnóstico específico del pipeline para 2025/05
===============================================

Diagnóstico detallado de por qué no hay conceptos disponibles
"""

import pandas as pd
import sys
import os

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.database_connection import DatabaseConnection
from extractors.data_extractor_manager import DataExtractorManager
from config.sicoss_config import SicossConfig

def diagnostico_completo():
    """Diagnóstico completo del problema"""
    
    print("🔍 DIAGNÓSTICO DETALLADO - PERÍODO 2025/05")
    print("=" * 60)
    
    db = None
    try:
        # Conexión
        db = DatabaseConnection()
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        # 1. Verificar legajos disponibles
        print("\n👥 PASO 1: Verificando legajos disponibles")
        print("-" * 40)
        
        query_legajos = """
        SELECT 
            nro_legaj, 
            desc_appat||' '||desc_nombr AS apnom,
            (nro_cuil1::char(2)||LPAD(nro_cuil::char(8),8,'0')||nro_cuil2::char(1))::float8 AS cuit
        FROM mapuche.dh01 
        LIMIT 10
        """
        
        df_legajos = db.execute_query(query_legajos)
        
        if df_legajos.empty:
            print("❌ No hay legajos para 2025/05")
            return False
        
        print(f"✅ Encontrados {len(df_legajos)} legajos")
        print("   Muestra de legajos:")
        for _, row in df_legajos.head(5).iterrows():
            print(f"   - {int(row['nro_legaj'])}: {row['apnom']}")
        
        # 2. Verificar conceptos para estos legajos específicos
        print("\n💰 PASO 2: Verificando conceptos para estos legajos")
        print("-" * 40)
        
        legajos_ids = df_legajos['nro_legaj'].tolist()[:5]  # Tomar solo 5 para diagnóstico
        
        query_conceptos = f"""
        SELECT 
            dh21.nro_legaj, dh21.codn_conce, dh21.impp_conce
        FROM mapuche.dh21
        INNER JOIN mapuche.dh22 ON dh22.nro_liqui = dh21.nro_liqui
        WHERE dh22.per_liano = 2025 AND dh22.per_limes = 5
        AND dh22.sino_genimp = true
        AND dh21.nro_legaj IN ({','.join(map(str, legajos_ids))})
        """
        
        df_conceptos = db.execute_query(query_conceptos)
        
        if df_conceptos.empty:
            print("❌ No hay conceptos para estos legajos en 2025/05")
            
            # Verificar si hay conceptos para estos legajos en otros períodos
            print("\n🔍 Verificando otros períodos para estos legajos...")
            query_otros_periodos = f"""
            SELECT DISTINCT 
                dh22.per_liano as per_anoct, dh22.per_limes as per_mesct, 
                COUNT(*) as num_conceptos
            FROM mapuche.dh21
            INNER JOIN mapuche.dh22 ON dh22.nro_liqui = dh21.nro_liqui
            WHERE dh22.sino_genimp = true
            AND dh21.nro_legaj IN ({','.join(map(str, legajos_ids))})
            GROUP BY dh22.per_liano, dh22.per_limes
            ORDER BY dh22.per_liano DESC, dh22.per_limes DESC
            LIMIT 10
            """
            
            df_otros = db.execute_query(query_otros_periodos)
            if not df_otros.empty:
                print("   Períodos con conceptos para estos legajos:")
                for _, row in df_otros.iterrows():
                    print(f"   - {int(row['per_anoct'])}/{int(row['per_mesct']):02d}: {int(row['num_conceptos'])} conceptos")
            
        else:
            print(f"✅ Encontrados {len(df_conceptos)} conceptos")
            print("   Muestra de conceptos:")
            for _, row in df_conceptos.head(10).iterrows():
                print(f"   - Legajo {int(row['nro_legaj'])}: Concepto {int(row['codn_conce'])} = ${float(row['impp_conce']):,.2f}")
        
        # 3. Verificar el extractor directamente
        print("\n🔧 PASO 3: Probando extractor directamente")
        print("-" * 40)
        
        extractor = DataExtractorManager(db)
        
        datos_extraidos = extractor.extraer_datos_completos(
            config=config,
            per_anoct=2025,
            per_mesct=5
        )
        
        print(f"   Resultado del extractor:")
        print(f"   - Legajos: {len(datos_extraidos['legajos'])}")
        print(f"   - Conceptos: {len(datos_extraidos['conceptos'])}")
        print(f"   - Otra actividad: {len(datos_extraidos['otra_actividad'])}")
        print(f"   - Obra social: {len(datos_extraidos['obra_social'])}")
        
        # 4. Verificar estructura de datos extraídos
        if not datos_extraidos['legajos'].empty:
            print(f"\n   Columnas en legajos: {list(datos_extraidos['legajos'].columns)}")
            
        if not datos_extraidos['conceptos'].empty:
            print(f"   Columnas en conceptos: {list(datos_extraidos['conceptos'].columns)}")
        else:
            print("   ⚠️ DataFrame de conceptos está vacío")
        
        # 5. Buscar el período más reciente con datos completos
        print("\n🎯 PASO 4: Buscando período alternativo")
        print("-" * 40)
        
        query_alternativo = """
        SELECT 
            dh22.per_liano as per_anoct, dh22.per_limes as per_mesct,
            COUNT(DISTINCT dh21.nro_legaj) as legajos,
            COUNT(DISTINCT dh21.nro_legaj) as legajos_con_conceptos,
            COUNT(dh21.nro_legaj) as total_conceptos
        FROM mapuche.dh21
        INNER JOIN mapuche.dh22 ON dh22.nro_liqui = dh21.nro_liqui
        WHERE dh22.per_liano >= 2024
        AND dh22.sino_genimp = true
        GROUP BY dh22.per_liano, dh22.per_limes
        HAVING COUNT(DISTINCT dh21.nro_legaj) > 0
        ORDER BY dh22.per_liano DESC, dh22.per_limes DESC
        LIMIT 5
        """
        
        df_alternativo = db.execute_query(query_alternativo)
        
        if not df_alternativo.empty:
            print("   Períodos alternativos con datos completos:")
            for _, row in df_alternativo.iterrows():
                ano = int(row['per_anoct'])
                mes = int(row['per_mesct'])
                legajos = int(row['legajos'])
                legajos_conceptos = int(row['legajos_con_conceptos'])
                total_conceptos = int(row['total_conceptos'])
                
                coverage = (legajos_conceptos / legajos) * 100 if legajos > 0 else 0
                
                print(f"   - {ano}/{mes:02d}: {legajos} legajos, "
                      f"{legajos_conceptos} con conceptos ({coverage:.1f}%), "
                      f"{total_conceptos} conceptos totales")
        
        # 6. Probar con un período que sabemos que funciona
        if not df_alternativo.empty:
            mejor_row = df_alternativo.iloc[0]
            mejor_ano = int(mejor_row['per_anoct'])
            mejor_mes = int(mejor_row['per_mesct'])
            
            print(f"\n🧪 PASO 5: Probando con período {mejor_ano}/{mejor_mes:02d}")
            print("-" * 40)
            
            datos_test = extractor.extraer_datos_completos(
                config=config,
                per_anoct=mejor_ano,
                per_mesct=mejor_mes
            )
            
            print(f"   Resultado con {mejor_ano}/{mejor_mes:02d}:")
            print(f"   - Legajos: {len(datos_test['legajos'])}")
            print(f"   - Conceptos: {len(datos_test['conceptos'])}")
            
            if len(datos_test['conceptos']) > 0:
                print(f"   🎉 ¡PERÍODO FUNCIONAL ENCONTRADO! Usar {mejor_ano}/{mejor_mes:02d}")
                return (mejor_ano, mejor_mes)
        
        return None
        
    except Exception as e:
        print(f"💥 Error en diagnóstico: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    resultado = diagnostico_completo()
    
    if resultado:
        ano, mes = resultado
        print(f"\n✅ RECOMENDACIÓN: Usar período {ano}/{mes:02d} para el test del pipeline")
    else:
        print("\n❌ No se encontró un período adecuado para el test") 