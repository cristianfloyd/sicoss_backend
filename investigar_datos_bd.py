#!/usr/bin/env python3
"""
Investigar datos disponibles en BD
================================

Script para investigar qué períodos y datos están disponibles
en la base de datos para hacer el test del pipeline.
"""

import pandas as pd
import sys
import os

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.database_connection import DatabaseConnection

def investigar_datos_disponibles():
    """Investiga qué datos están disponibles en la BD"""
    
    print("🔍 INVESTIGANDO DATOS DISPONIBLES EN BD")
    print("=" * 50)
    
    db = None
    try:
        # Conectar a BD
        db = DatabaseConnection()
        print("✅ Conexión establecida")
        
        # 1. Verificar esquemas disponibles
        print("\n📂 ESQUEMAS DISPONIBLES:")
        try:
            query_schemas = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('suc', 'sicoss', 'dh01', 'dc01', 'public')
            ORDER BY schema_name
            """
            schemas = db.execute_query(query_schemas)
            for _, row in schemas.iterrows():
                print(f"   - {row['schema_name']}")
        except Exception as e:
            print(f"   ⚠️ Error consultando esquemas: {e}")
        
        # 2. Verificar tablas en esquema 'suc'
        print("\n📋 TABLAS EN ESQUEMA 'suc':")
        try:
            query_tables = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'suc' 
            AND table_name LIKE '%legaj%' OR table_name LIKE '%conce%'
            ORDER BY table_name
            """
            tables = db.execute_query(query_tables)
            for _, row in tables.iterrows():
                print(f"   - {row['table_name']}")
        except Exception as e:
            print(f"   ⚠️ Error consultando tablas: {e}")
        
        # 3. Verificar datos en dh01 (legajos)
        print("\n👥 DATOS DE LEGAJOS (dh01):")
        try:
            query_legajos = """
            SELECT 
                per_anoct, per_mesct, 
                COUNT(*) as total_legajos,
                MIN(nro_legaj) as min_legajo,
                MAX(nro_legaj) as max_legajo
            FROM suc.dh01 
            WHERE per_anoct >= 2020
            GROUP BY per_anoct, per_mesct
            ORDER BY per_anoct DESC, per_mesct DESC
            LIMIT 10
            """
            legajos_data = db.execute_query(query_legajos)
            
            if not legajos_data.empty:
                print("   Períodos con legajos disponibles:")
                for _, row in legajos_data.iterrows():
                    print(f"   - {int(row['per_anoct'])}/{int(row['per_mesct']):02d}: "
                          f"{int(row['total_legajos'])} legajos "
                          f"(rango: {int(row['min_legajo'])}-{int(row['max_legajo'])})")
            else:
                print("   ⚠️ No hay datos de legajos disponibles")
                
        except Exception as e:
            print(f"   ⚠️ Error consultando legajos: {e}")
        
        # 4. Verificar datos en dc01 (conceptos)
        print("\n💰 DATOS DE CONCEPTOS (dc01):")
        try:
            query_conceptos = """
            SELECT 
                per_anoct, per_mesct,
                COUNT(*) as total_conceptos,
                COUNT(DISTINCT nro_legaj) as legajos_con_conceptos,
                COUNT(DISTINCT codn_conce) as tipos_conceptos_distintos
            FROM suc.dc01 
            WHERE per_anoct >= 2020
            GROUP BY per_anoct, per_mesct
            ORDER BY per_anoct DESC, per_mesct DESC
            LIMIT 10
            """
            conceptos_data = db.execute_query(query_conceptos)
            
            if not conceptos_data.empty:
                print("   Períodos con conceptos disponibles:")
                for _, row in conceptos_data.iterrows():
                    print(f"   - {int(row['per_anoct'])}/{int(row['per_mesct']):02d}: "
                          f"{int(row['total_conceptos'])} conceptos, "
                          f"{int(row['legajos_con_conceptos'])} legajos, "
                          f"{int(row['tipos_conceptos_distintos'])} tipos")
            else:
                print("   ⚠️ No hay datos de conceptos disponibles")
                
        except Exception as e:
            print(f"   ⚠️ Error consultando conceptos: {e}")
        
        # 5. Buscar el período más reciente con datos completos
        print("\n🎯 PERÍODO RECOMENDADO PARA TESTING:")
        try:
            query_completo = """
            SELECT 
                l.per_anoct, l.per_mesct,
                COUNT(DISTINCT l.nro_legaj) as legajos,
                COUNT(DISTINCT c.nro_legaj) as legajos_con_conceptos,
                COUNT(c.nro_legaj) as total_conceptos
            FROM suc.dh01 l
            LEFT JOIN suc.dc01 c ON l.nro_legaj = c.nro_legaj 
                AND l.per_anoct = c.per_anoct 
                AND l.per_mesct = c.per_mesct
            WHERE l.per_anoct >= 2020
            GROUP BY l.per_anoct, l.per_mesct
            HAVING COUNT(DISTINCT c.nro_legaj) > 0
            ORDER BY l.per_anoct DESC, l.per_mesct DESC
            LIMIT 5
            """
            completo_data = db.execute_query(query_completo)
            
            if not completo_data.empty:
                print("   Períodos con datos completos (legajos + conceptos):")
                mejor_periodo = None
                for _, row in completo_data.iterrows():
                    ano = int(row['per_anoct'])
                    mes = int(row['per_mesct'])
                    legajos = int(row['legajos'])
                    legajos_conceptos = int(row['legajos_con_conceptos'])
                    total_conceptos = int(row['total_conceptos'])
                    
                    coverage = (legajos_conceptos / legajos) * 100 if legajos > 0 else 0
                    
                    print(f"   - {ano}/{mes:02d}: {legajos} legajos, "
                          f"{legajos_conceptos} con conceptos ({coverage:.1f}% cobertura), "
                          f"{total_conceptos} conceptos totales")
                    
                    if mejor_periodo is None and coverage > 50:
                        mejor_periodo = (ano, mes)
                
                if mejor_periodo:
                    print(f"\n🏆 MEJOR PERÍODO PARA TESTING: {mejor_periodo[0]}/{mejor_periodo[1]:02d}")
                    return mejor_periodo
                    
            else:
                print("   ⚠️ No se encontraron períodos con datos completos")
                
        except Exception as e:
            print(f"   ⚠️ Error buscando período completo: {e}")
        
        # 6. Verificar algunos legajos específicos con conceptos
        print("\n🔍 MUESTRA DE LEGAJOS CON CONCEPTOS:")
        try:
            query_muestra = """
            SELECT DISTINCT 
                c.nro_legaj,
                c.per_anoct,
                c.per_mesct,
                COUNT(c.codn_conce) as num_conceptos
            FROM suc.dc01 c
            WHERE c.per_anoct >= 2020
            GROUP BY c.nro_legaj, c.per_anoct, c.per_mesct
            HAVING COUNT(c.codn_conce) >= 2
            ORDER BY c.per_anoct DESC, c.per_mesct DESC, COUNT(c.codn_conce) DESC
            LIMIT 10
            """
            muestra_data = db.execute_query(query_muestra)
            
            if not muestra_data.empty:
                for _, row in muestra_data.iterrows():
                    print(f"   - Legajo {int(row['nro_legaj'])} "
                          f"({int(row['per_anoct'])}/{int(row['per_mesct']):02d}): "
                          f"{int(row['num_conceptos'])} conceptos")
            else:
                print("   ⚠️ No hay legajos con conceptos")
                
        except Exception as e:
            print(f"   ⚠️ Error consultando muestra: {e}")
        
    finally:
        if db:
            db.close()
            print("\n🔒 Conexión cerrada")
    
    return None

if __name__ == "__main__":
    periodo = investigar_datos_disponibles()
    if periodo:
        print(f"\n✅ Usar período {periodo[0]}/{periodo[1]:02d} para el test del pipeline")
    else:
        print("\n❌ No se encontró un período adecuado para testing") 