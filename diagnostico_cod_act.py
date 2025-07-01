#!/usr/bin/env python3
"""
Diagnóstico Específico: Código de Actividad (cod_act)
=====================================================

Verifica la implementación de la lógica de cod_act (TipoDeActividad) 
comparando la implementación Python con la lógica PHP legacy.
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.database_connection import DatabaseConnection
from processors.conceptos_processor import ConceptosProcessor
from config.sicoss_config import SicossConfig

class DiagnosticoCodAct:
    """Diagnóstico específico del campo cod_act"""
    
    def __init__(self):
        print("🔍 DIAGNÓSTICO ESPECÍFICO: Código de Actividad (cod_act)")
        print("=" * 60)
        
        self.db = DatabaseConnection()
        self.config = SicossConfig(
            tope_jubilatorio_patronal=99000000.0,
            tope_jubilatorio_personal=3245240.49,
            tope_otros_aportes_personales=3245240.49,
            trunca_tope=True
        )
        self.conceptos_processor = ConceptosProcessor(self.config)
        
    def ejecutar_diagnostico(self, per_anoct: int = 2025, per_mesct: int = 5, 
                           limite_legajos: int = 20):
        """Ejecutar diagnóstico completo de cod_act"""
        
        print(f"📅 Período: {per_anoct}/{per_mesct:02d}")
        print(f"👥 Analizando {limite_legajos} legajos")
        print("-" * 60)
        
        # 1. Obtener datos de prueba
        legajos, conceptos = self._obtener_datos_prueba(per_anoct, per_mesct, limite_legajos)
        
        print(f"✅ Datos obtenidos:")
        print(f"   - Legajos: {len(legajos)}")
        print(f"   - Conceptos: {len(conceptos)}")
        
        # 2. Procesar con lógica Python
        print(f"\n🐍 PROCESANDO CON LÓGICA PYTHON...")
        resultado_python = self._procesar_python(legajos, conceptos)
        
        # 3. Comparar con lógica PHP (desde BD)
        print(f"\n🐘 COMPARANDO CON LÓGICA PHP...")
        resultado_php = self._obtener_datos_php(per_anoct, per_mesct, limite_legajos)
        
        # 4. Análisis comparativo
        print(f"\n🔄 ANÁLISIS COMPARATIVO...")
        self._analizar_diferencias(resultado_python, resultado_php)
        
        # 5. Diagnóstico detallado por tipo de actividad
        print(f"\n📊 DIAGNÓSTICO DETALLADO...")
        self._diagnostico_detallado(resultado_python, conceptos)
        
        return {
            'python': resultado_python,
            'php': resultado_php,
            'conceptos': conceptos
        }
    
    def _obtener_datos_prueba(self, per_anoct: int, per_mesct: int, limite: int):
        """Obtener datos de prueba"""
        
        # Query para legajos con conceptos investigador (tipos 11-15, 48-49)
        query_legajos = f"""
        SELECT DISTINCT 
            l.nro_legaj,
            l.codigoactividad,
            l.apyno,
            l.cuit,
            l.codigosituacion,
            l.codigocondicion,
            l.codigozona
        FROM mapuche.dh21 l
        INNER JOIN conceptos_liquidados c ON l.nro_legaj = c.nro_legaj
        WHERE l.per_liano = {per_anoct} 
          AND l.per_limes = {per_mesct}
          AND c.tipos_grupos IS NOT NULL
          AND (
              c.tipos_grupos ~ '[[:<:]]11[[:>:]]' OR
              c.tipos_grupos ~ '[[:<:]]12[[:>:]]' OR
              c.tipos_grupos ~ '[[:<:]]13[[:>:]]' OR
              c.tipos_grupos ~ '[[:<:]]14[[:>:]]' OR
              c.tipos_grupos ~ '[[:<:]]15[[:>:]]' OR
              c.tipos_grupos ~ '[[:<:]]48[[:>:]]' OR
              c.tipos_grupos ~ '[[:<:]]49[[:>:]]'
          )
        ORDER BY l.nro_legaj
        LIMIT {limite}
        """
        
        df_legajos = self.db.execute_query(query_legajos)
        
        if df_legajos.empty:
            print("⚠️ No se encontraron legajos con conceptos investigador, usando muestra general...")
            
            query_general = f"""
            SELECT 
                nro_legaj, codigoactividad, apyno, cuit,
                codigosituacion, codigocondicion, codigozona
            FROM mapuche.dh21 
            WHERE per_liano = {per_anoct} AND per_limes = {per_mesct}
            ORDER BY nro_legaj 
            LIMIT {limite}
            """
            df_legajos = self.db.execute_query(query_general)
        
        # Obtener conceptos para estos legajos
        legajos_list = "','".join(map(str, df_legajos['nro_legaj'].unique()))
        
        query_conceptos = f"""
        SELECT 
            nro_legaj, codn_conce, impp_conce, tipos_grupos, codigoescalafon
        FROM conceptos_liquidados
        WHERE nro_legaj IN ('{legajos_list}')
          AND tipos_grupos IS NOT NULL
        ORDER BY nro_legaj, codn_conce
        """
        
        df_conceptos = self.db.execute_query(query_conceptos)
        
        return df_legajos, df_conceptos
    
    def _procesar_python(self, legajos: pd.DataFrame, conceptos: pd.DataFrame):
        """Procesar con lógica Python"""
        
        # Procesador de conceptos que incluye la nueva lógica de TipoDeActividad
        resultado = self.conceptos_processor.process(legajos, conceptos)
        
        # Extraer campos relevantes para el diagnóstico
        campos_interes = [
            'nro_legaj', 'codigoactividad', 'PrioridadTipoDeActividad', 
            'TipoDeActividad', 'ImporteImponible_6'
        ]
        
        return resultado[campos_interes].copy()
    
    def _obtener_datos_php(self, per_anoct: int, per_mesct: int, limite: int):
        """Obtener datos procesados por PHP desde BD"""
        
        query_php = f"""
        SELECT 
            s.cuil,
            s.cod_act,
            s.rem_imp7 as ImporteImponible_6,
            l.nro_legaj,
            l.codigoactividad
        FROM suc.afip_mapuche_sicoss_bkp s
        INNER JOIN mapuche.dh21 l ON s.cuil = l.cuit
        WHERE s.periodo_fiscal = '{per_anoct}{per_mesct:02d}'
          AND l.per_liano = {per_anoct} 
          AND l.per_limes = {per_mesct}
        ORDER BY l.nro_legaj
        LIMIT {limite}
        """
        
        df_php = self.db.execute_query(query_php)
        
        if not df_php.empty:
            # Renombrar para consistencia
            df_php = df_php.rename(columns={'cod_act': 'TipoDeActividad_PHP'})
        
        return df_php
    
    def _analizar_diferencias(self, python_data: pd.DataFrame, php_data: pd.DataFrame):
        """Analizar diferencias entre Python y PHP"""
        
        if php_data.empty:
            print("⚠️ No se encontraron datos PHP para comparar")
            return
        
        # Merge por legajo
        comparacion = python_data.merge(
            php_data[['nro_legaj', 'TipoDeActividad_PHP']], 
            on='nro_legaj', 
            how='inner'
        )
        
        if comparacion.empty:
            print("⚠️ No hay legajos en común para comparar")
            return
        
        # Comparar TipoDeActividad
        comparacion['coincide_cod_act'] = (
            comparacion['TipoDeActividad'] == comparacion['TipoDeActividad_PHP']
        )
        
        coincidencias = comparacion['coincide_cod_act'].sum()
        total = len(comparacion)
        porcentaje = (coincidencias / total) * 100 if total > 0 else 0
        
        print(f"📊 RESULTADOS DE COMPARACIÓN:")
        print(f"   - Total legajos comparados: {total}")
        print(f"   - Coincidencias cod_act: {coincidencias}/{total} ({porcentaje:.1f}%)")
        
        if coincidencias < total:
            print(f"\n❌ DIFERENCIAS ENCONTRADAS:")
            diferencias = comparacion[~comparacion['coincide_cod_act']]
            for _, row in diferencias.head(5).iterrows():
                print(f"   Legajo {row['nro_legaj']}: Python={row['TipoDeActividad']}, PHP={row['TipoDeActividad_PHP']}")
                print(f"     codigoactividad={row['codigoactividad']}, prioridad={row['PrioridadTipoDeActividad']}")
        
        # Estadísticas por valor
        print(f"\n📈 DISTRIBUCIÓN DE VALORES:")
        print(f"Python TipoDeActividad:")
        print(python_data['TipoDeActividad'].value_counts().head())
        print(f"\nPHP cod_act:")
        print(php_data['TipoDeActividad_PHP'].value_counts().head())
    
    def _diagnostico_detallado(self, resultado: pd.DataFrame, conceptos: pd.DataFrame):
        """Diagnóstico detallado de la lógica implementada"""
        
        print(f"🎯 LÓGICA DE ASIGNACIÓN IMPLEMENTADA:")
        print(f"   Caso 1: Prioridad 38 o 0 → usar codigoactividad")
        print(f"   Caso 2: Prioridad 34-37, 87-88 → usar prioridad")
        
        # Estadísticas por caso
        caso1 = resultado[
            (resultado['PrioridadTipoDeActividad'] == 38) | 
            (resultado['PrioridadTipoDeActividad'] == 0)
        ]
        caso2 = resultado[
            ((resultado['PrioridadTipoDeActividad'] >= 34) & (resultado['PrioridadTipoDeActividad'] <= 37)) |
            (resultado['PrioridadTipoDeActividad'] == 87) |
            (resultado['PrioridadTipoDeActividad'] == 88)
        ]
        
        print(f"\n📊 ESTADÍSTICAS POR CASO:")
        print(f"   Caso 1 (codigoactividad): {len(caso1)} legajos")
        if len(caso1) > 0:
            verificacion1 = (caso1['TipoDeActividad'] == caso1['codigoactividad']).all()
            print(f"     ✅ Lógica correcta: {verificacion1}")
        
        print(f"   Caso 2 (prioridad): {len(caso2)} legajos")
        if len(caso2) > 0:
            verificacion2 = (caso2['TipoDeActividad'] == caso2['PrioridadTipoDeActividad']).all()
            print(f"     ✅ Lógica correcta: {verificacion2}")
        
        # Verificar conceptos investigador
        tipos_investigador = [11, 12, 13, 14, 15, 48, 49]
        conceptos_inv = conceptos[conceptos['tipos_grupos'].str.contains('|'.join(map(str, tipos_investigador)), na=False)]
        
        if not conceptos_inv.empty:
            print(f"\n🔬 CONCEPTOS INVESTIGADOR ENCONTRADOS:")
            stats_tipos = conceptos_inv['tipos_grupos'].value_counts().head()
            print(f"   {len(conceptos_inv)} conceptos de {stats_tipos.index.tolist()}")
    
    def close(self):
        """Cerrar conexiones"""
        self.db.close()

def main():
    """Función principal"""
    diagnostico = None
    try:
        diagnostico = DiagnosticoCodAct()
        resultado = diagnostico.ejecutar_diagnostico()
        
        print(f"\n✅ Diagnóstico completado")
        print(f"📄 Revisar resultados arriba para verificar implementación")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en diagnóstico: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if diagnostico:
            diagnostico.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 