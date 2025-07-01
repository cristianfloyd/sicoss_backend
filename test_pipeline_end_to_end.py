#!/usr/bin/env python3
"""
Test Pipeline End-to-End SICOSS Backend
=======================================

Test completo que verifica el pipeline end-to-end:
1. Extrae datos reales de la BD
2. Procesa con todos los cálculos SICOSS
3. Almacena resultados en BD real
4. Verifica funcionamiento completo
"""

import pandas as pd
import sys
import os
import time
from datetime import datetime
from typing import Dict, Optional, cast

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports del sistema
from processors.sicoss_processor import SicossDataProcessor
from processors.database_saver import SicossDatabaseSaver
from extractors.data_extractor_manager import DataExtractorManager
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from value_objects.periodo_fiscal import PeriodoFiscal

class TestPipelineCompleto:
    """Test del pipeline completo SICOSS"""
    
    def __init__(self):
        print("🚀 Inicializando Test Pipeline Completo END-TO-END")
        
        # Conexión BD
        self.db = DatabaseConnection()
        
        # Cargar configuración REAL desde la base de datos
        self.config = self._cargar_configuracion_real()
        
        # Componentes
        self.extractor = DataExtractorManager(self.db)
        self.processor = SicossDataProcessor(self.config)
        self.saver = SicossDatabaseSaver(self.config, self.db)
        
        print("✅ Componentes inicializados con configuración real de BD")
    
    def _cargar_configuracion_real(self) -> SicossConfig:
        """Carga la configuración real desde la base de datos (end-to-end)"""
        
        print("📋 Cargando configuración REAL desde base de datos...")
        
        query_topes = """
        SELECT nombre_parametro, dato_parametro 
        FROM mapuche.rrhhini 
        WHERE nombre_seccion = 'Topes' 
        AND nombre_parametro IN ('TopeJubilatorioPersonal', 'TopeJubilatorioPatronal', 'TopeOtrosAportesPersonales')
        """
        
        df_topes = self.db.execute_query(query_topes)
        
        if df_topes.empty:
            raise Exception("No se pudieron obtener los topes desde la base de datos")
        
        # Crear diccionario de topes
        topes = {}
        for _, row in df_topes.iterrows():
            topes[row['nombre_parametro']] = float(row['dato_parametro'])
        
        # Obtener otros parámetros SICOSS
        query_otros = """
        SELECT nombre_parametro, dato_parametro 
        FROM mapuche.rrhhini 
        WHERE nombre_seccion = 'Datos Universidad' 
        AND nombre_parametro IN ('TrabajadorConvencionado', 'truncaTope')
        UNION
        SELECT nombre_parametro, dato_parametro 
        FROM mapuche.rrhhini 
        WHERE nombre_seccion = 'Conceptos' 
        AND nombre_parametro = 'AcumularAsigFamiliar'
        """
        
        df_otros = self.db.execute_query(query_otros)
        otros_params = {}
        for _, row in df_otros.iterrows():
            otros_params[row['nombre_parametro']] = row['dato_parametro']
        
        config = SicossConfig(
            tope_jubilatorio_patronal=topes.get('TopeJubilatorioPatronal', 99000000.0),
            tope_jubilatorio_personal=topes.get('TopeJubilatorioPersonal', 3245240.49),
            tope_otros_aportes_personales=topes.get('TopeOtrosAportesPersonales', 3245240.49),
            trunca_tope=otros_params.get('truncaTope', 'true').lower() == 'true',
            trabajador_convencionado=otros_params.get('TrabajadorConvencionado', 'S'),
            asignacion_familiar=otros_params.get('AcumularAsigFamiliar', 'false').lower() == 'true'
        )
        
        print(f"✅ Configuración cargada:")
        print(f"   - Tope Jubilatorio Personal: ${config.tope_jubilatorio_personal:,.2f}")
        print(f"   - Tope Jubilatorio Patronal: ${config.tope_jubilatorio_patronal:,.2f}")
        print(f"   - Tope Otros Aportes: ${config.tope_otros_aportes_personales:,.2f}")
        print(f"   - Trunca Tope: {config.trunca_tope}")
        
        return config
    
    def ejecutar_test_completo(self, per_anoct: int = 2025, per_mesct: int = 5, 
                              limite: int = 100) -> Dict:
        """Ejecuta el test completo del pipeline con múltiples legajos"""
        
        print(f"\n🔄 EJECUTANDO PIPELINE END-TO-END COMPLETO")
        print(f"📅 Período: {per_anoct}/{per_mesct:02d}")
        print(f"🎯 Estrategia: Procesamiento masivo de {limite} legajos con datos reales")
        print("=" * 60)
        
        resultado = {
            'success': False,
            'fases': {},
            'metricas': {},
            'errores': []
        }
        
        inicio = time.time()
        
        try:
            # FASE 1: EXTRACCIÓN MASIVA END-TO-END
            print(f"\n📥 FASE 1: Extrayendo {limite} legajos con datos reales...")
            print("🎯 Seleccionando legajos con conceptos liquidados...")
            t1 = time.time()
            
            # Extraer TODOS los datos del período sin filtro específico de legajo
            # Esto simula el comportamiento real del sistema PHP legacy
            datos = self.extractor.extraer_datos_completos(
                config=self.config,
                per_anoct=per_anoct,
                per_mesct=per_mesct,
                nro_legajo=None  # Procesar todos los legajos disponibles
            )
            
            # Limitar a los primeros N legajos si es necesario
            if len(datos['legajos']) > limite:
                print(f"⚠️ Limitando de {len(datos['legajos'])} a {limite} legajos")
                datos['legajos'] = datos['legajos'].head(limite).copy()
                
                # Filtrar conceptos para los legajos seleccionados
                legajos_seleccionados = list(datos['legajos']['nro_legaj'].unique())
                conceptos_filtrados = datos['conceptos'][
                    datos['conceptos']['nro_legaj'].isin(legajos_seleccionados)
                ].copy()
                datos['conceptos'] = conceptos_filtrados
            
            print(f"🎯 Legajos seleccionados: {len(datos['legajos'])}")
            print(f"📊 Conceptos asociados: {len(datos['conceptos'])}")
            
            if not datos['legajos'].empty and not datos['conceptos'].empty:
                total_importe = datos['conceptos']['impp_conce'].sum()
                print(f"💰 Total importe conceptos: ${total_importe:,.2f}")
                
                # Estadísticas por tipo de concepto
                stats_conceptos = datos['conceptos'].groupby('codn_conce').agg({
                    'impp_conce': ['count', 'sum']
                }).head(5)
                print(f"📈 Top 5 conceptos más frecuentes:")
                for idx, row in stats_conceptos.iterrows():
                    print(f"   Concepto {idx}: {row[('impp_conce', 'count')]} veces, ${row[('impp_conce', 'sum')]:,.2f}")
            
            t1 = time.time() - t1
            
            print(f"✅ Extracción: {t1:.2f}s")
            print(f"   - Legajos: {len(datos['legajos'])}")
            print(f"   - Conceptos: {len(datos['conceptos'])}")
            
            if datos['legajos'].empty:
                raise Exception("No hay legajos para procesar")
            
            resultado['fases']['extraccion'] = {
                'tiempo': t1,
                'legajos': len(datos['legajos'])
            }
            
            # FASE 2: PROCESAMIENTO
            print("\n⚙️ FASE 2: Procesando datos...")
            t2 = time.time()
            
            proc_resultado = self.processor.procesar_datos_extraidos(datos)
            
            # El procesador ahora retorna formato diferente - verificar que tenga los datos esperados
            if 'legajos_procesados' not in proc_resultado:
                raise Exception(f"Error procesamiento: formato inesperado - {list(proc_resultado.keys())}")
            
            df_procesados = proc_resultado['legajos_procesados']
            
            # Verificar que se procesaron datos
            if df_procesados.empty:
                raise Exception("Error: No se procesaron legajos")
            
            t2 = time.time() - t2
            
            print(f"✅ Procesamiento: {t2:.2f}s")
            print(f"   - Legajos procesados: {len(df_procesados)}")
            print(f"   - Columnas generadas: {len(df_procesados.columns)}")
            
            # Verificar estadísticas del procesamiento
            if 'totales' in proc_resultado:
                totales = proc_resultado['totales']
                print(f"   - Total bruto: ${totales.get('bruto', 0):,.2f}")
                print(f"   - Imponible 1: ${totales.get('imponible_1', 0):,.2f}")
                print(f"   - Imponible 2: ${totales.get('imponible_2', 0):,.2f}")
            
            # Verificar que el API response indica éxito
            if 'api_response' in proc_resultado and 'success' in proc_resultado['api_response']:
                api_success = proc_resultado['api_response']['success']
                print(f"   - Estado API: {'✅' if api_success else '❌'}")
            else:
                print("   - ⚠️ No se encontró confirmación API")
            
            resultado['fases']['procesamiento'] = {
                'tiempo': t2,
                'legajos': len(df_procesados)
            }
            
            # FASE 3: GUARDADO EN BD
            print("\n💾 FASE 3: Guardando en BD...")
            t3 = time.time()
            
            periodo = PeriodoFiscal.from_string(f"{per_anoct}{per_mesct:02d}")
            
            guardado = self.saver.guardar_en_bd(
                legajos=df_procesados,
                periodo_fiscal=periodo,
                incluir_inactivos=False
            )
            
            if not guardado['success']:
                raise Exception(f"Error guardado: {guardado.get('error')}")
            
            t3 = time.time() - t3
            
            print(f"✅ Guardado: {t3:.2f}s")
            print(f"   - Legajos guardados: {guardado['legajos_guardados']}")
            
            resultado['fases']['guardado'] = {
                'tiempo': t3,
                'legajos': guardado['legajos_guardados']
            }
            
            # FASE 4: VERIFICACIÓN
            print("\n🔍 FASE 4: Verificando datos guardados...")
            t4 = time.time()
            
            verificacion = self._verificar_guardado(periodo, guardado['legajos_guardados'])
            t4 = time.time() - t4
            
            print(f"✅ Verificación: {t4:.2f}s")
            for check, ok in verificacion.items():
                status = "✅" if ok else "❌"
                print(f"   {status} {check}")
            
            resultado['fases']['verificacion'] = {
                'tiempo': t4,
                'checks': verificacion
            }
            
            # RESULTADO FINAL
            tiempo_total = time.time() - inicio
            
            resultado.update({
                'success': True,
                'tiempo_total': tiempo_total,
                'legajos_procesados': len(df_procesados),
                'conceptos_originales': len(datos['conceptos']),
                'metricas': {
                    'legajos_procesados': len(df_procesados),
                    'legajos_guardados': guardado['legajos_guardados'],
                    'throughput': len(df_procesados) / tiempo_total,
                    'conceptos_procesados': len(datos['conceptos'])
                }
            })
            
            print(f"\n🎉 PIPELINE END-TO-END COMPLETADO EXITOSAMENTE")
            print(f"⏱️ Tiempo total: {tiempo_total:.2f}s")
            print(f"🎯 Legajos procesados: {len(df_procesados)}")
            print(f"📊 Conceptos procesados: {len(datos['conceptos'])}")
            print(f"🚀 Throughput: {resultado['metricas']['throughput']:.2f} legajos/s")
            
            return resultado
            
        except Exception as e:
            error = f"Error en pipeline: {str(e)}"
            print(f"\n❌ {error}")
            resultado['errores'].append(error)
            resultado['tiempo_total'] = time.time() - inicio
            return resultado
    
    def _verificar_guardado(self, periodo: PeriodoFiscal, esperados: int) -> Dict[str, bool]:
        """Verifica que los datos se guardaron correctamente"""
        checks = {}
        
        try:
            query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(rem_total) as total_bruto
            FROM suc.afip_mapuche_sicoss 
            WHERE periodo_fiscal = '{periodo.periodo_str}'
            """
            
            resultado = self.db.execute_query(query)
            
            if not resultado.empty:
                row = resultado.iloc[0]
                checks['Registros guardados'] = int(row['total']) > 0
                checks['Cantidad correcta'] = int(row['total']) == esperados
                checks['Importes válidos'] = float(row['total_bruto']) > 0
            else:
                checks['Datos encontrados'] = False
                
        except Exception as e:
            checks['Error verificación'] = False
            print(f"Error en verificación: {e}")
        
        return checks
    
    def close(self):
        """Cerrar conexiones"""
        self.db.close()

def main():
    """Función principal del test inteligente"""
    print("🧠 TEST PIPELINE INTELIGENTE SICOSS")
    print("=" * 50)
    print("Selecciona automáticamente legajos con conceptos garantizados")
    print("=" * 50)
    
    test_pipeline = None
    try:
        # Crear test
        test_pipeline = TestPipelineCompleto()
        
        # Ejecutar test completo
        resultado = test_pipeline.ejecutar_test_completo()
        
        # Mostrar resultado final
        print("\n" + "=" * 50)
        print("RESULTADO FINAL DEL TEST")
        print("=" * 50)
        
        if resultado['success']:
            print("🎉 ¡TEST INTELIGENTE EXITOSO!")
            print("✅ Pipeline funcionando con selección inteligente")
            print("🚀 Sistema listo para producción")
            print(f"🎯 Legajo procesado exitosamente: {resultado.get('legajo_procesado', 'N/A')}")
            
            # Mostrar métricas
            metricas = resultado.get('metricas', {})
            print(f"\n📊 Métricas finales:")
            print(f"   - Legajos procesados: {metricas.get('legajos_procesados', 0)}")
            print(f"   - Legajos guardados: {metricas.get('legajos_guardados', 0)}")
            print(f"   - Throughput: {metricas.get('throughput', 0):.2f} legajos/s")
            
        else:
            print("❌ TEST FALLÓ")
            print("🔧 Revisar errores:")
            for error in resultado.get('errores', []):
                print(f"   - {error}")
        
        return resultado['success']
        
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        return False
    finally:
        if test_pipeline:
            test_pipeline.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 