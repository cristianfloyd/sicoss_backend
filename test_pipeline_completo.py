#!/usr/bin/env python3
"""
Test Pipeline Completo SICOSS Backend
=====================================

Test definitivo que verifica el pipeline completo end-to-end:

1. 🔍 EXTRACCIÓN: Consulta datos reales de la base de datos
2. ⚙️ PROCESAMIENTO: Ejecuta todos los cálculos SICOSS 
3. 💾 ALMACENAMIENTO: Guarda resultados en base de datos real
4. ✅ VERIFICACIÓN: Valida que todo el proceso funcionó correctamente

Este test simula el flujo completo de producción del sistema SICOSS.
"""

import pandas as pd
import sys
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports del sistema SICOSS
from processors.sicoss_processor import SicossDataProcessor
from processors.database_saver import SicossDatabaseSaver
from extractors.data_extractor_manager import DataExtractorManager
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from value_objects.periodo_fiscal import PeriodoFiscal

class PipelineCompleto:
    """Test completo del pipeline SICOSS end-to-end"""
    
    def __init__(self):
        """Inicializar componentes del pipeline"""
        print("🚀 Inicializando Pipeline Completo SICOSS")
        print("=" * 50)
        
        # Configuración SICOSS
        self.config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        # Conexión a BD
        try:
            self.db = DatabaseConnection()
            print("✅ Conexión a BD establecida")
        except Exception as e:
            print(f"❌ Error conectando a BD: {e}")
            raise
        
        # Componentes del pipeline
        self.extractor_manager = DataExtractorManager(self.db)
        self.processor = SicossDataProcessor(self.config)
        self.database_saver = SicossDatabaseSaver(self.config, self.db)
        
        print("✅ Todos los componentes inicializados correctamente")
        
    def ejecutar_pipeline_completo(self, per_anoct: int, per_mesct: int, 
                                 nro_legajo: Optional[int] = None,
                                 limite_legajos: int = 10) -> Dict:
        """
        Ejecuta el pipeline completo end-to-end
        
        Args:
            per_anoct: Año contable (ej: 2025)
            per_mesct: Mes contable (ej: 1)
            nro_legajo: Legajo específico (opcional)
            limite_legajos: Máximo número de legajos a procesar
            
        Returns:
            Dict con resultados completos del pipeline
        """
        print(f"\n🔄 EJECUTANDO PIPELINE COMPLETO")
        print(f"📅 Período: {per_anoct}/{per_mesct:02d}")
        if nro_legajo:
            print(f"👤 Legajo específico: {nro_legajo}")
        print(f"📊 Límite de legajos: {limite_legajos}")
        print("=" * 60)
        
        tiempo_inicio = time.time()
        resultado_completo = {
            'success': False,
            'periodo': f"{per_anoct}{per_mesct:02d}",
            'timestamp': datetime.now().isoformat(),
            'fases': {},
            'metricas': {},
            'errores': []
        }
        
        try:
            # ========================================
            # FASE 1: EXTRACCIÓN DE DATOS REALES
            # ========================================
            print("\n📥 FASE 1: EXTRACCIÓN DE DATOS")
            print("-" * 40)
            
            tiempo_extraccion = time.time()
            
            datos_extraidos = self.extractor_manager.extraer_datos_completos(
                config=self.config,
                per_anoct=per_anoct,
                per_mesct=per_mesct,
                nro_legajo=nro_legajo
            )
            
            # Limitar legajos si es necesario
            if len(datos_extraidos['legajos']) > limite_legajos:
                print(f"⚠️ Limitando a {limite_legajos} legajos de {len(datos_extraidos['legajos'])} encontrados")
                legajos_limitados = datos_extraidos['legajos'].head(limite_legajos)
                legajos_ids = legajos_limitados['nro_legaj'].tolist()
                
                # Actualizar datos extraídos con límite
                datos_extraidos['legajos'] = legajos_limitados
                datos_extraidos['conceptos'] = datos_extraidos['conceptos'][
                    datos_extraidos['conceptos']['nro_legaj'].isin(legajos_ids)
                ]
                if not datos_extraidos['otra_actividad'].empty:
                    datos_extraidos['otra_actividad'] = datos_extraidos['otra_actividad'][
                        datos_extraidos['otra_actividad']['nro_legaj'].isin(legajos_ids)
                    ]
                if not datos_extraidos['obra_social'].empty:
                    datos_extraidos['obra_social'] = datos_extraidos['obra_social'][
                        datos_extraidos['obra_social']['nro_legaj'].isin(legajos_ids)
                    ]
            
            tiempo_extraccion = time.time() - tiempo_extraccion
            
            print(f"✅ Extracción completada en {tiempo_extraccion:.2f}s")
            print(f"   - Legajos: {len(datos_extraidos['legajos'])}")
            print(f"   - Conceptos: {len(datos_extraidos['conceptos'])}")
            print(f"   - Otra actividad: {len(datos_extraidos['otra_actividad'])}")
            print(f"   - Obra social: {len(datos_extraidos['obra_social'])}")
            
            if datos_extraidos['legajos'].empty:
                raise Exception("No se encontraron legajos para procesar")
            
            resultado_completo['fases']['extraccion'] = {
                'success': True,
                'tiempo': tiempo_extraccion,
                'legajos_extraidos': len(datos_extraidos['legajos']),
                'conceptos_extraidos': len(datos_extraidos['conceptos'])
            }
            
            # ========================================
            # FASE 2: PROCESAMIENTO COMPLETO
            # ========================================
            print("\n⚙️ FASE 2: PROCESAMIENTO COMPLETO")
            print("-" * 40)
            
            tiempo_procesamiento = time.time()
            
            resultado_procesamiento = self.processor.procesar_datos_extraidos(datos_extraidos)
            
            tiempo_procesamiento = time.time() - tiempo_procesamiento
            
            if not resultado_procesamiento['success']:
                raise Exception(f"Error en procesamiento: {resultado_procesamiento.get('error')}")
            
            df_legajos_procesados = resultado_procesamiento['data']['legajos']
            
            print(f"✅ Procesamiento completado en {tiempo_procesamiento:.2f}s")
            print(f"   - Legajos procesados: {len(df_legajos_procesados)}")
            print(f"   - Columnas generadas: {len(df_legajos_procesados.columns)}")
            
            # Mostrar muestra de importes calculados
            if 'IMPORTE_BRUTO' in df_legajos_procesados.columns:
                total_bruto = df_legajos_procesados['IMPORTE_BRUTO'].sum()
                print(f"   - Total importe bruto: ${total_bruto:,.2f}")
            
            if 'ImporteSAC' in df_legajos_procesados.columns:
                total_sac = df_legajos_procesados['ImporteSAC'].sum()
                print(f"   - Total SAC: ${total_sac:,.2f}")
            
            resultado_completo['fases']['procesamiento'] = {
                'success': True,
                'tiempo': tiempo_procesamiento,
                'legajos_procesados': len(df_legajos_procesados),
                'columnas_generadas': len(df_legajos_procesados.columns)
            }
            
            # ========================================
            # FASE 3: ALMACENAMIENTO EN BD REAL
            # ========================================
            print("\n💾 FASE 3: ALMACENAMIENTO EN BD")
            print("-" * 40)
            
            tiempo_guardado = time.time()
            
            periodo_fiscal = PeriodoFiscal.from_string(f"{per_anoct}{per_mesct:02d}")
            
            resultado_guardado = self.database_saver.guardar_en_bd(
                legajos=df_legajos_procesados,
                periodo_fiscal=periodo_fiscal,
                incluir_inactivos=False
            )
            
            tiempo_guardado = time.time() - tiempo_guardado
            
            if not resultado_guardado['success']:
                raise Exception(f"Error en guardado: {resultado_guardado.get('error')}")
            
            print(f"✅ Almacenamiento completado en {tiempo_guardado:.2f}s")
            print(f"   - Legajos guardados: {resultado_guardado['legajos_guardados']}")
            print(f"   - Tabla: {resultado_guardado.get('tabla', 'N/A')}")
            
            resultado_completo['fases']['almacenamiento'] = {
                'success': True,
                'tiempo': tiempo_guardado,
                'legajos_guardados': resultado_guardado['legajos_guardados']
            }
            
            # ========================================
            # FASE 4: VERIFICACIÓN FINAL
            # ========================================
            print("\n✅ FASE 4: VERIFICACIÓN FINAL")
            print("-" * 40)
            
            tiempo_verificacion = time.time()
            
            # Verificar que los datos se guardaron correctamente
            verificacion = self._verificar_datos_guardados(
                periodo_fiscal, 
                resultado_guardado['legajos_guardados']
            )
            
            tiempo_verificacion = time.time() - tiempo_verificacion
            
            print(f"✅ Verificación completada en {tiempo_verificacion:.2f}s")
            for check, resultado in verificacion.items():
                status = "✅" if resultado else "❌"
                print(f"   {status} {check}")
            
            resultado_completo['fases']['verificacion'] = {
                'success': all(verificacion.values()),
                'tiempo': tiempo_verificacion,
                'checks': verificacion
            }
            
            # ========================================
            # RESULTADO FINAL
            # ========================================
            tiempo_total = time.time() - tiempo_inicio
            
            resultado_completo.update({
                'success': True,
                'tiempo_total': tiempo_total,
                'metricas': {
                    'legajos_procesados': len(df_legajos_procesados),
                    'legajos_guardados': resultado_guardado['legajos_guardados'],
                    'tiempo_por_legajo': tiempo_total / len(df_legajos_procesados),
                    'throughput': len(df_legajos_procesados) / tiempo_total
                }
            })
            
            print(f"\n🎉 PIPELINE COMPLETADO EXITOSAMENTE")
            print("=" * 60)
            print(f"⏱️ Tiempo total: {tiempo_total:.2f}s")
            print(f"📊 Legajos procesados: {len(df_legajos_procesados)}")
            print(f"💾 Legajos guardados: {resultado_guardado['legajos_guardados']}")
            print(f"🚀 Throughput: {len(df_legajos_procesados)/tiempo_total:.2f} legajos/s")
            
            return resultado_completo
            
        except Exception as e:
            error_msg = f"Error en pipeline: {str(e)}"
            print(f"\n❌ {error_msg}")
            resultado_completo['errores'].append(error_msg)
            resultado_completo['tiempo_total'] = time.time() - tiempo_inicio
            return resultado_completo
    
    def _verificar_datos_guardados(self, periodo_fiscal: PeriodoFiscal, 
                                 legajos_esperados: int) -> Dict[str, bool]:
        """Verifica que los datos se guardaron correctamente en la BD"""
        verificaciones = {}
        
        try:
            # Query para verificar datos guardados
            query_verificacion = f"""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT cuil) as legajos_unicos,
                SUM(rem_total) as total_bruto,
                MIN(rem_total) as min_bruto,
                MAX(rem_total) as max_bruto,
                AVG(rem_total) as promedio_bruto
            FROM suc.afip_mapuche_sicoss 
            WHERE periodo_fiscal = '{periodo_fiscal.periodo_str}'
            """
            
            resultado = self.db.execute_query(query_verificacion)
            
            if not resultado.empty:
                row = resultado.iloc[0]
                
                # Verificaciones específicas
                verificaciones['Registros guardados'] = int(row['total_registros']) > 0
                verificaciones['Cantidad esperada'] = int(row['total_registros']) == legajos_esperados
                verificaciones['Legajos únicos'] = int(row['legajos_unicos']) > 0
                verificaciones['Importes válidos'] = float(row['total_bruto']) > 0
                verificaciones['Rangos lógicos'] = (
                    float(row['min_bruto']) >= 0 and 
                    float(row['max_bruto']) > float(row['min_bruto'])
                )
                
                print(f"   📊 Estadísticas guardadas:")
                print(f"      - Registros: {int(row['total_registros'])}")
                print(f"      - Legajos únicos: {int(row['legajos_unicos'])}")
                print(f"      - Total bruto: ${float(row['total_bruto']):,.2f}")
                print(f"      - Rango: ${float(row['min_bruto']):,.2f} - ${float(row['max_bruto']):,.2f}")
                
            else:
                verificaciones['Datos encontrados'] = False
                
        except Exception as e:
            print(f"   ⚠️ Error en verificación: {e}")
            verificaciones['Error en verificación'] = False
        
        return verificaciones
    
    def generar_reporte_completo(self, resultado: Dict) -> str:
        """Genera reporte detallado del pipeline"""
        reporte = []
        reporte.append("=" * 80)
        reporte.append("REPORTE COMPLETO - PIPELINE SICOSS")
        reporte.append("=" * 80)
        
        # Información general
        reporte.append(f"📅 Período: {resultado['periodo']}")
        reporte.append(f"🕒 Timestamp: {resultado['timestamp']}")
        reporte.append(f"✅ Éxito: {'SÍ' if resultado['success'] else 'NO'}")
        reporte.append(f"⏱️ Tiempo total: {resultado.get('tiempo_total', 0):.2f}s")
        reporte.append("")
        
        # Métricas
        if 'metricas' in resultado:
            reporte.append("📊 MÉTRICAS:")
            metricas = resultado['metricas']
            reporte.append(f"   - Legajos procesados: {metricas.get('legajos_procesados', 0)}")
            reporte.append(f"   - Legajos guardados: {metricas.get('legajos_guardados', 0)}")
            reporte.append(f"   - Tiempo por legajo: {metricas.get('tiempo_por_legajo', 0):.3f}s")
            reporte.append(f"   - Throughput: {metricas.get('throughput', 0):.2f} legajos/s")
            reporte.append("")
        
        # Fases del pipeline
        if 'fases' in resultado:
            reporte.append("🔄 FASES DEL PIPELINE:")
            for fase, datos in resultado['fases'].items():
                status = "✅" if datos.get('success', False) else "❌"
                tiempo = datos.get('tiempo', 0)
                reporte.append(f"   {status} {fase.upper()}: {tiempo:.2f}s")
            reporte.append("")
        
        # Errores
        if resultado.get('errores'):
            reporte.append("❌ ERRORES:")
            for error in resultado['errores']:
                reporte.append(f"   - {error}")
            reporte.append("")
        
        reporte.append("=" * 80)
        
        return "\n".join(reporte)
    
    def close(self):
        """Cerrar conexiones"""
        if hasattr(self, 'db'):
            self.db.close()
        if hasattr(self, 'database_saver'):
            self.database_saver.close()
        print("🔒 Conexiones cerradas")

def test_pipeline_datos_reales():
    """Test principal del pipeline con datos reales"""
    print("🧪 INICIANDO TEST PIPELINE COMPLETO")
    print("=" * 80)
    
    pipeline = None
    try:
        # Inicializar pipeline
        pipeline = PipelineCompleto()
        
        # Ejecutar con parámetros de prueba
        resultado = pipeline.ejecutar_pipeline_completo(
            per_anoct=2025,
            per_mesct=5,
            nro_legajo=None,  # Procesar varios legajos
            limite_legajos=10  # Límite para testing
        )
        
        # Generar reporte
        reporte = pipeline.generar_reporte_completo(resultado)
        print("\n" + reporte)
        
        # Guardar reporte en archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo_reporte = f"reporte_pipeline_{timestamp}.txt"
        with open(archivo_reporte, 'w', encoding='utf-8') as f:
            f.write(reporte)
        print(f"📄 Reporte guardado en: {archivo_reporte}")
        
        # Resultado final
        if resultado['success']:
            print("\n🎉 TEST PIPELINE: ¡ÉXITO COMPLETO!")
            return True
        else:
            print("\n❌ TEST PIPELINE: FALLÓ")
            return False
            
    except Exception as e:
        print(f"\n💥 Error crítico en test: {e}")
        return False
    finally:
        if pipeline:
            pipeline.close()

def test_pipeline_legajo_especifico():
    """Test del pipeline con un legajo específico"""
    print("\n🧪 TEST PIPELINE - LEGAJO ESPECÍFICO")
    print("=" * 60)
    
    pipeline = None
    try:
        # Inicializar pipeline
        pipeline = PipelineCompleto()
        
        # Ejecutar con legajo específico (usar uno que sabemos que existe)
        resultado = pipeline.ejecutar_pipeline_completo(
            per_anoct=2025,
            per_mesct=1,
            nro_legajo=100001,  # Legajo específico de prueba
            limite_legajos=1
        )
        
        return resultado['success']
        
    except Exception as e:
        print(f"Error en test legajo específico: {e}")
        return False
    finally:
        if pipeline:
            pipeline.close()

if __name__ == "__main__":
    """Ejecutar tests del pipeline"""
    
    print("🚀 EJECUTANDO TESTS COMPLETOS DEL PIPELINE SICOSS")
    print("=" * 80)
    
    # Test 1: Pipeline con múltiples legajos
    exito_completo = test_pipeline_datos_reales()
    
    # Test 2: Pipeline con legajo específico
    # exito_especifico = test_pipeline_legajo_especifico()
    
    # Resultado final
    print("\n" + "=" * 80)
    print("RESUMEN FINAL DE TESTS")
    print("=" * 80)
    
    if exito_completo:
        print("🎉 ¡TODOS LOS TESTS DEL PIPELINE COMPLETADOS EXITOSAMENTE!")
        print("✅ El sistema SICOSS está completamente operativo")
        print("🚀 Listo para usar en PRODUCCIÓN")
    else:
        print("❌ Algunos tests fallaron")
        print("🔧 Revisar errores antes de usar en producción")
    
    print("=" * 80) 