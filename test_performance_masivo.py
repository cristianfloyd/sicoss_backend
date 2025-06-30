#!/usr/bin/env python3
"""
Tests de Performance Masivos - SICOSS Backend

Tests para evaluar el rendimiento del sistema con grandes volúmenes de datos
y identificar cuellos de botella de performance.
"""

import pandas as pd
import numpy as np
import time
import sys
import os
from typing import Dict, List, Tuple
import psutil
import gc
from datetime import datetime

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.sicoss_processor import SicossDataProcessor
from config.sicoss_config import SicossConfig
from utils.statistics import EstadisticasHelper

class PerformanceProfiler:
    """Profiler para medir performance del sistema SICOSS"""
    
    def __init__(self):
        self.metricas = {}
        self.start_time = None
        self.start_memory = None
    
    def iniciar_medicion(self, nombre: str):
        """Inicia medición de performance"""
        self.start_time = time.time()
        self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        print(f"🔄 Iniciando medición: {nombre}")
    
    def finalizar_medicion(self, nombre: str, datos_procesados: int) -> Dict:
        """Finaliza medición y calcula métricas"""
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Verificar que start_time no sea None antes de calcular elapsed_time
        if self.start_time is None:
            raise ValueError("start_time no ha sido inicializado. Llama a iniciar_medicion() primero.")
        
        elapsed_time = end_time - self.start_time
        memory_used = end_memory - self.start_memory
        throughput = datos_procesados / elapsed_time if elapsed_time > 0 else 0
        
        metricas = {
            'nombre': nombre,
            'tiempo_total': elapsed_time,
            'memoria_usada_mb': memory_used,
            'datos_procesados': datos_procesados,
            'throughput_por_segundo': throughput,
            'tiempo_por_registro': elapsed_time / datos_procesados if datos_procesados > 0 else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        self.metricas[nombre] = metricas
        
        print(f"✅ {nombre} completado:")
        print(f"   ⏱️ Tiempo: {elapsed_time:.2f}s")
        print(f"   💾 Memoria: {memory_used:.1f}MB")
        print(f"   🚀 Throughput: {throughput:.1f} registros/s")
        print(f"   📊 Tiempo por registro: {elapsed_time/datos_procesados*1000:.2f}ms")
        
        return metricas

def generar_datos_masivos(num_legajos: int, conceptos_por_legajo: int = 5) -> Dict:
    """Genera datos de prueba masivos para testing de performance"""
    print(f"📊 Generando {num_legajos:,} legajos con ~{conceptos_por_legajo} conceptos cada uno...")
    
    # Generar legajos
    legajos_data = []
    for i in range(num_legajos):
        legajo_id = 100000 + i
        legajos_data.append({
            'nro_legaj': legajo_id,
            'apnom': f'EMPLEADO TEST {i+1:06d}',
            'cuil': f'20{300000000 + i:09d}',
            'situacion_revista': np.random.choice([1, 2, 3], p=[0.7, 0.2, 0.1]),
            'codigo_obra_social': np.random.choice([101, 102, 103, 104, 105])
        })
    
    df_legajos = pd.DataFrame(legajos_data)
    
    # Generar conceptos
    conceptos_data = []
    tipos_conceptos = [1, 4, 9, 15, 16, 17, 18, 25, 33]  # Tipos comunes
    tipos_grupos_map = {
        1: [1], 4: [4], 9: [9], 15: [15], 16: [16], 
        17: [17], 18: [18], 25: [25], 33: [33]
    }
    escalafones = ['NODO', 'AUTO', 'DOCE']
    
    for legajo_id in df_legajos['nro_legaj']:
        # Número variable de conceptos por legajo
        num_conceptos = np.random.poisson(conceptos_por_legajo)
        conceptos_legajo = np.random.choice(tipos_conceptos, size=min(num_conceptos, len(tipos_conceptos)), replace=False)
        
        for codn_conce in conceptos_legajo:
            # Importes realistas según tipo de concepto
            if codn_conce == 1:  # Sueldo básico
                importe = np.random.uniform(50000, 150000)
            elif codn_conce == 9:  # SAC
                importe = np.random.uniform(4000, 12500)
            elif codn_conce in [15, 16, 17]:  # Investigador
                importe = np.random.uniform(5000, 25000)
            else:
                importe = np.random.uniform(1000, 20000)
            
            conceptos_data.append({
                'nro_legaj': legajo_id,
                'codn_conce': codn_conce,
                'impp_conce': round(importe, 2),
                'tipos_grupos': tipos_grupos_map.get(codn_conce, [codn_conce]),
                'codigoescalafon': np.random.choice(escalafones)
            })
    
    df_conceptos = pd.DataFrame(conceptos_data)
    
    # Datos vacíos para otra_actividad y obra_social (para simplificar)
    df_otra_actividad = pd.DataFrame()
    df_obra_social = pd.DataFrame()
    
    print(f"✅ Datos generados:")
    print(f"   - Legajos: {len(df_legajos):,}")
    print(f"   - Conceptos: {len(df_conceptos):,}")
    print(f"   - Promedio conceptos/legajo: {len(df_conceptos)/len(df_legajos):.1f}")
    
    return {
        'legajos': df_legajos,
        'conceptos': df_conceptos,
        'otra_actividad': df_otra_actividad,
        'obra_social': df_obra_social
    }

def test_performance_escalabilidad():
    """Test de escalabilidad con diferentes volúmenes de datos"""
    print("\n🧪 TEST PERFORMANCE: Escalabilidad")
    print("=" * 50)
    
    # Configurar sistema
    config = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=400000.0,
        trunca_tope=True
    )
    
    processor = SicossDataProcessor(config)
    profiler = PerformanceProfiler()
    
    # Diferentes tamaños de datos para probar escalabilidad
    tamaños_test = [10, 50, 100, 500, 1000]
    resultados_escalabilidad = []
    
    for tamaño in tamaños_test:
        print(f"\n🔄 Testing con {tamaño} legajos...")
        
        try:
            # Generar datos
            datos = generar_datos_masivos(tamaño, conceptos_por_legajo=3)
            
            # Limpiar memoria antes de test
            gc.collect()
            
            # Medir performance
            profiler.iniciar_medicion(f"Procesamiento_{tamaño}_legajos")
            
            resultado = processor.procesar_datos_extraidos(datos)
            
            if resultado['success']:
                metricas = profiler.finalizar_medicion(
                    f"Procesamiento_{tamaño}_legajos",
                    tamaño
                )
                metricas['tamaño_dataset'] = tamaño
                metricas['success'] = True
                resultados_escalabilidad.append(metricas)
                
                # Estadísticas del resultado
                legajos_procesados = len(resultado['data']['legajos'])
                print(f"   ✅ Legajos procesados: {legajos_procesados}/{tamaño}")
            else:
                print(f"   ❌ Error en procesamiento: {resultado.get('error', 'Error desconocido')}")
                resultados_escalabilidad.append({
                    'tamaño_dataset': tamaño,
                    'success': False,
                    'error': resultado.get('error', 'Error desconocido')
                })
                
        except Exception as e:
            print(f"   ❌ Excepción en test {tamaño}: {e}")
            resultados_escalabilidad.append({
                'tamaño_dataset': tamaño,
                'success': False,
                'error': str(e)
            })
    
    # Análisis de escalabilidad
    print(f"\n📊 ANÁLISIS DE ESCALABILIDAD:")
    print("-" * 40)
    
    resultados_exitosos = [r for r in resultados_escalabilidad if r.get('success', False)]
    
    if len(resultados_exitosos) >= 2:
        # Calcular tendencias
        tamaños = [r['tamaño_dataset'] for r in resultados_exitosos]
        tiempos = [r['tiempo_total'] for r in resultados_exitosos]
        throughputs = [r['throughput_por_segundo'] for r in resultados_exitosos]
        
        print(f"📈 TENDENCIAS:")
        print(f"   - Tiempo mín: {min(tiempos):.2f}s ({min(tamaños)} legajos)")
        print(f"   - Tiempo máx: {max(tiempos):.2f}s ({max(tamaños)} legajos)")
        print(f"   - Throughput promedio: {np.mean(throughputs):.1f} legajos/s")
        print(f"   - Throughput máximo: {max(throughputs):.1f} legajos/s")
        
        # Eficiencia lineal (ideal = 1.0)
        if len(tamaños) >= 2:
            eficiencia = (tiempos[0] * tamaños[-1]) / (tiempos[-1] * tamaños[0])
            print(f"   - Eficiencia escalabilidad: {eficiencia:.2f} (1.0 = ideal)")
    
    return resultados_escalabilidad

def test_performance_memoria():
    """Test de uso de memoria con datasets grandes"""
    print("\n🧪 TEST PERFORMANCE: Uso de Memoria")
    print("=" * 50)
    
    try:
        # Configurar sistema
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        processor = SicossDataProcessor(config)
        
        # Dataset mediano para análisis de memoria
        print("🔄 Generando dataset para análisis de memoria...")
        datos = generar_datos_masivos(500, conceptos_por_legajo=4)
        
        # Medición inicial de memoria
        memoria_inicial = psutil.Process().memory_info().rss / 1024 / 1024
        print(f"📊 Memoria inicial: {memoria_inicial:.1f}MB")
        
        # Procesamiento con monitoreo de memoria
        memoria_picos = []
        
        def monitor_memoria():
            return psutil.Process().memory_info().rss / 1024 / 1024
        
        print("🔄 Ejecutando procesamiento con monitoreo de memoria...")
        start_time = time.time()
        
        # Medir memoria en diferentes puntos
        memoria_picos.append(('Inicio', monitor_memoria()))
        
        resultado = processor.procesar_datos_extraidos(datos)
        
        memoria_picos.append(('Post-procesamiento', monitor_memoria()))
        
        # Limpieza de memoria
        del datos
        gc.collect()
        
        memoria_picos.append(('Post-limpieza', monitor_memoria()))
        
        elapsed_time = time.time() - start_time
        
        # Análisis de memoria
        print(f"\n📊 ANÁLISIS DE MEMORIA:")
        print("-" * 30)
        
        for punto, memoria in memoria_picos:
            incremento = memoria - memoria_inicial
            print(f"   {punto}: {memoria:.1f}MB (+{incremento:.1f}MB)")
        
        memoria_maxima = max(m[1] for m in memoria_picos)
        memoria_total_usada = memoria_maxima - memoria_inicial
        
        print(f"\n🎯 RESUMEN MEMORIA:")
        print(f"   - Memoria máxima: {memoria_maxima:.1f}MB")
        print(f"   - Incremento total: {memoria_total_usada:.1f}MB")
        print(f"   - Memoria por legajo: {memoria_total_usada/500:.2f}MB/legajo")
        print(f"   - Tiempo procesamiento: {elapsed_time:.2f}s")
        
        if resultado['success']:
            print("✅ Test Performance Memoria: EXITOSO")
            return True
        else:
            print(f"❌ Error en procesamiento: {resultado.get('error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error en test memoria: {e}")
        return False

def test_performance_componentes():
    """Test de performance de componentes individuales"""
    print("\n🧪 TEST PERFORMANCE: Componentes Individuales")
    print("=" * 50)
    
    try:
        # Configurar sistema
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        from processors.conceptos_processor import ConceptosProcessor
        from processors.calculos_processor import CalculosSicossProcessor
        from processors.topes_processor import TopesProcessor
        
        # Generar datos de test
        datos = generar_datos_masivos(200, conceptos_por_legajo=4)
        
        # Test individual de cada componente
        componentes = [
            ("ConceptosProcessor", ConceptosProcessor(config)),
            ("CalculosProcessor", CalculosSicossProcessor(config)),
            ("TopesProcessor", TopesProcessor(config))
        ]
        
        profiler = PerformanceProfiler()
        resultados_componentes = {}
        
        # Simular pipeline paso a paso
        df_legajos = datos['legajos'].copy()
        
        for nombre, processor in componentes:
            print(f"\n🔄 Testing {nombre}...")
            
            try:
                profiler.iniciar_medicion(nombre)
                
                if nombre == "ConceptosProcessor":
                    df_resultado = processor.process(df_legajos, datos['conceptos'])
                else:
                    df_resultado = processor.process(df_legajos)
                
                metricas = profiler.finalizar_medicion(nombre, len(df_legajos))
                resultados_componentes[nombre] = metricas
                
                # Usar el resultado para el siguiente componente
                df_legajos = df_resultado
                
            except Exception as e:
                print(f"   ❌ Error en {nombre}: {e}")
                resultados_componentes[nombre] = {'error': str(e)}
        
        # Análisis comparativo
        print(f"\n📊 COMPARACIÓN DE COMPONENTES:")
        print("-" * 40)
        
        for nombre, metricas in resultados_componentes.items():
            if 'error' not in metricas:
                print(f"{nombre}:")
                print(f"   ⏱️ Tiempo: {metricas['tiempo_total']:.3f}s")
                print(f"   🚀 Throughput: {metricas['throughput_por_segundo']:.1f} legajos/s")
                print(f"   💾 Memoria: {metricas['memoria_usada_mb']:.1f}MB")
        
        print("✅ Test Performance Componentes: EXITOSO")
        return True
        
    except Exception as e:
        print(f"❌ Error en test componentes: {e}")
        return False

def generar_reporte_performance(resultados: Dict):
    """Genera reporte detallado de performance"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_reporte = f"reporte_performance_{timestamp}.txt"
    
    with open(archivo_reporte, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("REPORTE PERFORMANCE SICOSS BACKEND\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Escribir resultados detallados
        for test_name, datos in resultados.items():
            f.write(f"\n{test_name}:\n")
            f.write("-" * 30 + "\n")
            
            if isinstance(datos, list):
                for item in datos:
                    f.write(f"  {item}\n")
            else:
                f.write(f"  {datos}\n")
    
    print(f"📄 Reporte performance generado: {archivo_reporte}")
    return archivo_reporte

def main():
    """Ejecuta todos los tests de performance masivos"""
    print("🚀 INICIANDO TESTS DE PERFORMANCE MASIVOS")
    print("=" * 60)
    
    start_time = time.time()
    
    # Ejecutar tests de performance
    tests = [
        ("Escalabilidad", test_performance_escalabilidad),
        ("Memoria", test_performance_memoria),
        ("Componentes", test_performance_componentes)
    ]
    
    resultados = {}
    tests_exitosos = 0
    
    for nombre, test_func in tests:
        print(f"\n🎯 {nombre}")
        try:
            resultado = test_func()
            resultados[nombre] = resultado
            if resultado:
                tests_exitosos += 1
        except Exception as e:
            print(f"❌ Error en test {nombre}: {e}")
            resultados[nombre] = f"Error: {e}"
    
    # Resumen final
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"📊 RESUMEN TESTS PERFORMANCE: {tests_exitosos}/{len(tests)} exitosos")
    print(f"⏱️ Tiempo total tests: {elapsed_time:.2f}s")
    
    # Generar reporte
    generar_reporte_performance(resultados)
    
    if tests_exitosos == len(tests):
        print("\n🎉 TODOS LOS TESTS PERFORMANCE EXITOSOS!")
        print("🚀 Sistema validado para manejo de grandes volúmenes")
    else:
        print(f"\n⚠️ {len(tests) - tests_exitosos} tests fallaron")
    
    return tests_exitosos == len(tests)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 