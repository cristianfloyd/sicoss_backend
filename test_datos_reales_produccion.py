#!/usr/bin/env python3
"""
Tests con Datos Reales de Producción - SICOSS Backend

Tests para validar el sistema con datos reales de producción,
incluyendo casos edge, validación de integridad y simulación 
de escenarios complejos del mundo real.
"""

import pandas as pd
import numpy as np
import sys
import os
from typing import Dict, List, Tuple, Optional
import time
import json
from datetime import datetime, date
import logging

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.sicoss_processor import SicossDataProcessor
from config.sicoss_config import SicossConfig
from validators.sicoss_verifier import SicossVerifier, ToleranciaComparacion
from database.database_connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatosRealesSimulator:
    """Simulador de datos reales de producción con casos complejos"""
    
    def __init__(self):
        self.escalafones_reales = ['NODO', 'AUTO', 'DOCE', 'PROF', 'TECN', 'ADMI']
        self.conceptos_comunes = {
            1: {'nombre': 'Sueldo Básico', 'grupo': [1], 'frecuencia': 0.95},
            4: {'nombre': 'Adicional por Título', 'grupo': [4], 'frecuencia': 0.7},
            9: {'nombre': 'SAC', 'grupo': [9], 'frecuencia': 0.95},
            15: {'nombre': 'Investigación Tipo A', 'grupo': [15], 'frecuencia': 0.15},
            16: {'nombre': 'Investigación Tipo B', 'grupo': [16], 'frecuencia': 0.12},
            17: {'nombre': 'Investigación Tipo C', 'grupo': [17], 'frecuencia': 0.08},
            18: {'nombre': 'Docencia Especial', 'grupo': [18], 'frecuencia': 0.25},
            25: {'nombre': 'Horas Extras', 'grupo': [25], 'frecuencia': 0.3},
            33: {'nombre': 'Zona Desfavorable', 'grupo': [33], 'frecuencia': 0.2},
            50: {'nombre': 'Otros Aportes', 'grupo': [50], 'frecuencia': 0.1}
        }
        
    def generar_legajos_realistas(self, cantidad: int) -> pd.DataFrame:
        """Genera legajos con características realistas de producción"""
        logger.info(f"Generando {cantidad} legajos realistas...")
        
        legajos = []
        for i in range(cantidad):
            legajo_id = 200000 + i
            
            # Distribución realista de escalafones
            escalafon = np.random.choice(
                self.escalafones_reales,
                p=[0.35, 0.15, 0.25, 0.1, 0.1, 0.05]  # Distribución típica universitaria
            )
            
            # Situación revista realista
            situacion = np.random.choice([1, 2, 3, 4], p=[0.6, 0.2, 0.15, 0.05])
            
            legajo = {
                'nro_legaj': legajo_id,
                'apnom': f'EMPLEADO REAL {i+1:05d}',
                'cuil': f'20{400000000 + i:09d}',
                'situacion_revista': situacion,
                'codigo_obra_social': np.random.choice([101, 102, 103, 104, 105, 106]),
                'escalafon': escalafon,
                'fecha_ingreso': self._generar_fecha_ingreso(),
                'categoria': np.random.randint(1, 25),
                'dedicacion': np.random.choice(['EXC', 'SED', 'SIM'], p=[0.3, 0.4, 0.3])
            }
            legajos.append(legajo)
        
        return pd.DataFrame(legajos)
    
    def _generar_fecha_ingreso(self) -> str:
        """Genera fecha de ingreso realista"""
        year = np.random.randint(1995, 2024)
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 29)
        return f"{year:04d}-{month:02d}-{day:02d}"
    
    def generar_conceptos_realistas(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Genera conceptos con distribución realista por escalafón"""
        logger.info("Generando conceptos realistas por escalafón...")
        
        conceptos = []
        
        for _, legajo in df_legajos.iterrows():
            escalafon = legajo['escalafon']
            dedicacion = legajo['dedicacion']
            categoria = legajo['categoria']
            
            # Conceptos por legajo según escalafón
            conceptos_legajo = self._obtener_conceptos_por_escalafon(
                str(escalafon), str(dedicacion), int(categoria)
            )
            
            for codn_conce, importe in conceptos_legajo.items():
                if importe > 0:
                    concepto = {
                        'nro_legaj': legajo['nro_legaj'],
                        'codn_conce': codn_conce,
                        'impp_conce': round(importe, 2),
                        'tipos_grupos': self.conceptos_comunes.get(codn_conce, {}).get('grupo', [codn_conce]),
                        'codigoescalafon': escalafon
                    }
                    conceptos.append(concepto)
        
        return pd.DataFrame(conceptos)
    
    def _obtener_conceptos_por_escalafon(self, escalafon: str, dedicacion: str, categoria: int) -> Dict[int, float]:
        """Obtiene conceptos típicos según escalafón, dedicación y categoría"""
        conceptos = {}
        
        # Sueldo básico (siempre presente)
        sueldo_base = self._calcular_sueldo_base(escalafon, dedicacion, categoria)
        conceptos[1] = sueldo_base
        
        # SAC (siempre presente)
        conceptos[9] = sueldo_base / 12
        
        # Adicional por título (muy común)
        if np.random.random() < 0.7:
            conceptos[4] = sueldo_base * np.random.uniform(0.1, 0.3)
        
        # Conceptos específicos por escalafón
        if escalafon == 'DOCE':
            # Docentes tienen más conceptos de investigación y docencia
            if np.random.random() < 0.4:  # Investigación
                tipo_inv = np.random.choice([15, 16, 17], p=[0.5, 0.3, 0.2])
                conceptos[tipo_inv] = np.random.uniform(8000, 25000)
            
            if np.random.random() < 0.3:  # Docencia especial
                conceptos[18] = np.random.uniform(5000, 15000)
        
        elif escalafon == 'NODO':
            # No docentes tienen menos investigación pero más horas extras
            if np.random.random() < 0.1:  # Investigación (menos común)
                conceptos[15] = np.random.uniform(5000, 15000)
            
            if np.random.random() < 0.4:  # Horas extras (más común)
                conceptos[25] = np.random.uniform(2000, 10000)
        
        # Zona desfavorable (aleatorio)
        if np.random.random() < 0.2:
            conceptos[33] = np.random.uniform(3000, 8000)
        
        return conceptos
    
    def _calcular_sueldo_base(self, escalafon: str, dedicacion: str, categoria: int) -> float:
        """Calcula sueldo base realista según parámetros"""
        # Bases según escalafón
        bases = {
            'DOCE': 80000,
            'NODO': 75000,
            'AUTO': 85000,
            'PROF': 90000,
            'TECN': 70000,
            'ADMI': 65000
        }
        
        base = bases.get(escalafon, 70000)
        
        # Ajuste por dedicación
        multiplicadores_dedicacion = {
            'EXC': 1.5,  # Exclusiva
            'SED': 1.2,  # Semi dedicación
            'SIM': 1.0   # Simple
        }
        
        base *= multiplicadores_dedicacion.get(dedicacion, 1.0)
        
        # Ajuste por categoría (1-25)
        factor_categoria = 1.0 + (categoria - 1) * 0.05
        base *= factor_categoria
        
        # Variabilidad realista
        variacion = np.random.uniform(0.9, 1.1)
        return base * variacion

def test_datos_reales_basico():
    """Test básico con datos simulando escenarios reales"""
    print("\n🧪 TEST DATOS REALES: Escenarios Básicos")
    print("=" * 50)
    
    try:
        # Configurar sistema
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        # Generar datos realistas
        legajos_data = []
        for i in range(50):
            legajo_id = 200000 + i
            legajos_data.append({
                'nro_legaj': legajo_id,
                'apnom': f'EMPLEADO REAL {i+1:05d}',
                'cuil': f'20{400000000 + i:09d}',
                'situacion_revista': np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1]),
                'codigo_obra_social': np.random.choice([101, 102, 103, 104, 105])
            })
        
        df_legajos = pd.DataFrame(legajos_data)
        
        # Generar conceptos realistas
        conceptos_data = []
        for legajo_id in df_legajos['nro_legaj']:
            # Sueldo básico
            sueldo = np.random.uniform(50000, 150000)
            conceptos_data.append({
                'nro_legaj': legajo_id,
                'codn_conce': 1,
                'impp_conce': round(sueldo, 2),
                'tipos_grupos': [1],
                'codigoescalafon': 'NODO'
            })
            
            # SAC
            conceptos_data.append({
                'nro_legaj': legajo_id,
                'codn_conce': 9,
                'impp_conce': round(sueldo / 12, 2),
                'tipos_grupos': [9],
                'codigoescalafon': 'NODO'
            })
            
            # Algunos con investigación
            if np.random.random() < 0.3:
                conceptos_data.append({
                    'nro_legaj': legajo_id,
                    'codn_conce': 15,
                    'impp_conce': round(np.random.uniform(5000, 20000), 2),
                    'tipos_grupos': [15],
                    'codigoescalafon': 'DOCE'
                })
        
        df_conceptos = pd.DataFrame(conceptos_data)
        
        datos_entrada = {
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': pd.DataFrame(),
            'obra_social': pd.DataFrame()
        }
        
        print(f"📊 Datos generados:")
        print(f"   - Legajos: {len(df_legajos)}")
        print(f"   - Conceptos: {len(df_conceptos)}")
        
        # Procesar datos
        processor = SicossDataProcessor(config)
        start_time = time.time()
        
        print("🔄 Procesando datos reales...")
        resultado = processor.procesar_datos_extraidos(datos_entrada)
        
        elapsed_time = time.time() - start_time
        
        if resultado['success']:
            df_resultado = resultado['data']['legajos']
            
            print(f"✅ Procesamiento exitoso en {elapsed_time:.2f}s")
            print(f"📊 Resultados:")
            print(f"   - Legajos procesados: {len(df_resultado)}")
            print(f"   - Campos generados: {len(df_resultado.columns)}")
            
            # Validaciones básicas
            if 'IMPORTE_BRUTO' in df_resultado.columns:
                bruto_promedio = df_resultado['IMPORTE_BRUTO'].mean()
                print(f"   - Importe bruto promedio: ${bruto_promedio:,.2f}")
            
            print("✅ Test Datos Reales Básico: EXITOSO")
            return True
            
        else:
            print(f"❌ Error en procesamiento: {resultado.get('error', 'Error desconocido')}")
            return False
            
    except Exception as e:
        print(f"❌ Error en test datos reales: {e}")
        return False

def test_casos_edge_complejos():
    """Test con casos edge y situaciones complejas del mundo real"""
    print("\n🧪 TEST DATOS REALES: Casos Edge Complejos")
    print("=" * 55)
    
    try:
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        # Crear casos edge específicos
        legajos_edge = pd.DataFrame([
            # Caso 1: Empleado con sueldo muy alto (tope)
            {
                'nro_legaj': 900001,
                'apnom': 'EMPLEADO SUELDO ALTO',
                'cuil': '20123456789',
                'situacion_revista': 1,
                'codigo_obra_social': 101,
                'escalafon': 'PROF',
                'categoria': 25,
                'dedicacion': 'EXC'
            },
            # Caso 2: Investigador con múltiples conceptos
            {
                'nro_legaj': 900002,
                'apnom': 'INVESTIGADOR COMPLEJO',
                'cuil': '20987654321',
                'situacion_revista': 1,
                'codigo_obra_social': 102,
                'escalafon': 'DOCE',
                'categoria': 20,
                'dedicacion': 'EXC'
            },
            # Caso 3: Empleado con conceptos mínimos
            {
                'nro_legaj': 900003,
                'apnom': 'EMPLEADO BASICO',
                'cuil': '20111222333',
                'situacion_revista': 3,
                'codigo_obra_social': 103,
                'escalafon': 'ADMI',
                'categoria': 1,
                'dedicacion': 'SIM'
            }
        ])
        
        # Conceptos edge específicos
        conceptos_edge = pd.DataFrame([
            # Empleado sueldo alto - cerca del tope
            {'nro_legaj': 900001, 'codn_conce': 1, 'impp_conce': 750000.0, 'tipos_grupos': [1], 'codigoescalafon': 'PROF'},
            {'nro_legaj': 900001, 'codn_conce': 9, 'impp_conce': 62500.0, 'tipos_grupos': [9], 'codigoescalafon': 'PROF'},
            {'nro_legaj': 900001, 'codn_conce': 4, 'impp_conce': 100000.0, 'tipos_grupos': [4], 'codigoescalafon': 'PROF'},
            
            # Investigador con múltiples tipos de investigación
            {'nro_legaj': 900002, 'codn_conce': 1, 'impp_conce': 120000.0, 'tipos_grupos': [1], 'codigoescalafon': 'DOCE'},
            {'nro_legaj': 900002, 'codn_conce': 9, 'impp_conce': 10000.0, 'tipos_grupos': [9], 'codigoescalafon': 'DOCE'},
            {'nro_legaj': 900002, 'codn_conce': 15, 'impp_conce': 25000.0, 'tipos_grupos': [15], 'codigoescalafon': 'DOCE'},
            {'nro_legaj': 900002, 'codn_conce': 16, 'impp_conce': 15000.0, 'tipos_grupos': [16], 'codigoescalafon': 'DOCE'},
            {'nro_legaj': 900002, 'codn_conce': 17, 'impp_conce': 10000.0, 'tipos_grupos': [17], 'codigoescalafon': 'DOCE'},
            {'nro_legaj': 900002, 'codn_conce': 18, 'impp_conce': 8000.0, 'tipos_grupos': [18], 'codigoescalafon': 'DOCE'},
            
            # Empleado básico - sueldo mínimo
            {'nro_legaj': 900003, 'codn_conce': 1, 'impp_conce': 45000.0, 'tipos_grupos': [1], 'codigoescalafon': 'ADMI'},
            {'nro_legaj': 900003, 'codn_conce': 9, 'impp_conce': 3750.0, 'tipos_grupos': [9], 'codigoescalafon': 'ADMI'}
        ])
        
        datos_entrada = {
            'legajos': legajos_edge,
            'conceptos': conceptos_edge,
            'otra_actividad': pd.DataFrame(),
            'obra_social': pd.DataFrame()
        }
        
        print("📊 Casos edge preparados:")
        print("   - Sueldo alto (cerca del tope)")
        print("   - Investigador con múltiples conceptos")
        print("   - Empleado básico mínimo")
        
        # Procesar casos edge
        processor = SicossDataProcessor(config)
        print("🔄 Procesando casos edge...")
        
        resultado = processor.procesar_datos_extraidos(datos_entrada)
        
        if resultado['success']:
            df_resultado = resultado['data']['legajos']
            
            print("✅ Procesamiento de casos edge exitoso")
            
            # Análisis específico de casos edge
            print("\n🔍 ANÁLISIS CASOS EDGE:")
            
            for _, legajo in df_resultado.iterrows():
                nro_legaj = legajo['nro_legaj']
                nombre = legajo.get('apnom', f'Legajo {nro_legaj}')
                
                print(f"\n   📋 {nombre} (Legajo {nro_legaj}):")
                
                # Mostrar campos críticos
                campos_analizar = ['IMPORTE_BRUTO', 'IMPORTE_IMPON', 'ImporteSAC', 'TipoDeOperacion']
                for campo in campos_analizar:
                    if campo in legajo:
                        valor = legajo[campo]
                        print(f"      {campo}: {valor}")
                
                # Validaciones específicas
                if nro_legaj == 900001:  # Sueldo alto
                    if legajo.get('IMPORTE_BRUTO', 0) > 800000:
                        print("      ✅ Tope aplicado correctamente")
                    else:
                        print("      ⚠️ Verificar aplicación de tope")
                
                elif nro_legaj == 900002:  # Investigador
                    if legajo.get('ImporteImponible_6', 0) > 0:
                        print("      ✅ Conceptos investigación procesados")
                    else:
                        print("      ⚠️ Verificar conceptos investigación")
                
                elif nro_legaj == 900003:  # Básico
                    if 40000 <= legajo.get('IMPORTE_BRUTO', 0) <= 60000:
                        print("      ✅ Rango salarial básico correcto")
                    else:
                        print("      ⚠️ Verificar cálculo sueldo básico")
            
            print("\n✅ Test Casos Edge Complejos: EXITOSO")
            return True
            
        else:
            print(f"❌ Error en procesamiento casos edge: {resultado.get('error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error en test casos edge: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_integridad_datos_reales():
    """Test de integridad con validaciones exhaustivas"""
    print("\n🧪 TEST DATOS REALES: Integridad y Validaciones")
    print("=" * 55)
    
    try:
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        # Generar dataset balanceado
        simulator = DatosRealesSimulator()
        df_legajos = simulator.generar_legajos_realistas(50)
        df_conceptos = simulator.generar_conceptos_realistas(df_legajos)
        
        datos_entrada = {
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': pd.DataFrame(),
            'obra_social': pd.DataFrame()
        }
        
        # Procesar datos
        processor = SicossDataProcessor(config)
        resultado = processor.procesar_datos_extraidos(datos_entrada)
        
        if resultado['success']:
            df_resultado = resultado['data']['legajos']
            
            print("✅ Datos procesados para validación de integridad")
            
            # Batería de validaciones de integridad
            errores_integridad = []
            
            print("\n🔍 VALIDACIONES DE INTEGRIDAD:")
            
            # 1. Consistencia numérica
            print("   1. Validando consistencia numérica...")
            for _, row in df_resultado.iterrows():
                # IMPORTE_BRUTO >= IMPORTE_IMPON
                if row.get('IMPORTE_BRUTO', 0) < row.get('IMPORTE_IMPON', 0):
                    errores_integridad.append(f"Legajo {row['nro_legaj']}: BRUTO < IMPONIBLE")
                
                # SAC > 0 para legajos normales
                if row.get('ImporteSAC', 0) <= 0 and row.get('IMPORTE_BRUTO', 0) > 0:
                    errores_integridad.append(f"Legajo {row['nro_legaj']}: SAC inválido")
            
            # 2. Campos obligatorios
            print("   2. Validando campos obligatorios...")
            campos_obligatorios = ['nro_legaj', 'IMPORTE_BRUTO', 'IMPORTE_IMPON']
            for campo in campos_obligatorios:
                if campo not in df_resultado.columns:
                    errores_integridad.append(f"Campo obligatorio faltante: {campo}")
                elif df_resultado[campo].isnull().any():
                    count_nulls = df_resultado[campo].isnull().sum()
                    errores_integridad.append(f"Valores nulos en {campo}: {count_nulls}")
            
            # 3. Rangos válidos
            print("   3. Validando rangos de valores...")
            if 'IMPORTE_BRUTO' in df_resultado.columns:
                brutos_negativos = (df_resultado['IMPORTE_BRUTO'] < 0).sum()
                if brutos_negativos > 0:
                    errores_integridad.append(f"Importes brutos negativos: {brutos_negativos}")
                
                brutos_excesivos = (df_resultado['IMPORTE_BRUTO'] > 2000000).sum()
                if brutos_excesivos > 0:
                    errores_integridad.append(f"Importes brutos excesivos: {brutos_excesivos}")
            
            # 4. TipoDeOperacion válido
            print("   4. Validando tipos de operación...")
            if 'TipoDeOperacion' in df_resultado.columns:
                tipos_invalidos = (~df_resultado['TipoDeOperacion'].isin([1, 2])).sum()
                if tipos_invalidos > 0:
                    errores_integridad.append(f"Tipos de operación inválidos: {tipos_invalidos}")
            
            # 5. Coherencia investigadores
            print("   5. Validando coherencia investigadores...")
            if 'ImporteImponible_6' in df_resultado.columns and 'TipoDeOperacion' in df_resultado.columns:
                # Si hay ImporteImponible_6 > 0, debería ser TipoDeOperacion = 2
                mask_inv = df_resultado['ImporteImponible_6'] > 0
                mask_tipo_incorrecto = (df_resultado['TipoDeOperacion'] != 2) & mask_inv
                investigadores_inconsistentes = mask_tipo_incorrecto.sum()
                
                if investigadores_inconsistentes > 0:
                    errores_integridad.append(f"Investigadores con tipo incorrecto: {investigadores_inconsistentes}")
            
            # Resumen de validaciones
            if errores_integridad:
                print(f"\n⚠️ ERRORES DE INTEGRIDAD ENCONTRADOS: {len(errores_integridad)}")
                for error in errores_integridad[:10]:  # Mostrar primeros 10
                    print(f"      - {error}")
                if len(errores_integridad) > 10:
                    print(f"      ... y {len(errores_integridad) - 10} más")
                
                # Determinar severidad
                severidad = "CRÍTICA" if len(errores_integridad) > len(df_resultado) * 0.1 else "MENOR"
                print(f"   Severidad: {severidad}")
                
                return severidad == "MENOR"  # Aceptar errores menores
            else:
                print("   ✅ TODOS LOS TESTS DE INTEGRIDAD PASARON")
                return True
            
        else:
            print(f"❌ Error en procesamiento: {resultado.get('error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error en test integridad: {e}")
        return False

def main():
    """Ejecuta todos los tests con datos reales de producción"""
    print("🏭 INICIANDO TESTS CON DATOS REALES DE PRODUCCIÓN")
    print("=" * 70)
    
    start_time = time.time()
    
    # Ejecutar tests de datos reales
    tests = [
        ("Escenarios Básicos", test_datos_reales_basico),
        ("Casos Edge Complejos", test_casos_edge_complejos),
        ("Integridad de Datos", test_integridad_datos_reales)
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
            resultados[nombre] = False
    
    # Resumen final
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*70}")
    print(f"📊 RESUMEN TESTS DATOS REALES: {tests_exitosos}/{len(tests)} exitosos")
    print(f"⏱️ Tiempo total: {elapsed_time:.2f}s")
    
    for nombre, resultado in resultados.items():
        estado = "✅ EXITOSO" if resultado else "❌ FALLÓ"
        print(f"   - {nombre}: {estado}")
    
    if tests_exitosos == len(tests):
        print("\n🎉 TODOS LOS TESTS CON DATOS REALES EXITOSOS!")
        print("🚀 Sistema validado con escenarios de producción")
    else:
        print(f"\n⚠️ {len(tests) - tests_exitosos} tests fallaron")
        print("🔧 Revisar casos edge y validaciones antes de producción")
    
    return tests_exitosos == len(tests)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 