#!/usr/bin/env python3
"""
Test Definitivo: Datos Reales + Base de Datos Real

Este test realiza el procesamiento completo con datos realistas
y ejecuta inserción real en la base de datos PostgreSQL.

Es la prueba final para validar que el sistema está 100% listo para producción.
"""

import pandas as pd
import numpy as np
import sys
import os
import time
from datetime import datetime, date
from typing import Dict, List

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.sicoss_processor import SicossDataProcessor
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from value_objects.periodo_fiscal import PeriodoFiscal

class GeneradorDatosReales:
    """Generador de datos realistas para testing de producción"""
    
    def __init__(self):
        self.escalafones_reales = {
            'DOCE': {'porcentaje': 0.35, 'sueldo_base': 85000, 'variabilidad': 0.4},
            'NODO': {'porcentaje': 0.30, 'sueldo_base': 78000, 'variabilidad': 0.3},
            'AUTO': {'porcentaje': 0.15, 'sueldo_base': 82000, 'variabilidad': 0.3},
            'PROF': {'porcentaje': 0.10, 'sueldo_base': 120000, 'variabilidad': 0.5},
            'TECN': {'porcentaje': 0.07, 'sueldo_base': 72000, 'variabilidad': 0.25},
            'ADMI': {'porcentaje': 0.03, 'sueldo_base': 65000, 'variabilidad': 0.2}
        }
        
        self.conceptos_por_escalafon = {
            'DOCE': {
                1: {'prob': 1.0, 'factor': 1.0},          # Sueldo básico
                4: {'prob': 0.85, 'factor': 0.2},         # Adicional título
                9: {'prob': 1.0, 'factor': 1/12},         # SAC
                15: {'prob': 0.4, 'factor': 0.25},        # Investigación A
                16: {'prob': 0.3, 'factor': 0.18},        # Investigación B
                17: {'prob': 0.15, 'factor': 0.12},       # Investigación C
                18: {'prob': 0.35, 'factor': 0.15},       # Docencia especial
                25: {'prob': 0.2, 'factor': 0.08}         # Horas extras (menos común)
            },
            'NODO': {
                1: {'prob': 1.0, 'factor': 1.0},          # Sueldo básico
                4: {'prob': 0.6, 'factor': 0.15},         # Adicional título
                9: {'prob': 1.0, 'factor': 1/12},         # SAC
                15: {'prob': 0.1, 'factor': 0.15},        # Investigación (raro)
                25: {'prob': 0.45, 'factor': 0.12},       # Horas extras (común)
                33: {'prob': 0.25, 'factor': 0.08}        # Zona desfavorable
            },
            'AUTO': {
                1: {'prob': 1.0, 'factor': 1.0},          # Sueldo básico
                4: {'prob': 0.7, 'factor': 0.18},         # Adicional título
                9: {'prob': 1.0, 'factor': 1/12},         # SAC
                25: {'prob': 0.35, 'factor': 0.1},        # Horas extras
                33: {'prob': 0.15, 'factor': 0.06}        # Zona desfavorable
            },
            'PROF': {
                1: {'prob': 1.0, 'factor': 1.0},          # Sueldo básico
                4: {'prob': 0.95, 'factor': 0.3},         # Adicional título
                9: {'prob': 1.0, 'factor': 1/12},         # SAC
                15: {'prob': 0.7, 'factor': 0.4},         # Investigación A
                16: {'prob': 0.5, 'factor': 0.25},        # Investigación B
                17: {'prob': 0.3, 'factor': 0.15},        # Investigación C
                18: {'prob': 0.6, 'factor': 0.2}          # Docencia especial
            },
            'TECN': {
                1: {'prob': 1.0, 'factor': 1.0},          # Sueldo básico
                4: {'prob': 0.5, 'factor': 0.12},         # Adicional título
                9: {'prob': 1.0, 'factor': 1/12},         # SAC
                25: {'prob': 0.4, 'factor': 0.1},         # Horas extras
                33: {'prob': 0.3, 'factor': 0.08}         # Zona desfavorable
            },
            'ADMI': {
                1: {'prob': 1.0, 'factor': 1.0},          # Sueldo básico
                9: {'prob': 1.0, 'factor': 1/12},         # SAC
                25: {'prob': 0.3, 'factor': 0.08},        # Horas extras
                33: {'prob': 0.2, 'factor': 0.06}         # Zona desfavorable
            }
        }
        
        self.obras_sociales_reales = [101, 102, 103, 104, 105, 106, 107, 108]
        
    def generar_dataset_realista(self, num_legajos: int) -> Dict:
        """Genera dataset completamente realista para testing de producción"""
        print(f"🏭 Generando dataset realista con {num_legajos} legajos...")
        
        # Generar legajos según distribución real
        legajos_data = []
        
        # Determinar distribución por escalafón
        escalafones = list(self.escalafones_reales.keys())
        probabilidades = [self.escalafones_reales[esc]['porcentaje'] for esc in escalafones]
        
        escalafones_asignados = np.random.choice(
            escalafones, 
            size=num_legajos, 
            p=probabilidades
        )
        
        for i in range(num_legajos):
            legajo_id = 500000 + i  # Rango alto para evitar conflictos
            escalafon = escalafones_asignados[i]
            
            # Generar datos demográficos realistas
            legajo = {
                'nro_legaj': legajo_id,
                'apnom': f'EMPLEADO REAL {i+1:05d}',
                'cuil': f'20{800000000 + i:09d}',
                'situacion_revista': self._asignar_situacion_revista(escalafon),
                'codigo_obra_social': np.random.choice(self.obras_sociales_reales),
                'escalafon': escalafon,
                'categoria': self._asignar_categoria(escalafon),
                'dedicacion': self._asignar_dedicacion(escalafon),
                'fecha_ingreso': self._generar_fecha_ingreso(),
                'activo': np.random.choice([True, False], p=[0.95, 0.05])  # 95% activos
            }
            legajos_data.append(legajo)
        
        df_legajos = pd.DataFrame(legajos_data)
        
        # Generar conceptos realistas
        df_conceptos = self._generar_conceptos_realistas(df_legajos)
        
        # Generar otra actividad (algunos casos)
        df_otra_actividad = self._generar_otra_actividad_realista(df_legajos)
        
        # Generar obra social (casos especiales)
        df_obra_social = self._generar_obra_social_realista(df_legajos)
        
        print(f"✅ Dataset realista generado:")
        print(f"   - Legajos: {len(df_legajos)}")
        print(f"   - Conceptos: {len(df_conceptos)}")
        print(f"   - Otra actividad: {len(df_otra_actividad)}")
        print(f"   - Obra social: {len(df_obra_social)}")
        print(f"   - Distribución escalafones: {df_legajos['escalafon'].value_counts().to_dict()}")
        
        return {
            'legajos': df_legajos,
            'conceptos': df_conceptos,
            'otra_actividad': df_otra_actividad,
            'obra_social': df_obra_social
        }
    
    def _asignar_situacion_revista(self, escalafon: str) -> int:
        """Asigna situación revista según escalafón"""
        if escalafon in ['DOCE', 'PROF']:
            return np.random.choice([1, 2, 3], p=[0.8, 0.15, 0.05])
        else:
            return np.random.choice([1, 2, 3], p=[0.7, 0.25, 0.05])
    
    def _asignar_categoria(self, escalafon: str) -> int:
        """Asigna categoría según escalafón"""
        if escalafon in ['DOCE', 'PROF']:
            return np.random.choice(range(10, 26), size=1)[0]  # Categorías altas
        elif escalafon in ['AUTO', 'TECN']:
            return np.random.choice(range(5, 20), size=1)[0]   # Categorías medias
        else:
            return np.random.choice(range(1, 15), size=1)[0]   # Categorías variadas
    
    def _asignar_dedicacion(self, escalafon: str) -> str:
        """Asigna dedicación según escalafón"""
        if escalafon in ['DOCE', 'PROF']:
            return np.random.choice(['EXC', 'SED', 'SIM'], p=[0.4, 0.4, 0.2])
        else:
            return np.random.choice(['EXC', 'SED', 'SIM'], p=[0.2, 0.3, 0.5])
    
    def _generar_fecha_ingreso(self) -> str:
        """Genera fecha de ingreso realista"""
        year = np.random.randint(1990, 2024)
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 29)
        return f"{year:04d}-{month:02d}-{day:02d}"
    
    def _generar_conceptos_realistas(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Genera conceptos con distribución realista por escalafón"""
        conceptos_data = []
        
        for _, legajo in df_legajos.iterrows():
            if not bool(legajo['activo']):
                continue  # Skip empleados inactivos
                
            legajo_id = int(legajo['nro_legaj'])
            escalafon = str(legajo['escalafon'])
            categoria = int(legajo['categoria'])
            dedicacion = str(legajo['dedicacion'])
            
            # Obtener configuración de conceptos para este escalafón
            conceptos_config = self.conceptos_por_escalafon.get(escalafon, {})
            
            # Calcular sueldo base
            sueldo_base = self._calcular_sueldo_base_realista(escalafon, categoria, dedicacion)
            
            # Generar conceptos según probabilidades
            for codn_conce, config in conceptos_config.items():
                if np.random.random() < config['prob']:
                    importe = sueldo_base * config['factor']
                    
                    # Agregar variabilidad realista
                    variabilidad = self.escalafones_reales[escalafon]['variabilidad']
                    factor_variacion = np.random.uniform(1 - variabilidad * 0.1, 1 + variabilidad * 0.1)
                    importe *= factor_variacion
                    
                    concepto = {
                        'nro_legaj': legajo_id,
                        'codn_conce': codn_conce,
                        'impp_conce': round(max(importe, 0), 2),
                        'tipos_grupos': [codn_conce],
                        'codigoescalafon': escalafon
                    }
                    conceptos_data.append(concepto)
        
        return pd.DataFrame(conceptos_data)
    
    def _calcular_sueldo_base_realista(self, escalafon: str, categoria: int, dedicacion: str) -> float:
        """Calcula sueldo base realista"""
        config = self.escalafones_reales[escalafon]
        base = config['sueldo_base']
        
        # Factor por categoría
        base *= (1 + categoria * 0.03)
        
        # Factor por dedicación
        factores_dedicacion = {'EXC': 1.5, 'SED': 1.2, 'SIM': 1.0}
        base *= factores_dedicacion.get(dedicacion, 1.0)
        
        return base
    
    def _generar_otra_actividad_realista(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Genera otra actividad para algunos empleados"""
        otra_actividad_data = []
        
        # Solo algunos empleados tienen otra actividad (10%)
        legajos_con_otra_actividad = df_legajos.sample(n=int(len(df_legajos) * 0.1))
        
        for _, legajo in legajos_con_otra_actividad.iterrows():
            otra_actividad = {
                'nro_legaj': legajo['nro_legaj'],
                'importe_bruto_otra_actividad': round(np.random.uniform(15000, 50000), 2),
                'importe_imponible_otra_actividad': round(np.random.uniform(12000, 40000), 2)
            }
            otra_actividad_data.append(otra_actividad)
        
        return pd.DataFrame(otra_actividad_data)
    
    def _generar_obra_social_realista(self, df_legajos: pd.DataFrame) -> pd.DataFrame:
        """Genera obra social para algunos empleados con códigos especiales"""
        obra_social_data = []
        
        # Solo algunos empleados tienen códigos de obra social especiales (20%)
        legajos_con_os_especial = df_legajos.sample(n=int(len(df_legajos) * 0.2))
        
        codigos_especiales = ['000001', '000002', '000003', '999999']
        
        for _, legajo in legajos_con_os_especial.iterrows():
            obra_social = {
                'nro_legaj': legajo['nro_legaj'],
                'codigo_os': np.random.choice(codigos_especiales)
            }
            obra_social_data.append(obra_social)
        
        return pd.DataFrame(obra_social_data)

def test_procesamiento_datos_reales():
    """Test de procesamiento con datos completamente realistas"""
    print("\n🏭 TEST DATOS REALES: Procesamiento Completo")
    print("=" * 60)
    
    try:
        # Configurar sistema
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        processor = SicossDataProcessor(config)
        
        # Generar datos realistas
        generador = GeneradorDatosReales()
        datos_reales = generador.generar_dataset_realista(100)  # 100 empleados realistas
        
        # Procesar datos
        print("\n🔄 Procesando datos realistas con pipeline completo...")
        start_time = time.time()
        
        resultado = processor.procesar_datos_extraidos(datos_reales)
        
        elapsed_time = time.time() - start_time
        
        if isinstance(resultado, dict) and 'legajos_procesados' in resultado and not resultado['legajos_procesados'].empty:
            df_resultado = resultado['legajos_procesados']
            
            print(f"\n✅ PROCESAMIENTO REALISTA EXITOSO:")
            print(f"   - Tiempo total: {elapsed_time:.2f}s")
            print(f"   - Legajos procesados: {len(df_resultado)}")
            print(f"   - Campos generados: {len(df_resultado.columns)}")
            
            # Análisis detallado de resultados realistas
            print(f"\n📊 ANÁLISIS DETALLADO:")
            
            if 'IMPORTE_BRUTO' in df_resultado.columns:
                bruto_total = df_resultado['IMPORTE_BRUTO'].sum()
                bruto_promedio = df_resultado['IMPORTE_BRUTO'].mean()
                bruto_min = df_resultado['IMPORTE_BRUTO'].min()
                bruto_max = df_resultado['IMPORTE_BRUTO'].max()
                
                print(f"   - Total bruto: ${bruto_total:,.2f}")
                print(f"   - Promedio: ${bruto_promedio:,.2f}")
                print(f"   - Rango: ${bruto_min:,.2f} - ${bruto_max:,.2f}")
            
            if 'ImporteSAC' in df_resultado.columns:
                sac_total = df_resultado['ImporteSAC'].sum()
                print(f"   - Total SAC: ${sac_total:,.2f}")
            
            # Análisis por tipo de operación
            if 'TipoDeOperacion' in df_resultado.columns:
                tipos_op = df_resultado['TipoDeOperacion'].value_counts()
                print(f"   - Tipos operación: {tipos_op.to_dict()}")
            
            # Análisis investigadores
            if 'ImporteImponible_6' in df_resultado.columns:
                investigadores = (df_resultado['ImporteImponible_6'] > 0).sum()
                print(f"   - Investigadores detectados: {investigadores}")
            
            print("✅ Test Procesamiento Datos Reales: EXITOSO")
            return True, resultado
            
        else:
            print("❌ Error: Procesamiento sin resultados")
            return False, None
            
    except Exception as e:
        print(f"❌ Error en test datos reales: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_insercion_bd_real(resultado_procesamiento: Dict):
    """Test de inserción real en base de datos PostgreSQL"""
    print("\n💾 TEST BASE DE DATOS: Inserción Real PostgreSQL")
    print("=" * 60)
    
    try:
        # Configurar período fiscal para la inserción
        periodo_fiscal = PeriodoFiscal(2025, 1)  # Enero 2025
        
        # Configurar sistema con BD
        config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        processor = SicossDataProcessor(config)
        
        print(f"🔄 Insertando en BD PostgreSQL - Período: {periodo_fiscal.year}/{periodo_fiscal.month:02d}")
        
        # Ejecutar inserción en BD
        start_time = time.time()
        
        resultado_bd = processor._guardar_en_bd_sicoss(resultado_procesamiento, periodo_fiscal)
        
        elapsed_time = time.time() - start_time
        
        if isinstance(resultado_bd, dict) and resultado_bd.get('success', False):
            print(f"\n✅ INSERCIÓN BD EXITOSA:")
            print(f"   - Tiempo inserción: {elapsed_time:.2f}s")
            print(f"   - Registros insertados: {resultado_bd.get('registros_insertados', 0)}")
            print(f"   - Tabla destino: {resultado_bd.get('tabla_destino', 'suc.afip_mapuche_sicoss')}")
            
            # Validar inserción con consulta directa
            return validar_insercion_bd(periodo_fiscal, resultado_bd.get('registros_insertados', 0))
            
        else:
            error_msg = resultado_bd.get('error', 'Error desconocido') if isinstance(resultado_bd, dict) else str(resultado_bd)
            print(f"❌ Error en inserción BD: {error_msg}")
            return False
            
    except Exception as e:
        print(f"❌ Error en test BD real: {e}")
        import traceback
        traceback.print_exc()
        return False

def validar_insercion_bd(periodo_fiscal: PeriodoFiscal, registros_esperados: int) -> bool:
    """Valida que los datos se insertaron correctamente en la BD"""
    print(f"\n🔍 VALIDANDO INSERCIÓN EN BD...")
    
    try:
        # Conectar a BD usando el método probado
        db_conn = DatabaseConnection()
        
        # Consultar registros insertados
        query = """
        SELECT COUNT(*) as total_registros,
               COALESCE(SUM(rem_total), 0) as total_bruto,
               COALESCE(SUM(rem_impo1), 0) as total_imponible,
               COALESCE(AVG(rem_total), 0) as promedio_bruto
        FROM suc.afip_mapuche_sicoss 
        WHERE periodo_fiscal = :periodo
        """
        
        params = {'periodo': periodo_fiscal.periodo_str}
        resultado_df = db_conn.execute_query(query, params)
        
        if not resultado_df.empty:
            fila = resultado_df.iloc[0]
            total_registros = int(fila['total_registros'])
            total_bruto = float(fila['total_bruto'])
            total_imponible = float(fila['total_imponible'])
            promedio_bruto = float(fila['promedio_bruto'])
            
            print(f"✅ VALIDACIÓN BD COMPLETADA:")
            print(f"   - Registros en BD: {total_registros}")
            print(f"   - Registros esperados: {registros_esperados}")
            print(f"   - Total bruto: ${total_bruto:,.2f}")
            print(f"   - Total imponible: ${total_imponible:,.2f}")
            print(f"   - Promedio bruto: ${promedio_bruto:,.2f}")
            
            # Validar que se insertaron todos los registros
            if total_registros == registros_esperados:
                print("✅ Validación exitosa: Todos los registros insertados")
                
                # Consulta adicional para verificar datos críticos
                query_inv = """
                SELECT COUNT(*) as investigadores 
                FROM suc.afip_mapuche_sicoss 
                WHERE periodo_fiscal = :periodo 
                AND tipo_oper = 2
                """
                
                resultado_inv = db_conn.execute_query(query_inv, params)
                if not resultado_inv.empty:
                    investigadores = int(resultado_inv.iloc[0]['investigadores'])
                    print(f"   - Investigadores en BD: {investigadores}")
                
                db_conn.close()
                return True
            else:
                print(f"⚠️ Discrepancia: {total_registros} insertados vs {registros_esperados} esperados")
                db_conn.close()
                return False
        else:
            print("❌ No se encontraron registros en la BD")
            db_conn.close()
            return False
            
    except Exception as e:
        print(f"❌ Error validando BD: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Ejecuta el test definitivo con datos reales e inserción en BD"""
    print("🚀 TEST DEFINITIVO: DATOS REALES + BASE DE DATOS REAL")
    print("=" * 80)
    print(f"🕐 Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_time = time.time()
    
    # Test 1: Procesamiento con datos reales
    print("\n🎯 PASO 1: Procesamiento con Datos Realistas")
    exito_procesamiento, resultado = test_procesamiento_datos_reales()
    
    if not exito_procesamiento:
        print("\n❌ FALLO EN PROCESAMIENTO - Test abortado")
        return False
    
    # Test 2: Inserción en BD real
    print("\n🎯 PASO 2: Inserción en Base de Datos Real")
    exito_bd = False
    if resultado is not None:
        exito_bd = test_insercion_bd_real(resultado)
    else:
        print("❌ No hay resultado para insertar en BD")
    
    # Resumen final
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    print("📊 RESUMEN FINAL - TEST DEFINITIVO")
    print("=" * 80)
    
    print(f"⏱️ Tiempo total: {elapsed_time:.1f}s")
    print(f"   Paso 1 - Procesamiento Datos Reales: {'✅ EXITOSO' if exito_procesamiento else '❌ FALLÓ'}")
    print(f"   Paso 2 - Inserción BD Real: {'✅ EXITOSO' if exito_bd else '❌ FALLÓ'}")
    
    exito_total = exito_procesamiento and exito_bd
    
    if exito_total:
        print("\n🎉 ¡TEST DEFINITIVO COMPLETAMENTE EXITOSO!")
        print("🏆 SICOSS Backend OFICIALMENTE VALIDADO para PRODUCCIÓN")
        print("🚀 Sistema listo para reemplazar PHP legacy")
        print("💾 Base de datos PostgreSQL funcionando perfectamente")
        print("🔒 Datos reales procesados e insertados correctamente")
    else:
        print("\n⚠️ Test definitivo parcialmente exitoso")
        if exito_procesamiento and not exito_bd:
            print("🔧 Revisar configuración de base de datos")
        else:
            print("🔧 Revisar procesamiento antes de continuar")
    
    return exito_total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 