#!/usr/bin/env python3
"""
Test de Comparación SICOSS Python vs PHP Legacy
===============================================

Compara los resultados del sistema Python refactorizado con el sistema PHP legacy
para verificar que los cálculos SICOSS son equivalentes campo por campo.

Referencia: https://documentacion.siu.edu.ar/wiki/SIU-Mapuche/Version3.15.1/Documentacion_de_las_operaciones/comunicacion/afip/sicoss
"""

import pandas as pd
import sys
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports del sistema
from processors.sicoss_processor import SicossDataProcessor
from processors.database_saver import SicossDatabaseSaver
from extractors.data_extractor_manager import DataExtractorManager
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from value_objects.periodo_fiscal import PeriodoFiscal

class ComparadorSicoss:
    """Comparador entre sistema Python y PHP legacy"""
    
    def __init__(self):
        print("🔍 Inicializando Comparador SICOSS Python vs PHP Legacy")
        
        # Configuración
        self.config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True
        )
        
        # Conexión BD
        self.db = DatabaseConnection()
        
        # Componentes del pipeline Python
        self.extractor = DataExtractorManager(self.db)
        self.processor = SicossDataProcessor(self.config)
        self.saver = SicossDatabaseSaver(self.config, self.db)
        
        # Campos SICOSS según documentación oficial
        self.campos_sicoss = self._definir_campos_sicoss()
        
        print("✅ Comparador inicializado")
    
    def _definir_campos_sicoss(self) -> Dict[str, Dict]:
        """Define los campos SICOSS según la documentación oficial"""
        return {
            # Campos de identificación
            'cuil': {'nombre': 'CUIL', 'tipo': 'identificacion'},
            'apnom': {'nombre': 'Apellido y Nombre', 'tipo': 'identificacion'},
            
            # Campos de configuración
            'conyuge': {'nombre': 'Cónyuge', 'tipo': 'configuracion'},
            'cant_hijos': {'nombre': 'Cantidad de Hijos', 'tipo': 'configuracion'},
            'cod_situacion': {'nombre': 'Código de Situación', 'tipo': 'configuracion'},
            'cod_cond': {'nombre': 'Código de Condición', 'tipo': 'configuracion'},
            'cod_act': {'nombre': 'Código de Actividad', 'tipo': 'configuracion'},
            'cod_zona': {'nombre': 'Código de Zona', 'tipo': 'configuracion'},
            'porc_aporte': {'nombre': 'Porcentaje de Aporte', 'tipo': 'configuracion'},
            'cod_mod_cont': {'nombre': 'Código Modalidad Contractual', 'tipo': 'configuracion'},
            'cod_os': {'nombre': 'Código Obra Social', 'tipo': 'configuracion'},
            'cant_adh': {'nombre': 'Cantidad Adherentes', 'tipo': 'configuracion'},
            
            # Campos de remuneración (críticos para verificar)
            'rem_total': {'nombre': 'Remuneración Total (Campo 7)', 'tipo': 'remuneracion', 'campo_sicoss': 7},
            'rem_impo1': {'nombre': 'Remuneración Imponible 1 (Campo 14)', 'tipo': 'remuneracion', 'campo_sicoss': 14},
            'rem_impo2': {'nombre': 'Remuneración Imponible 2 (Campo 21)', 'tipo': 'remuneracion', 'campo_sicoss': 21},
            'rem_impo3': {'nombre': 'Remuneración Imponible 3 (Campo 22)', 'tipo': 'remuneracion', 'campo_sicoss': 22},
            'rem_impo4': {'nombre': 'Remuneración Imponible 4 (Campo 23)', 'tipo': 'remuneracion', 'campo_sicoss': 23},
            'rem_impo5': {'nombre': 'Remuneración Imponible 5 (Campo 42)', 'tipo': 'remuneracion', 'campo_sicoss': 42},
            'rem_impo6': {'nombre': 'Remuneración Imponible 6 (Campo 44)', 'tipo': 'remuneracion', 'campo_sicoss': 44},
            'rem_imp7': {'nombre': 'Remuneración 7 (Campo 49)', 'tipo': 'remuneracion', 'campo_sicoss': 49},
            'rem_dec_788': {'nombre': 'Remuneración 788/05 - Imponible 8 (Campo 48)', 'tipo': 'remuneracion', 'campo_sicoss': 48},
            'rem_imp9': {'nombre': 'Remuneración Imponible 9 (Campo 54)', 'tipo': 'remuneracion', 'campo_sicoss': 54},
            
            # Campos de conceptos específicos
            'sac': {'nombre': 'SAC (Campo 8)', 'tipo': 'concepto', 'campo_sicoss': 8},
            'horas_extras': {'nombre': 'Horas Extras (Campo 9)', 'tipo': 'concepto', 'campo_sicoss': 9},
            'zona_desfav': {'nombre': 'Zona Desfavorable (Campo 10)', 'tipo': 'concepto', 'campo_sicoss': 10},
            'vacaciones': {'nombre': 'Vacaciones (Campo 40)', 'tipo': 'concepto', 'campo_sicoss': 40},
            'adicionales': {'nombre': 'Importe Adicionales (Campo 46)', 'tipo': 'concepto', 'campo_sicoss': 46},
            'premios': {'nombre': 'Importe Premios (Campo 47)', 'tipo': 'concepto', 'campo_sicoss': 47},
            'cpto_no_remun': {'nombre': 'Conceptos No Remunerativos (Campo 51)', 'tipo': 'concepto', 'campo_sicoss': 51},
            'maternidad': {'nombre': 'Maternidad (Campo 52)', 'tipo': 'concepto', 'campo_sicoss': 52},
            'rectificacion_remun': {'nombre': 'Rectificación de Remuneración (Campo 53)', 'tipo': 'concepto', 'campo_sicoss': 53},
            
            # Otros campos importantes
            'asig_fam_pag': {'nombre': 'Asignaciones Familiares Pagadas', 'tipo': 'otros'},
            'aporte_vol': {'nombre': 'Aportes Voluntarios', 'tipo': 'otros'},
            'regimen': {'nombre': 'Régimen (Campo 29)', 'tipo': 'otros', 'campo_sicoss': 29},
            'convencionado': {'nombre': 'Trabajador Convencionado (Campo 43)', 'tipo': 'otros', 'campo_sicoss': 43},
            'tipo_empresa': {'nombre': 'Tipo de Empresa (Campo 27)', 'tipo': 'otros', 'campo_sicoss': 27},
            'seguro': {'nombre': 'Seguro de Vida Obligatorio (Campo 57)', 'tipo': 'otros', 'campo_sicoss': 57},
        }
    
    def ejecutar_comparacion_completa(self, per_anoct: int = 2025, per_mesct: int = 5) -> Dict:
        """Ejecuta la comparación completa"""
        
        print(f"\n🔄 EJECUTANDO COMPARACIÓN SICOSS")
        print(f"📅 Período: {per_anoct}/{per_mesct:02d}")
        print("=" * 60)
        
        resultado = {
            'success': False,
            'legajo_comparado': None,
            'diferencias': [],
            'coincidencias': [],
            'resumen_estadistico': {},
            'errores': []
        }
        
        try:
            # PASO 1: Encontrar un legajo para comparar que exista en ambas tablas
            print("\n🔍 PASO 1: Buscando legajo común en ambas tablas...")
            legajo_comun = self._encontrar_legajo_comun(per_anoct, per_mesct)
            
            if not legajo_comun:
                raise Exception("No se encontró ningún legajo común entre las tablas Python y PHP Legacy")
            
            print(f"✅ Legajo encontrado para comparar: {legajo_comun}")
            resultado['legajo_comparado'] = legajo_comun
            
            # PASO 2: Ejecutar pipeline Python para este legajo específico
            print(f"\n⚙️ PASO 2: Ejecutando pipeline Python para legajo {legajo_comun}...")
            resultado_python = self._ejecutar_pipeline_python(per_anoct, per_mesct, legajo_comun)
            
            if not resultado_python['success']:
                raise Exception(f"Error en pipeline Python: {resultado_python.get('error')}")
            
            # PASO 3: Obtener datos del sistema legacy
            print(f"\n📊 PASO 3: Obteniendo datos legacy para legajo {legajo_comun}...")
            datos_legacy = self._obtener_datos_legacy(legajo_comun, per_anoct, per_mesct)
            
            if datos_legacy.empty:
                raise Exception(f"No se encontraron datos legacy para legajo {legajo_comun}")
            
            # PASO 4: Obtener datos del sistema Python
            print(f"\n📊 PASO 4: Obteniendo datos Python para legajo {legajo_comun}...")
            datos_python = self._obtener_datos_python(legajo_comun, per_anoct, per_mesct)
            
            if datos_python.empty:
                raise Exception(f"No se encontraron datos Python para legajo {legajo_comun}")
            
            # PASO 5: Comparar campo por campo
            print(f"\n🔍 PASO 5: Comparando campos SICOSS...")
            comparacion = self._comparar_datos(datos_python.iloc[0], datos_legacy.iloc[0])
            
            resultado.update({
                'success': True,
                'diferencias': comparacion['diferencias'],
                'coincidencias': comparacion['coincidencias'],
                'resumen_estadistico': comparacion['resumen']
            })
            
            # PASO 6: Generar reporte
            self._generar_reporte_comparacion(resultado)
            
            return resultado
            
        except Exception as e:
            error = f"Error en comparación: {str(e)}"
            print(f"\n❌ {error}")
            resultado['errores'].append(error)
            return resultado
    
    def _encontrar_legajo_comun(self, per_anoct: int, per_mesct: int) -> Optional[str]:
        """Encuentra un legajo que exista en ambas tablas"""
        
        periodo_str = f"{per_anoct}{per_mesct:02d}"
        
        query = f"""
        SELECT p.cuil
        FROM suc.afip_mapuche_sicoss p
        INNER JOIN suc.afip_mapuche_sicoss_bkp l ON p.cuil = l.cuil
        WHERE p.periodo_fiscal = '{periodo_str}'
        AND l.periodo_fiscal = '{periodo_str}'
        LIMIT 1
        """
        
        resultado = self.db.execute_query(query)
        
        if not resultado.empty:
            return resultado.iloc[0]['cuil']
        
        return None
    
    def _ejecutar_pipeline_python(self, per_anoct: int, per_mesct: int, cuil_target: str) -> Dict:
        """Ejecuta el pipeline Python para un CUIL específico"""
        
        try:
            # Obtener el legajo correspondiente al CUIL
            query_legajo = f"""
            SELECT nro_legaj FROM mapuche.dh08 
            WHERE nro_cuil = '{cuil_target}' 
            LIMIT 1
            """
            
            df_legajo = self.db.execute_query(query_legajo)
            
            if df_legajo.empty:
                return {'success': False, 'error': f'No se encontró legajo para CUIL {cuil_target}'}
            
            legajo = int(df_legajo.iloc[0]['nro_legaj'])
            
            # Extraer datos
            datos = self.extractor.extraer_datos_completos(
                config=self.config,
                per_anoct=per_anoct,
                per_mesct=per_mesct,
                nro_legajo=legajo
            )
            
            # Procesar
            proc_resultado = self.processor.procesar_datos_extraidos(datos)
            
            if 'legajos_procesados' not in proc_resultado or proc_resultado['legajos_procesados'].empty:
                return {'success': False, 'error': 'No se procesaron datos'}
            
            # Guardar en BD
            periodo = PeriodoFiscal.from_string(f"{per_anoct}{per_mesct:02d}")
            guardado = self.saver.guardar_en_bd(
                legajos=proc_resultado['legajos_procesados'],
                periodo_fiscal=periodo,
                incluir_inactivos=False
            )
            
            return {'success': guardado['success'], 'legajos_guardados': guardado.get('legajos_guardados', 0)}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _obtener_datos_legacy(self, cuil: str, per_anoct: int, per_mesct: int) -> pd.DataFrame:
        """Obtiene datos del sistema legacy"""
        
        periodo_str = f"{per_anoct}{per_mesct:02d}"
        
        query = f"""
        SELECT * FROM suc.afip_mapuche_sicoss_bkp 
        WHERE cuil = '{cuil}' 
        AND periodo_fiscal = '{periodo_str}'
        """
        
        return self.db.execute_query(query)
    
    def _obtener_datos_python(self, cuil: str, per_anoct: int, per_mesct: int) -> pd.DataFrame:
        """Obtiene datos del sistema Python"""
        
        periodo_str = f"{per_anoct}{per_mesct:02d}" 
        
        query = f"""
        SELECT * FROM suc.afip_mapuche_sicoss 
        WHERE cuil = '{cuil}' 
        AND periodo_fiscal = '{periodo_str}'
        ORDER BY id DESC
        LIMIT 1
        """
        
        return self.db.execute_query(query)
    
    def _comparar_datos(self, datos_python: pd.Series, datos_legacy: pd.Series) -> Dict:
        """Compara los datos campo por campo"""
        
        diferencias = []
        coincidencias = []
        
        for campo, config in self.campos_sicoss.items():
            if campo in datos_python.index and campo in datos_legacy.index:
                valor_python = datos_python[campo]
                valor_legacy = datos_legacy[campo]
                
                # Convertir a tipos comparables
                if pd.isna(valor_python):
                    valor_python = None
                if pd.isna(valor_legacy):
                    valor_legacy = None
                
                # Comparar valores numéricos con tolerancia
                if isinstance(valor_python, (int, float, Decimal)) and isinstance(valor_legacy, (int, float, Decimal)):
                    if valor_python is None and valor_legacy is None:
                        coincide = True
                    elif valor_python is None or valor_legacy is None:
                        coincide = False
                    else:
                        # Tolerancia de 0.01 para valores numéricos
                        coincide = abs(float(valor_python) - float(valor_legacy)) < 0.01
                else:
                    coincide = valor_python == valor_legacy
                
                info_comparacion = {
                    'campo': campo,
                    'nombre': config['nombre'],
                    'tipo': config['tipo'],
                    'campo_sicoss': config.get('campo_sicoss'),
                    'valor_python': valor_python,
                    'valor_legacy': valor_legacy,
                    'coincide': coincide
                }
                
                if coincide:
                    coincidencias.append(info_comparacion)
                else:
                    diferencias.append(info_comparacion)
        
        resumen = {
            'total_campos': len(self.campos_sicoss),
            'coincidencias': len(coincidencias),
            'diferencias': len(diferencias),
            'porcentaje_coincidencia': (len(coincidencias) / len(self.campos_sicoss)) * 100 if self.campos_sicoss else 0
        }
        
        return {
            'diferencias': diferencias,
            'coincidencias': coincidencias,
            'resumen': resumen
        }
    
    def _generar_reporte_comparacion(self, resultado: Dict):
        """Genera el reporte de comparación"""
        
        print(f"\n" + "="*80)
        print("REPORTE DE COMPARACIÓN SICOSS PYTHON vs PHP LEGACY")
        print("="*80)
        
        if resultado['success']:
            print(f"✅ Comparación exitosa para CUIL: {resultado['legajo_comparado']}")
            
            resumen = resultado['resumen_estadistico']
            print(f"\n📊 RESUMEN ESTADÍSTICO:")
            print(f"   - Total campos comparados: {resumen['total_campos']}")
            print(f"   - Coincidencias: {resumen['coincidencias']}")
            print(f"   - Diferencias: {resumen['diferencias']}")
            print(f"   - Porcentaje de coincidencia: {resumen['porcentaje_coincidencia']:.1f}%")
            
            # Mostrar diferencias críticas
            if resultado['diferencias']:
                print(f"\n❌ DIFERENCIAS ENCONTRADAS ({len(resultado['diferencias'])}):")
                print("-" * 80)
                
                for diff in resultado['diferencias']:
                    print(f"Campo: {diff['nombre']} ({diff['campo']})")
                    print(f"  Tipo: {diff['tipo']}")
                    if diff['campo_sicoss']:
                        print(f"  Campo SICOSS: {diff['campo_sicoss']}")
                    print(f"  Python: {diff['valor_python']}")
                    print(f"  Legacy: {diff['valor_legacy']}")
                    print(f"  Diferencia: {self._calcular_diferencia(diff['valor_python'], diff['valor_legacy'])}")
                    print()
            
            # Mostrar algunas coincidencias importantes
            coincidencias_importantes = [c for c in resultado['coincidencias'] 
                                       if c['tipo'] in ['remuneracion', 'concepto']]
            
            if coincidencias_importantes:
                print(f"\n✅ COINCIDENCIAS IMPORTANTES ({len(coincidencias_importantes)}):")
                print("-" * 80)
                
                for coin in coincidencias_importantes[:10]:  # Mostrar solo las primeras 10
                    print(f"✓ {coin['nombre']}: {coin['valor_python']}")
            
        else:
            print("❌ La comparación falló")
            for error in resultado.get('errores', []):
                print(f"   Error: {error}")
        
        print("\n" + "="*80)
    
    def _calcular_diferencia(self, val1, val2):
        """Calcula la diferencia entre dos valores"""
        
        if val1 is None and val2 is None:
            return "Ambos nulos"
        elif val1 is None:
            return f"Python nulo, Legacy: {val2}"
        elif val2 is None:
            return f"Python: {val1}, Legacy nulo"
        
        try:
            diff = float(val1) - float(val2)
            return f"{diff:+.2f}"
        except (ValueError, TypeError):
            return f"Tipos diferentes: {type(val1).__name__} vs {type(val2).__name__}"
    
    def close(self):
        """Cerrar conexiones"""
        self.db.close()


def main():
    """Función principal del comparador"""
    print("🔍 COMPARADOR SICOSS PYTHON vs PHP LEGACY")
    print("=" * 50)
    print("Compara campo por campo los resultados del sistema refactorizado")
    print("con el sistema legacy para verificar equivalencia de cálculos")
    print("=" * 50)
    
    comparador = None
    try:
        # Crear comparador
        comparador = ComparadorSicoss()
        
        # Ejecutar comparación
        resultado = comparador.ejecutar_comparacion_completa()
        
        # Resultado final
        if resultado['success']:
            resumen = resultado['resumen_estadistico']
            if resumen['diferencias'] == 0:
                print("\n🎉 ¡ÉXITO TOTAL! Los sistemas son equivalentes")
                print("✅ Todos los campos coinciden exactamente")
                return True
            else:
                print(f"\n⚠️ Se encontraron {resumen['diferencias']} diferencias")
                print(f"📊 Coincidencia: {resumen['porcentaje_coincidencia']:.1f}%")
                print("🔧 Revisar diferencias específicas en el reporte anterior")
                return False
        else:
            print("\n❌ La comparación falló")
            return False
        
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        return False
    finally:
        if comparador:
            comparador.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 