#!/usr/bin/env python3
"""
test_conceptos_processor.py

Script de prueba para validar el ConceptosProcessor vectorizado
Uso: python test_conceptos_processor.py --legajo 123456 [--periodo 2024/12]
"""

import argparse
import sys
import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any
import json

# Agregar el directorio padre al path
sys.path.append(os.path.dirname(__file__))

# Imports del proyecto
from database.database_connection import DatabaseConnection
from extractors.legajos_extractor import LegajosExtractor
from extractors.conceptos_extractor import ConceptosExtractor
from processors.conceptos_processor import ConceptosProcessor
from config.sicoss_config import SicossConfig

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConceptosProcessorTester:
    """Tester para validar ConceptosProcessor vectorizado"""
    
    def __init__(self):
        self.db = None
        self.config = None
        self.legajos_extractor = None
        self.conceptos_extractor = None
        self.conceptos_processor = None
        
    
    def setup(self):
        """Inicializa conexiones y componentes"""
        try:
            logger.info("🔧 Inicializando conexiones...")
            
            # Conexión a base de datos
            self.db = DatabaseConnection()
            
            # Obtener topes desde la base de datos usando MapucheConfig
            logger.info("📊 Obteniendo topes desde la base de datos...")
            
            # Leer configuración de BD
            import configparser
            config_ini = configparser.ConfigParser()
            config_ini.read('database.ini')
            db_params = config_ini['postgresql']
            
            # Crear parámetros de conexión con el tipo correcto
            from mapuche_config import create_mapuche_config, ConnectionParams
            connection_params: ConnectionParams = {
                'host': db_params.get('host', 'localhost'),
                'database': db_params.get('database', ''),
                'user': db_params.get('user', ''),
                'password': db_params.get('password', ''),
                'port': db_params.get('port', '5432')
            }
            
            # Crear instancia de MapucheConfig para obtener topes
            mapuche_config = create_mapuche_config(connection_params)
            
            # Obtener topes desde BD
            tope_jubilatorio_patronal = float(mapuche_config.get_topes_jubilatorio_patronal() or 0)
            tope_jubilatorio_personal = float(mapuche_config.get_topes_jubilatorio_personal() or 0)
            tope_otros_aportes_personales = float(mapuche_config.get_topes_otros_aportes_personales() or 0)
            
            logger.info(f"   💰 Tope jubilatorio patronal: ${tope_jubilatorio_patronal:,.2f}")
            logger.info(f"   💰 Tope jubilatorio personal: ${tope_jubilatorio_personal:,.2f}")
            logger.info(f"   💰 Tope otros aportes: ${tope_otros_aportes_personales:,.2f}")
            
            # Configuración con topes reales
            self.config = SicossConfig(
                tope_jubilatorio_patronal=tope_jubilatorio_patronal,
                tope_jubilatorio_personal=tope_jubilatorio_personal,
                tope_otros_aportes_personales=tope_otros_aportes_personales,
                trunca_tope=True,
                check_lic=False,
                check_retro=False,
                check_sin_activo=False,
                asignacion_familiar=False,
                trabajador_convencionado='S'
            )
            
            # Extractores
            self.legajos_extractor = LegajosExtractor(self.db)
            self.conceptos_extractor = ConceptosExtractor(self.db)
            
            # Procesador
            self.conceptos_processor = ConceptosProcessor(self.config)
            
            logger.info("✅ Setup completado con topes reales desde BD")
            
        except Exception as e:
            logger.error(f"❌ Error en setup: {e}")
            raise
    
    def test_legajo(self, nro_legaj: int, per_anoct: int, per_mesct: int) -> Dict[str, Any]:
        """Prueba el procesamiento para un legajo específico"""
        logger.info(f"🧪 Probando legajo {nro_legaj} para período {per_anoct}/{per_mesct}")
        
        try:
            # Verificar que los componentes estén inicializados
            if not self.legajos_extractor or not self.conceptos_extractor or not self.conceptos_processor:
                return {
                    'error': 'Componentes no inicializados correctamente. Ejecutar setup() primero.',
                    'legajo_encontrado': False
                }
            
            # 1. EXTRAER DATOS DEL LEGAJO
            logger.info("📥 Extrayendo datos del legajo...")
            where_legajo = f"dh01.nro_legaj = {nro_legaj}"
            
            df_legajos = self.legajos_extractor.extract(per_anoct, per_mesct, where_legajo)
            
            if df_legajos.empty:
                return {
                    'error': f"No se encontró el legajo {nro_legaj}",
                    'legajo_encontrado': False
                }
            
            logger.info(f"✅ Legajo encontrado: {df_legajos.iloc[0]['apyno']}")
            
            # 2. EXTRAER CONCEPTOS DEL LEGAJO
            logger.info("📥 Extrayendo conceptos liquidados...")
            df_conceptos = self.conceptos_extractor.extract_for_legajos(
                per_anoct, per_mesct, [nro_legaj]
            )
            
            logger.info(f"📊 Conceptos encontrados: {len(df_conceptos)}")
            
            # 3. PROCESAR CON CONCEPTOS PROCESSOR
            logger.info("⚡ Procesando con ConceptosProcessor vectorizado...")
            start_time = datetime.now()
            
            df_resultado = self.conceptos_processor.process(df_legajos, df_conceptos)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"⏱️ Procesamiento completado en {elapsed:.4f}s")
            
            # 4. ANALIZAR RESULTADOS
            return self._analizar_resultados(nro_legaj, df_legajos, df_conceptos, df_resultado)
            
        except Exception as e:
            logger.error(f"❌ Error procesando legajo {nro_legaj}: {e}")
            return {'error': str(e), 'legajo_encontrado': True}
    
    def _analizar_resultados(self, nro_legaj: int, df_legajos: pd.DataFrame, 
                           df_conceptos: pd.DataFrame, df_resultado: pd.DataFrame) -> Dict[str, Any]:
        """Analiza y estructura los resultados"""
        
        if df_resultado.empty:
            return {'error': 'Resultado vacío', 'legajo_encontrado': True}
        
        legajo_data = df_resultado.iloc[0]
        
        # Datos básicos del legajo
        legajo_info = {
            'nro_legaj': int(legajo_data['nro_legaj']),
            'apyno': df_legajos.iloc[0]['apyno'],
            'cuit': df_legajos.iloc[0]['cuit'],
            'estado': df_legajos.iloc[0]['estado']
        }
        
        # Conceptos liquidados (entrada)
        conceptos_detalle = []
        if not df_conceptos.empty:
            for _, concepto in df_conceptos.iterrows():
                conceptos_detalle.append({
                    'codn_conce': int(concepto['codn_conce']),
                    'tipo_conce': concepto['tipo_conce'],
                    'impp_conce': float(concepto['impp_conce']),
                    'tipos_grupos': concepto['tipos_grupos'],
                    'codigoescalafon': concepto.get('codigoescalafon', '')
                })
        
        # Campos SICOSS procesados (salida)
        campos_sicoss = {}
        campos_importantes = [
            'ImporteSAC', 'ImporteHorasExtras', 'ImporteVacaciones',
            'ImporteAdicionales', 'ImportePremios', 'ImporteNoRemun',
            'ImporteMaternidad', 'ImporteZonaDesfavorable', 'ImporteImponible_6',
            'AporteAdicionalObraSocial', 'CantidadHorasExtras', 'SeguroVidaObligatorio',
            'ImporteImponibleBecario', 'ImporteSICOSS27430', 'ImporteSICOSSDec56119',
            'NoRemun4y8', 'IncrementoSolidario', 'ImporteTipo91', 'ImporteNoRemun96',
            'PrioridadTipoDeActividad'
        ]
        
        for campo in campos_importantes:
            if campo in legajo_data:
                valor = legajo_data[campo]
                if pd.notna(valor) and valor != 0:
                    campos_sicoss[campo] = float(valor) if isinstance(valor, (int, float)) else valor
        
        # Estadísticas
        estadisticas = {
            'total_conceptos': len(df_conceptos),
            'total_importe': float(df_conceptos['impp_conce'].sum()) if not df_conceptos.empty else 0,
            'campos_con_valor': len(campos_sicoss),
            'tipos_grupos_únicos': list(set([
                tipo for tipos in df_conceptos['tipos_grupos'].tolist() 
                for tipo in (tipos if isinstance(tipos, list) else [])
            ])) if not df_conceptos.empty else []
        }
        
        return {
            'legajo_encontrado': True,
            'legajo_info': legajo_info,
            'conceptos_entrada': conceptos_detalle,
            'campos_sicoss_salida': campos_sicoss,
            'estadisticas': estadisticas
        }
    
    def mostrar_resultados(self, resultados: Dict[str, Any]):
        """Muestra los resultados de manera organizada"""
        
        if not resultados.get('legajo_encontrado', False):
            print(f"❌ {resultados.get('error', 'Error desconocido')}")
            return
        
        if 'error' in resultados:
            print(f"❌ Error: {resultados['error']}")
            return
        
        # Header
        print("\n" + "="*80)
        print("🧪 RESULTADOS DE PRUEBA - CONCEPTOS PROCESSOR VECTORIZADO")
        print("="*80)
        
        # Información del legajo
        info = resultados['legajo_info']
        print(f"\n👤 LEGAJO: {info['nro_legaj']}")
        print(f"   Nombre: {info['apyno']}")
        print(f"   CUIT: {info['cuit']}")
        print(f"   Estado: {info['estado']}")
        
        # Estadísticas
        stats = resultados['estadisticas']
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"   Conceptos procesados: {stats['total_conceptos']}")
        print(f"   Importe total: ${stats['total_importe']:,.2f}")
        print(f"   Campos SICOSS con valor: {stats['campos_con_valor']}")
        print(f"   Tipos grupos únicos: {stats['tipos_grupos_únicos']}")
        
        # Conceptos de entrada
        if resultados['conceptos_entrada']:
            print(f"\n📥 CONCEPTOS LIQUIDADOS ({len(resultados['conceptos_entrada'])}):")
            print("-" * 60)
            for concepto in resultados['conceptos_entrada']:
                print(f"   {concepto['codn_conce']:>6} | {concepto['tipo_conce']} | "
                      f"${concepto['impp_conce']:>10,.2f} | {concepto['tipos_grupos']} | "
                      f"{concepto['codigoescalafon']}")
        
        # Campos SICOSS de salida
        if resultados['campos_sicoss_salida']:
            print(f"\n⚡ CAMPOS SICOSS PROCESADOS ({len(resultados['campos_sicoss_salida'])}):")
            print("-" * 60)
            for campo, valor in sorted(resultados['campos_sicoss_salida'].items()):
                if isinstance(valor, float):
                    print(f"   {campo:<30} | ${valor:>12,.2f}")
                else:
                    print(f"   {campo:<30} | {valor:>12}")
        
        print("\n" + "="*80)
        print("✅ PRUEBA COMPLETADA")
        print("="*80)
    
    def cleanup(self):
        """Limpia recursos"""
        if self.db:
            self.db.close()
        logger.info("🧹 Cleanup completado")

def parse_arguments():
    """Parsea argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description='Prueba ConceptosProcessor vectorizado para un legajo específico'
    )
    
    parser.add_argument(
        '--legajo', '-l',
        type=int,
        required=True,
        help='Número de legajo a probar'
    )
    
    parser.add_argument(
        '--periodo', '-p',
        type=str,
        default=None,
        help='Período en formato YYYY/MM (default: período actual)'
    )
    
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Activar modo debug'
    )
    
    return parser.parse_args()

def parse_periodo(periodo_str: str) -> tuple:
    """Parsea string de período YYYY/MM"""
    if not periodo_str:
        # Usar período actual
        now = datetime.now()
        return now.year, now.month
    
    try:
        year, month = periodo_str.split('/')
        return int(year), int(month)
    except ValueError:
        raise ValueError(f"Formato de período inválido: {periodo_str}. Use YYYY/MM")

def main():
    """Función principal"""
    args = parse_arguments()
    
    # Configurar logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parsear período
    try:
        per_anoct, per_mesct = parse_periodo(args.periodo)
    except ValueError as e:
        print(f"❌ Error: {e}")
        return 1
    
    # Crear y ejecutar tester
    tester = ConceptosProcessorTester()
    
    try:
        tester.setup()
        resultados = tester.test_legajo(args.legajo, per_anoct, per_mesct)
        tester.mostrar_resultados(resultados)
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        return 1
        
    finally:
        tester.cleanup()

if __name__ == "__main__":
    sys.exit(main()) 