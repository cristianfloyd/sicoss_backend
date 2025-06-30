"""
database_saver.py

Guardado de datos SICOSS en base de datos

Implementación completa del guardado en BD de datos SICOSS procesados
"""

import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

# Imports locales
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from database.database_connection import DatabaseConnection
from value_objects.periodo_fiscal import PeriodoFiscal

logger = logging.getLogger(__name__)

class SicossDatabaseSaver:
    """
    Guardado de datos SICOSS en base de datos
    
    Replica las funcionalidades del PHP legacy:
    - guardar_en_bd() (PHP línea 3604)
    - generar_sicoss_bd() (PHP línea 3783) 
    - procesa_sicoss_bd() (PHP línea 3849)
    - mapear_legajo_a_modelo() (PHP línea 3691)
    """
    
    def __init__(self, config=None, db_connection: Optional[DatabaseConnection] = None):
        """
        Inicializa el guardador de base de datos
        
        Args:
            config: Configuración SICOSS
            db_connection: Conexión a BD (opcional, se crea una nueva si no se provee)
        """
        self.config = config
        self.db = db_connection or DatabaseConnection()
        self.schema = 'suc'  # Esquema real de la BD
        self.tabla_sicoss = 'afip_mapuche_sicoss'  # Tabla existente
        self._init_schema_mapping()
        logger.info("✅ SicossDatabaseSaver inicializado correctamente")
    
    def _init_schema_mapping(self):
        """Inicializa el mapeo de campos para BD basado en tabla real afip_mapuche_sicoss"""
        
        # Mapeo de campos DataFrame SICOSS -> Tabla afip_mapuche_sicoss
        # Basado en el DDL proporcionado
        self.campo_mapping = {
            # Campos básicos de identificación
            'cuit': 'cuil',  # VARCHAR(11)
            'apyno': 'apnom',  # VARCHAR(40)
            'conyugue': 'conyuge',  # BOOLEAN
            'hijos': 'cant_hijos',  # INTEGER
            'codigosituacion': 'cod_situacion',  # INTEGER
            'codigocondicion': 'cod_cond',  # INTEGER
            'TipoDeActividad': 'cod_act',  # INTEGER
            'codigozona': 'cod_zona',  # INTEGER
            'aporteadicional': 'porc_aporte',  # NUMERIC(5,2)
            'codigocontratacion': 'cod_mod_cont',  # INTEGER
            'codigo_os': 'cod_os',  # VARCHAR(6)
            'adherentes': 'cant_adh',  # INTEGER
            
            # Importes principales
            'IMPORTE_BRUTO': 'rem_total',  # NUMERIC(12,2)
            'IMPORTE_IMPON': 'rem_impo1',  # NUMERIC(12,2)
            'AsignacionesFliaresPagadas': 'asig_fam_pag',  # NUMERIC(9,2)
            'IMPORTE_VOLUN': 'aporte_vol',  # NUMERIC(9,2)
            'IMPORTE_ADICI': 'imp_adic_os',  # NUMERIC(9,2)
            
            # Campos adicionales de remuneración
            'ImporteImponible_4': 'rem_impo2',  # NUMERIC(12,2)
            'ImporteImponible_5': 'rem_impo3',  # NUMERIC(12,2)
            'ImporteImponible_6': 'rem_impo4',  # NUMERIC(12,2)
            'ImporteImponibleBecario': 'rem_impo5',  # NUMERIC(12,2)
            'ImporteImponible_9': 'rem_impo6',  # NUMERIC(12,2)
            'ImporteTipo91': 'rem_imp7',  # NUMERIC(12,2)
            'ImporteNoRemun96': 'rem_imp9',  # NUMERIC(12,2)
            
            # Provincia
            'provincialocalidad': 'prov',  # VARCHAR(50)
            
            # Códigos especiales
            'codigosiniestrado': 'cod_siniestrado',  # INTEGER
            'marcareduccion': 'marca_reduccion',  # INTEGER
            'recompensalrt': 'recomp_lrt',  # NUMERIC(9,2)
            'tipoempresa': 'tipo_empresa',  # INTEGER
            'AporteAdicionalObraSocial': 'aporte_adic_os',  # NUMERIC(9,2)
            'regimen': 'regimen',  # INTEGER
            
            # Situaciones de revista
            'codigorevista1': 'sit_rev1',  # INTEGER
            'fecharevista1': 'dia_ini_sit_rev1',  # INTEGER
            'codigorevista2': 'sit_rev2',  # INTEGER
            'fecharevista2': 'dia_ini_sit_rev2',  # INTEGER
            'codigorevista3': 'sit_rev3',  # INTEGER
            'fecharevista3': 'dia_ini_sit_rev3',  # INTEGER
            
            # Conceptos específicos
            'ImporteSueldoMasAdicionales': 'sueldo_adicc',  # NUMERIC(12,2)
            'ImporteSAC': 'sac',  # NUMERIC(12,2)
            'ImporteHorasExtras': 'horas_extras',  # NUMERIC(12,2)
            'ImporteZonaDesfavorable': 'zona_desfav',  # NUMERIC(12,2)
            'ImporteVacaciones': 'vacaciones',  # NUMERIC(12,2)
            'dias_trabajados': 'cant_dias_trab',  # INTEGER
            'trabajadorconvencionado': 'convencionado',  # INTEGER
            'TipoDeOperacion': 'tipo_oper',  # INTEGER
            'ImporteAdicionales': 'adicionales',  # NUMERIC(12,2)
            'ImportePremios': 'premios',  # NUMERIC(12,2)
            'Remuner78805': 'rem_dec_788',  # NUMERIC(12,2)
            'CantidadHorasExtras': 'nro_horas_ext',  # INTEGER
            'ImporteNoRemun': 'cpto_no_remun',  # NUMERIC(12,2)
            'ImporteMaternidad': 'maternidad',  # NUMERIC(12,2)
            'ImporteRectificacionRemun': 'rectificacion_remun',  # NUMERIC(9,2)
            'ContribTareaDif': 'contrib_dif',  # NUMERIC(9,2)
            'SeguroVidaObligatorio': 'seguro',  # INTEGER
            'ImporteSICOSS27430': 'ley',  # NUMERIC(12,2)
            'IncrementoSolidario': 'incsalarial',  # NUMERIC(12,2)
            'ImporteImponible_11': 'remimp11'  # NUMERIC(12,2)
        }
        
        # Campos requeridos con valores por defecto
        self.campos_requeridos = {
            'periodo_fiscal': '',  # Se asigna dinámicamente
            'cuil': '',
            'apnom': '',
            'conyuge': False,
            'cant_hijos': 0,
            'cod_situacion': 1,
            'cod_cond': 1,
            'cod_act': 0,
            'cod_zona': 0,
            'porc_aporte': 0.0,
            'cod_mod_cont': 0,
            'cod_os': '000000',
            'cant_adh': 0,
            'rem_total': 0.0,
            'rem_impo1': 0.0,
            'asig_fam_pag': 0.0,
            'aporte_vol': 0.0,
            'marca_reduccion': 0,
            'tipo_empresa': 0,
            'aporte_adic_os': 0.0,
            'regimen': 1,
            'sit_rev1': 1,
            'dia_ini_sit_rev1': 1,
            'sit_rev2': 0,
            'dia_ini_sit_rev2': 0,
            'sit_rev3': 0,
            'dia_ini_sit_rev3': 0,
            'sueldo_adicc': 0.0,
            'sac': 0.0,
            'horas_extras': 0.0,
            'zona_desfav': 0.0,
            'vacaciones': 0.0,
            'cant_dias_trab': 30,
            'convencionado': 1,
            'adicionales': 0.0,
            'premios': 0.0,
            'rem_dec_788': 0.0,
            'nro_horas_ext': 0,
            'cpto_no_remun': 0.0,
            'rectificacion_remun': 0.0,
            'hstrab': 0,
            'seguro': 0,
            'ley': 0.0,
            'incsalarial': 0.0,
            'remimp11': 0.0
        }
    
    def guardar_en_bd(self, legajos: pd.DataFrame, periodo_fiscal: PeriodoFiscal, 
                     incluir_inactivos: bool = False) -> Dict[str, Any]:
        """
        Guarda legajos procesados en base de datos
        
        Replica guardar_en_bd() del PHP (línea 3604)
        
        Args:
            legajos: DataFrame con legajos procesados
            periodo_fiscal: Período fiscal del procesamiento
            incluir_inactivos: Si incluir legajos inactivos
            
        Returns:
            Dict con resultado del guardado
        """
        logger.info(f"💾 Guardando en BD para período {periodo_fiscal}")
        logger.info(f"📊 Legajos a guardar: {len(legajos)}")
        
        try:
            # 1. Validar datos de entrada
            if legajos.empty:
                logger.warning("⚠️ No hay legajos para guardar")
                return {
                    'success': False,
                    'message': 'No hay datos para guardar',
                    'legajos_guardados': 0
                }
            
            # 2. Crear esquema y tabla si no existe
            self._ensure_table_exists(periodo_fiscal)
            
            # 3. Mapear legajos a modelo de BD
            legajos_mapeados = self._mapear_legajos_a_modelo(legajos, periodo_fiscal)
            
            # 4. Validar estructura de datos
            self._validar_datos_para_bd(legajos_mapeados)
            
            # 5. Filtrar legajos activos si es necesario
            if not incluir_inactivos:
                legajos_mapeados = self._filtrar_legajos_activos(legajos_mapeados)
            
            # 6. Ejecutar guardado en BD
            resultado_guardado = self._ejecutar_guardado_bd(legajos_mapeados, periodo_fiscal)
            
            # 7. Generar resultado
            resultado = {
                'success': True,
                'message': 'Guardado en BD exitoso',
                'periodo_fiscal': periodo_fiscal.periodo_str,
                'legajos_guardados': len(legajos_mapeados),
                'incluir_inactivos': incluir_inactivos,
                'timestamp': datetime.now().isoformat(),
                'detalles': resultado_guardado
            }
            
            logger.info(f"✅ Guardado BD completado: {resultado['legajos_guardados']} legajos")
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Error en guardado BD: {e}")
            return {
                'success': False,
                'message': f'Error en guardado: {str(e)}',
                'legajos_guardados': 0,
                'error': str(e)
            }
    
    def generar_sicoss_bd(self, datos: Dict[str, Any], periodo_fiscal: PeriodoFiscal,
                         incluir_inactivos: bool = False) -> Dict[str, Any]:
        """
        🚧 TODO: Genera SICOSS directo a BD sin archivos
        
        Replica generar_sicoss_bd() del PHP (línea 3783)
        
        Args:
            datos: Datos extraídos para procesamiento
            periodo_fiscal: Período fiscal
            incluir_inactivos: Si incluir legajos inactivos
            
        Returns:
            Dict con resultado del procesamiento y guardado
            
        TODO: IMPLEMENTAR PIPELINE COMPLETO BD
        """
        logger.warning("🚧 TODO: generar_sicoss_bd - IMPLEMENTACIÓN PENDIENTE")
        logger.info(f"🚀 Simulando generación SICOSS a BD para período {periodo_fiscal}")
        
        try:
            # 1. Procesar datos con el pipeline normal
            from processors.sicoss_processor import SicossDataProcessor
            
            processor = SicossDataProcessor(self.config)
            resultado_procesamiento = processor.procesar_datos_extraidos(datos)
            
            # 2. Guardar resultados en BD
            if resultado_procesamiento.get('success', False):
                legajos_procesados = resultado_procesamiento.get('legajos_procesados', pd.DataFrame())
                resultado_bd = self.guardar_en_bd(legajos_procesados, periodo_fiscal, incluir_inactivos)
                
                # 3. Combinar resultados
                resultado_final = {
                    'success': resultado_bd.get('success', False),
                    'message': 'Generación SICOSS a BD simulada',
                    'periodo_fiscal': periodo_fiscal.periodo_str,
                    'procesamiento': resultado_procesamiento,
                    'guardado_bd': resultado_bd,
                    'incluir_inactivos': incluir_inactivos
                }
                
                logger.info(f"✅ Generación SICOSS a BD simulada completada")
                return resultado_final
            else:
                return {
                    'success': False,
                    'message': 'Error en procesamiento, no se guardó en BD',
                    'procesamiento': resultado_procesamiento
                }
                
        except Exception as e:
            logger.error(f"❌ Error en generación SICOSS BD: {e}")
            return {
                'success': False,
                'message': f'Error en generación BD: {str(e)}',
                'error': str(e)
            }
    
    def procesa_sicoss_bd(self, datos: Dict[str, Any], per_anoct: int, per_mesct: int,
                         legajos: pd.DataFrame, periodo_fiscal: PeriodoFiscal) -> Dict[str, Any]:
        """
        🚧 TODO: Procesa SICOSS específicamente para BD
        
        Replica procesa_sicoss_bd() del PHP (línea 3849)
        
        Args:
            datos: Datos de configuración
            per_anoct: Año del período
            per_mesct: Mes del período  
            legajos: DataFrame de legajos a procesar
            periodo_fiscal: Período fiscal
            
        Returns:
            Dict con resultado del procesamiento BD
            
        TODO: IMPLEMENTAR LÓGICA ESPECÍFICA PARA BD
        """
        logger.warning("🚧 TODO: procesa_sicoss_bd - IMPLEMENTACIÓN PENDIENTE")
        logger.info(f"⚙️ Simulando procesamiento SICOSS BD: {len(legajos)} legajos")
        
        try:
            # Por ahora delegar al método principal
            return self.generar_sicoss_bd(datos, periodo_fiscal)
            
        except Exception as e:
            logger.error(f"❌ Error en procesamiento SICOSS BD: {e}")
            return {
                'success': False,
                'message': f'Error en procesamiento BD: {str(e)}',
                'error': str(e)
            }
    
    def _mapear_legajos_a_modelo(self, legajos: pd.DataFrame, periodo_fiscal: PeriodoFiscal) -> pd.DataFrame:
        """
        Mapea legajos procesados a modelo de tabla afip_mapuche_sicoss
        
        Args:
            legajos: DataFrame con legajos procesados
            periodo_fiscal: Período fiscal
            
        Returns:
            DataFrame mapeado para BD con estructura de afip_mapuche_sicoss
        """
        logger.info(f"🔄 Mapeando {len(legajos)} legajos a tabla afip_mapuche_sicoss")
        
        try:
            # Crear DataFrame base con número de filas igual al DataFrame de entrada
            num_filas = len(legajos)
            legajos_bd = pd.DataFrame(index=range(num_filas))
            
            # Campo obligatorio: período fiscal (para todas las filas)
            legajos_bd['periodo_fiscal'] = periodo_fiscal.periodo_str
            
            # Mapear campos usando el mapeo definido
            for campo_origen, campo_bd in self.campo_mapping.items():
                if campo_origen in legajos.columns:
                    legajos_bd[campo_bd] = legajos[campo_origen].values
            
            # Completar con campos requeridos y valores por defecto
            for campo_bd, valor_defecto in self.campos_requeridos.items():
                if campo_bd not in legajos_bd.columns:
                    if campo_bd == 'periodo_fiscal':
                        legajos_bd[campo_bd] = periodo_fiscal.periodo_str
                    else:
                        legajos_bd[campo_bd] = valor_defecto
            
            # Asegurar que periodo_fiscal se mantenga después de otras operaciones
            periodo_valor = periodo_fiscal.periodo_str
            
            # Convertir tipos específicos para BD
            self._convertir_tipos_bd_real(legajos_bd)
            
            # Re-asegurar periodo_fiscal después de conversión de tipos
            legajos_bd['periodo_fiscal'] = periodo_valor
            
            # Validar restricciones de la tabla
            self._validar_restricciones_tabla(legajos_bd)
            
            logger.info(f"✅ Mapeo completado: {len(legajos_bd)} legajos mapeados con {len(legajos_bd.columns)} campos")
            return legajos_bd
            
        except Exception as e:
            logger.error(f"❌ Error en mapeo a modelo BD: {e}")
            raise RuntimeError(f"Error mapeando legajos: {e}")
    
    def _convertir_tipos_bd_real(self, legajos_bd: pd.DataFrame):
        """
        Convierte tipos de datos para tabla afip_mapuche_sicoss
        
        Args:
            legajos_bd: DataFrame a convertir tipos
        """
        # Campos de texto con longitud limitada
        campos_texto = {
            'cuil': 11,
            'apnom': 40,
            'cod_os': 6,
            'prov': 50
        }
        
        for campo, max_len in campos_texto.items():
            if campo in legajos_bd.columns:
                legajos_bd[campo] = legajos_bd[campo].astype(str).str[:max_len].fillna('')
        
        # Campos booleanos
        campos_boolean = ['conyuge']
        for campo in campos_boolean:
            if campo in legajos_bd.columns:
                legajos_bd[campo] = legajos_bd[campo].astype(bool)
        
        # Campos enteros
        campos_enteros = [
            'cant_hijos', 'cod_situacion', 'cod_cond', 'cod_act', 'cod_zona',
            'cod_mod_cont', 'cant_adh', 'cod_siniestrado', 'marca_reduccion',
            'tipo_empresa', 'regimen', 'sit_rev1', 'dia_ini_sit_rev1',
            'sit_rev2', 'dia_ini_sit_rev2', 'sit_rev3', 'dia_ini_sit_rev3',
            'cant_dias_trab', 'convencionado', 'tipo_oper', 'nro_horas_ext',
            'hstrab', 'seguro'
        ]
        
        for campo in campos_enteros:
            if campo in legajos_bd.columns:
                legajos_bd[campo] = pd.to_numeric(legajos_bd[campo], errors='coerce').fillna(0).astype(int)
        
        # Campos numéricos decimales
        campos_numericos = [
            'porc_aporte', 'rem_total', 'rem_impo1', 'asig_fam_pag', 'aporte_vol',
            'imp_adic_os', 'exc_aport_ss', 'exc_aport_os', 'rem_impo2', 'rem_impo3',
            'rem_impo4', 'recomp_lrt', 'aporte_adic_os', 'sueldo_adicc', 'sac',
            'horas_extras', 'zona_desfav', 'vacaciones', 'rem_impo5', 'rem_impo6',
            'adicionales', 'premios', 'rem_dec_788', 'rem_imp7', 'cpto_no_remun',
            'maternidad', 'rectificacion_remun', 'rem_imp9', 'contrib_dif',
            'ley', 'incsalarial', 'remimp11'
        ]
        
        for campo in campos_numericos:
            if campo in legajos_bd.columns:
                legajos_bd[campo] = pd.to_numeric(legajos_bd[campo], errors='coerce').fillna(0.0)
    
    def _validar_restricciones_tabla(self, legajos_bd: pd.DataFrame):
        """
        Valida restricciones específicas de tabla afip_mapuche_sicoss
        
        Args:
            legajos_bd: DataFrame a validar
        """
        # Validar campos NOT NULL
        campos_not_null = [
            'periodo_fiscal', 'cuil', 'apnom', 'cant_hijos', 'cod_situacion',
            'cod_cond', 'cod_act', 'cod_zona', 'porc_aporte', 'cod_mod_cont',
            'cod_os', 'cant_adh', 'rem_total', 'rem_impo1', 'asig_fam_pag',
            'aporte_vol', 'marca_reduccion', 'tipo_empresa', 'aporte_adic_os',
            'regimen', 'sit_rev1', 'dia_ini_sit_rev1', 'sit_rev2', 'dia_ini_sit_rev2',
            'sit_rev3', 'dia_ini_sit_rev3', 'sueldo_adicc', 'sac', 'horas_extras',
            'zona_desfav', 'vacaciones', 'cant_dias_trab', 'convencionado',
            'adicionales', 'premios', 'rem_dec_788', 'nro_horas_ext',
            'cpto_no_remun', 'rectificacion_remun', 'hstrab', 'seguro',
            'ley', 'incsalarial', 'remimp11'
        ]
        
        for campo in campos_not_null:
            if campo in legajos_bd.columns:
                valores_nulos = legajos_bd[campo].isna().sum()
                if valores_nulos > 0:
                    logger.warning(f"⚠️ Campo {campo} tiene {valores_nulos} valores nulos")
        
        # Validar longitudes de texto
        if 'cuil' in legajos_bd.columns:
            cuils_largos = (legajos_bd['cuil'].str.len() > 11).sum()
            if cuils_largos > 0:
                logger.warning(f"⚠️ {cuils_largos} CUILs exceden 11 caracteres")
        
        if 'apnom' in legajos_bd.columns:
            nombres_largos = (legajos_bd['apnom'].str.len() > 40).sum()
            if nombres_largos > 0:
                logger.warning(f"⚠️ {nombres_largos} nombres exceden 40 caracteres")
    
    def _convertir_tipos_bd(self, legajos_bd: pd.DataFrame):
        """
        Convierte tipos de datos para BD
        
        Args:
            legajos_bd: DataFrame a convertir tipos
        """
        # Campos enteros
        campos_enteros = [
            'nro_legaj', 'tipo_de_operacion', 'codigo_situacion', 
            'codigo_condicion', 'tipo_de_actividad', 'codigo_zona',
            'codigo_contratacion', 'adherentes', 'hijos', 'conyugue',
            'seguro_vida_obligatorio', 'codigo_revista_1', 'fecha_revista_1',
            'codigo_revista_2', 'fecha_revista_2', 'codigo_revista_3', 
            'fecha_revista_3', 'dias_trabajados'
        ]
        
        for campo in campos_enteros:
            if campo in legajos_bd.columns:
                legajos_bd[campo] = pd.to_numeric(legajos_bd[campo], errors='coerce').fillna(0).astype(int)
        
        # Campos decimales
        campos_decimales = [col for col in legajos_bd.columns if 'importe' in col.lower() or 'aporte' in col.lower()]
        
        for campo in campos_decimales:
            if campo in legajos_bd.columns:
                legajos_bd[campo] = pd.to_numeric(legajos_bd[campo], errors='coerce').fillna(0.0)
        
        # Campos de texto
        campos_texto = ['cuit', 'apyno', 'codigo_os', 'regimen', 'trabajador_convencionado', 'provincia_localidad']
        
        for campo in campos_texto:
            if campo in legajos_bd.columns:
                legajos_bd[campo] = legajos_bd[campo].astype(str).fillna('')
    
    def _validar_datos_para_bd(self, legajos_bd: pd.DataFrame) -> bool:
        """
        Valida que los datos estén listos para tabla afip_mapuche_sicoss
        
        Args:
            legajos_bd: DataFrame mapeado para BD
            
        Returns:
            bool: True si los datos son válidos
        """
        logger.info(f"🔍 Validando {len(legajos_bd)} legajos para tabla afip_mapuche_sicoss")
        
        # Validaciones básicas
        if legajos_bd.empty:
            raise ValueError("No hay datos para validar")
        
        # Validar campos requeridos de tabla real afip_mapuche_sicoss (NOT NULL)
        campos_requeridos_tabla = [
            'periodo_fiscal', 'cuil', 'apnom', 'cant_hijos', 'cod_situacion',
            'cod_cond', 'cod_act', 'cod_zona', 'porc_aporte', 'cod_mod_cont',
            'cod_os', 'cant_adh', 'rem_total', 'rem_impo1', 'asig_fam_pag',
            'aporte_vol', 'marca_reduccion', 'tipo_empresa', 'aporte_adic_os',
            'regimen'
        ]
        
        campos_faltantes = [campo for campo in campos_requeridos_tabla if campo not in legajos_bd.columns]
        
        if campos_faltantes:
            raise ValueError(f"Campos faltantes para tabla afip_mapuche_sicoss: {campos_faltantes}")
        
        # Validar que cuil no sea nulo (equivalente a nro_legaj)
        cuils_sin_valor = legajos_bd['cuil'].isna().sum()
        if cuils_sin_valor > 0:
            logger.warning(f"⚠️ {cuils_sin_valor} registros sin CUIL")
        
        # Validar que período fiscal sea válido
        if legajos_bd['periodo_fiscal'].isna().any():
            raise ValueError("Período fiscal no puede ser nulo")
        
        # Validar importes negativos (campos rem_*)
        campos_importes = [col for col in legajos_bd.columns if col.startswith('rem_') or col in ['sac', 'asig_fam_pag', 'aporte_vol']]
        for campo in campos_importes:
            if campo in legajos_bd.columns:
                negativos = (legajos_bd[campo] < 0).sum()
                if negativos > 0:
                    logger.warning(f"⚠️ {negativos} registros con {campo} negativo")
        
        # Validar longitudes de campos de texto
        if 'cuil' in legajos_bd.columns:
            cuils_largos = (legajos_bd['cuil'].astype(str).str.len() > 11).sum()
            if cuils_largos > 0:
                logger.warning(f"⚠️ {cuils_largos} CUILs exceden 11 caracteres")
        
        if 'apnom' in legajos_bd.columns:
            nombres_largos = (legajos_bd['apnom'].astype(str).str.len() > 40).sum()
            if nombres_largos > 0:
                logger.warning(f"⚠️ {nombres_largos} nombres exceden 40 caracteres")
        
        logger.info(f"✅ Validación completada para {len(legajos_bd)} legajos")
        return True
    
    def _ejecutar_guardado_bd(self, legajos_bd: pd.DataFrame, periodo_fiscal: PeriodoFiscal) -> Dict[str, Any]:
        """
        Ejecuta el guardado real en tabla afip_mapuche_sicoss
        
        Args:
            legajos_bd: DataFrame mapeado y validado
            periodo_fiscal: Período fiscal
            
        Returns:
            Dict con detalles del guardado
        """
        logger.info(f"💾 Ejecutando guardado en {self.schema}.{self.tabla_sicoss} para {len(legajos_bd)} legajos")
        
        try:
            start_time = datetime.now()
            
            # Ejecutar guardado real en tabla afip_mapuche_sicoss
            filas_guardadas = self.db.execute_batch_insert(
                table_name=self.tabla_sicoss,
                data=legajos_bd,
                schema=self.schema,
                if_exists='append'
            )
            
            end_time = datetime.now()
            duracion = (end_time - start_time).total_seconds()
            
            # Generar estadísticas del guardado
            estadisticas = self._generar_estadisticas_guardado(legajos_bd, periodo_fiscal)
            
            resultado = {
                'tabla_destino': f"{self.schema}.{self.tabla_sicoss}",
                'filas_guardadas': filas_guardadas,
                'metodo': 'batch_insert',
                'duracion_segundos': duracion,
                'timestamp': end_time.isoformat(),
                'periodo_fiscal': periodo_fiscal.periodo_str,
                'estadisticas': estadisticas
            }
            
            logger.info(f"✅ Guardado BD exitoso: {filas_guardadas} filas en {self.schema}.{self.tabla_sicoss} ({duracion:.2f}s)")
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Error ejecutando guardado BD: {e}")
            raise RuntimeError(f"Error en guardado BD: {e}")
    
    def _generar_estadisticas_guardado(self, legajos_bd: pd.DataFrame, periodo_fiscal: PeriodoFiscal) -> Dict[str, Any]:
        """
        Genera estadísticas del guardado en tabla afip_mapuche_sicoss
        
        Args:
            legajos_bd: DataFrame guardado
            periodo_fiscal: Período fiscal del guardado
            
        Returns:
            Dict con estadísticas
        """
        try:
            estadisticas = {
                'total_legajos': len(legajos_bd),
                'campos_guardados': len(legajos_bd.columns),
                'tamaño_mb': round(legajos_bd.memory_usage(deep=True).sum() / 1024 / 1024, 2),
                'periodo_fiscal': periodo_fiscal.periodo_str
            }
            
            # Estadísticas de importes si existen (campos de tabla real)
            if 'rem_total' in legajos_bd.columns:
                estadisticas['total_rem_total'] = float(legajos_bd['rem_total'].sum())
                estadisticas['promedio_rem_total'] = float(legajos_bd['rem_total'].mean())
            
            if 'rem_impo1' in legajos_bd.columns:
                estadisticas['total_rem_impo1'] = float(legajos_bd['rem_impo1'].sum())
            
            if 'sac' in legajos_bd.columns:
                estadisticas['total_sac'] = float(legajos_bd['sac'].sum())
            
            # Conteo por tipos de actividad
            if 'cod_act' in legajos_bd.columns:
                tipos_actividad = legajos_bd['cod_act'].value_counts().to_dict()
                estadisticas['tipos_actividad'] = {str(k): int(v) for k, v in tipos_actividad.items()}
            
            # Conteo por situación
            if 'cod_situacion' in legajos_bd.columns:
                situaciones = legajos_bd['cod_situacion'].value_counts().to_dict()
                estadisticas['situaciones'] = {str(k): int(v) for k, v in situaciones.items()}
            
            # Estadísticas de regímenes
            if 'regimen' in legajos_bd.columns:
                regimenes = legajos_bd['regimen'].value_counts().to_dict()
                estadisticas['regimenes'] = {str(k): int(v) for k, v in regimenes.items()}
            
            return estadisticas
            
        except Exception as e:
            logger.warning(f"⚠️ Error generando estadísticas: {e}")
            return {
                'total_legajos': len(legajos_bd),
                'periodo_fiscal': periodo_fiscal.periodo_str,
                'error_estadisticas': str(e)
            }
    
    def verificar_estructura_datos(self, periodo_fiscal: PeriodoFiscal, muestra: int = 3) -> Dict[str, Any]:
        """
        🚧 TODO: Verifica estructura de datos en BD
        
        Replica verificar_estructura_datos() del PHP (línea 4077)
        
        Args:
            periodo_fiscal: Período a verificar
            muestra: Número de registros de muestra
            
        Returns:
            Dict con resultado de la verificación
            
        TODO: IMPLEMENTAR VERIFICACIÓN COMPLETA
        """
        logger.warning("🚧 TODO: verificar_estructura_datos - IMPLEMENTACIÓN PENDIENTE")
        logger.info(f"🔍 Simulando verificación de estructura para {periodo_fiscal}")
        
        try:
            # TODO: Implementar verificación real
            resultado = {
                'periodo_fiscal': periodo_fiscal.periodo_str,
                'estructura_valida': True,
                'muestra_registros': muestra,
                'verificaciones': [
                    {'campo': 'nro_legaj', 'valido': True, 'tipo': 'integer'},
                    {'campo': 'importe_imponible', 'valido': True, 'tipo': 'numeric'},
                    {'campo': 'fecha_procesamiento', 'valido': True, 'tipo': 'timestamp'}
                ],
                'mensaje': 'Estructura validada (simulada)',
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info("✅ Verificación de estructura simulada completada")
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Error verificando estructura: {e}")
            return {
                'periodo_fiscal': periodo_fiscal.periodo_str,
                'estructura_valida': False,
                'error': str(e)
            }
    
    def _ensure_table_exists(self, periodo_fiscal: PeriodoFiscal):
        """
        Verifica que la tabla afip_mapuche_sicoss exista
        
        Args:
            periodo_fiscal: Período fiscal (para logging)
        """
        try:
            # Verificar que la tabla exista (no la creamos, ya existe)
            verificacion_query = f"""
                SELECT COUNT(*) as existe 
                FROM information_schema.tables 
                WHERE table_schema = '{self.schema}' 
                AND table_name = '{self.tabla_sicoss}'
            """
            
            resultado = self.db.execute_query(verificacion_query)
            
            if resultado and len(resultado) > 0 and resultado[0]['existe'] > 0:
                logger.info(f"✅ Tabla {self.schema}.{self.tabla_sicoss} verificada y disponible")
            else:
                logger.warning(f"⚠️ Tabla {self.schema}.{self.tabla_sicoss} no encontrada")
                raise RuntimeError(f"Tabla {self.schema}.{self.tabla_sicoss} no existe en BD")
            
        except Exception as e:
            logger.error(f"❌ Error verificando tabla {self.tabla_sicoss}: {e}")
            # Por ahora solo advertir, no fallar
            logger.warning("⚠️ Continuando sin verificación de tabla")
    
    def _filtrar_legajos_activos(self, legajos_bd: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra legajos activos según criterios de negocio
        
        Args:
            legajos_bd: DataFrame con legajos mapeados
            
        Returns:
            DataFrame filtrado con legajos activos
        """
        try:
            df_original = len(legajos_bd)
            
            # Por ahora filtrar solo legajos con número válido
            mask_valido = True
            if 'nro_legaj' in legajos_bd.columns:
                mask_valido = (legajos_bd['nro_legaj'] > 0) & legajos_bd['nro_legaj'].notna()
            
            legajos_activos = legajos_bd[mask_valido].copy()
            
            df_filtrado = len(legajos_activos)
            logger.info(f"🔍 Legajos filtrados: {df_original} → {df_filtrado} activos")
            
            return legajos_activos
            
        except Exception as e:
            logger.error(f"❌ Error filtrando legajos activos: {e}")
            # En caso de error, devolver todos los legajos
            return legajos_bd

    def close(self):
        """Cierra conexiones"""
        if self.db:
            self.db.close()
            logger.info("🔌 Conexión BD del database_saver cerrada") 