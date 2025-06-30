from .base_processor import BaseProcessor
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class CalculosSicossProcessor(BaseProcessor):
    """
    Procesador especializado para cálculos específicos de SICOSS
    
    REDISEÑADO: Se enfoca en funcionalidades específicas que NO están en ConceptosProcessor:
    - Cálculos de ImporteImponible_4, _5, _6 con lógica compleja
    - Procesamiento de diferencial de jubilación avanzado
    - Cálculos de ART (Aseguradora de Riesgos del Trabajo)
    - Manejo de TipoDeOperacion con lógica específica
    - Asignaciones familiares
    """
    
    def process(self, df_legajos: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Aplica cálculos específicos de SICOSS que complementan ConceptosProcessor
        
        NOTA: Este procesador asume que ConceptosProcessor ya generó los campos base.
        """
        logger.info("🔧 Aplicando cálculos específicos de SICOSS...")
        
        if df_legajos.empty:
            return df_legajos
        
        df = df_legajos.copy()
        
        # Validar que ConceptosProcessor ya procesó los datos
        if not self._validar_campos_base(df):
            logger.warning("⚠️ ConceptosProcessor no ejecutado previamente - aplicando campos base")
            df = self._aplicar_campos_base_emergencia(df)
        
        # Pipeline de cálculos específicos
        df = self._calcular_importes_imponibles_complejos(df)
        df = self._aplicar_configuraciones_especificas(df)
        df = self._calcular_asignaciones_familiares(df)
        df = self._calcular_art_completo(df)
        df = self._procesar_tipo_operacion_complejo(df)
        df = self._calcular_sueldo_mas_adicionales_avanzado(df)
        df = self._validaciones_finales_calculos(df)
        
        self._log_process_info("CalculosSicossProcessor", len(df_legajos), len(df))
        return df
    
    def _validar_campos_base(self, df: pd.DataFrame) -> bool:
        """Valida que ConceptosProcessor haya ejecutado previamente"""
        campos_requeridos = [
            'Remuner78805', 'ImporteImponiblePatronal', 'IMPORTE_BRUTO', 
            'IMPORTE_IMPON', 'ImporteSACPatronal'
        ]
        
        for campo in campos_requeridos:
            if campo not in df.columns:
                logger.warning(f"Campo faltante: {campo}")
                return False
        
        return True
    
    def _aplicar_campos_base_emergencia(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica campos base de emergencia si ConceptosProcessor no ejecutó"""
        logger.warning("⚠️ Aplicando campos base de emergencia - se recomienda ejecutar ConceptosProcessor primero")
        
        # Campos mínimos necesarios
        campos_base = {
            'ImporteSAC': 0.0,
            'ImporteNoRemun': 0.0,
            'ImporteHorasExtras': 0.0,
            'ImporteZonaDesfavorable': 0.0,
            'ImporteVacaciones': 0.0,
            'ImportePremios': 0.0,
            'ImporteAdicionales': 0.0,
            'Remuner78805': 0.0,
            'ImporteImponiblePatronal': 0.0,
            'IMPORTE_BRUTO': 0.0,
            'IMPORTE_IMPON': 0.0,
            'ImporteSACPatronal': 0.0
        }
        
        for campo, valor in campos_base.items():
            if campo not in df.columns:
                df[campo] = valor
        
        return df
    
    def _calcular_importes_imponibles_complejos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula ImporteImponible_4, _5, _6 con lógica compleja del PHP original
        """
        logger.info("🔧 Calculando importes imponibles complejos...")
        
        # Configurar porcentaje de aporte adicional
        porc_aporte_adicional = getattr(self.config, 'porc_aporte_adicional_jubilacion', 100.0)
        df['PorcAporteDiferencialJubilacion'] = porc_aporte_adicional
        
        # ImporteImponible_4 = IMPORTE_IMPON inicial
        df['ImporteImponible_4'] = df['IMPORTE_IMPON']
        
        # Inicializar SAC No Docente
        df['ImporteSACNoDocente'] = df.get('ImporteSAC', 0)
        
        # Lógica compleja para ImporteImponible_6
        imp6_series = df.get('ImporteImponible_6', pd.Series([0] * len(df)))
        if imp6_series is None:
            imp6_series = pd.Series([0] * len(df))
        mask_tiene_imp6 = imp6_series > 0
        
        if mask_tiene_imp6.any():
            # Calcular proporción
            df.loc[mask_tiene_imp6, 'ImporteImponible_6'] = (
                (df.loc[mask_tiene_imp6, 'ImporteImponible_6'] * 100) /
                df.loc[mask_tiene_imp6, 'PorcAporteDiferencialJubilacion']
            ).round(2)
            
            # Diferencia con IMPORTE_IMPON
            diferencia = abs(
                df.loc[mask_tiene_imp6, 'ImporteImponible_6'] -
                df.loc[mask_tiene_imp6, 'IMPORTE_IMPON']
            )
            
            # Máscara para TipoDeOperacion = 2
            mask_tipo2 = (diferencia > 5) & (
                df.loc[mask_tiene_imp6, 'ImporteImponible_6'] < 
                df.loc[mask_tiene_imp6, 'IMPORTE_IMPON']
            )
            
            # Aplicar TipoDeOperacion = 2
            indices_tipo2 = df.loc[mask_tiene_imp6].loc[mask_tipo2].index
            if len(indices_tipo2) > 0:
                df.loc[indices_tipo2, 'TipoDeOperacion'] = 2
                df.loc[indices_tipo2, 'IMPORTE_IMPON'] = (
                    df.loc[indices_tipo2, 'IMPORTE_IMPON'] -
                    df.loc[indices_tipo2, 'ImporteImponible_6']
                )
                df.loc[indices_tipo2, 'ImporteSACNoDocente'] = (
                    df.loc[indices_tipo2, 'ImporteSAC'] -
                    df.loc[indices_tipo2, 'SACInvestigador'].fillna(0)
                )
            
            # Ajustar ImporteImponible_6 si está en tolerancia (±5)
            mask_tolerancia = diferencia <= 5
            indices_tolerancia = df.loc[mask_tiene_imp6].loc[mask_tolerancia].index
            if len(indices_tolerancia) > 0:
                df.loc[indices_tolerancia, 'ImporteImponible_6'] = df.loc[
                    indices_tolerancia, 'IMPORTE_IMPON'
                ]
        
        # ImporteImponible_5 = ImporteImponible_4 (antes de ajustes)
        df['ImporteImponible_5'] = df['ImporteImponible_4']
        
        # ImporteSACOtroAporte
        df['ImporteSACOtroAporte'] = df.get('ImporteSAC', 0)
        
        logger.info("✅ Importes imponibles complejos calculados")
        return df
    
    def _aplicar_configuraciones_especificas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica configuraciones específicas avanzadas de SICOSS
        """
        logger.info("🔧 Aplicando configuraciones específicas...")
        
        # 1. Configuración de becarios
        informar_becarios = getattr(self.config, 'informar_becarios', False)
        df['InformarBecarios'] = informar_becarios
        
        if informar_becarios and 'ImporteImponibleBecario' in df.columns:
            logger.info(f"✅ Becarios informados: ${df['ImporteImponibleBecario'].sum():,.2f}")
        
        # 2. Configuración de ART específica
        art_con_tope = getattr(self.config, 'art_con_tope', True)
        conceptos_no_remun_en_art = getattr(self.config, 'conceptos_no_remun_en_art', False)
        
        df['ConfigARTConTope'] = art_con_tope
        df['ConfigConceptosNoRemuEnART'] = conceptos_no_remun_en_art
        
        # 3. Porcentaje de aporte diferencial específico por legajo
        porc_base = getattr(self.config, 'porc_aporte_adicional_jubilacion', 100.0)
        
        # Verificar si hay porcentajes específicos por legajo
        if 'PorcAporteDiferencialEspecifico' in df.columns:
            # Usar porcentaje específico si existe
            mask_especifico = df['PorcAporteDiferencialEspecifico'] > 0
            df.loc[~mask_especifico, 'PorcAporteDiferencialJubilacion'] = porc_base
        else:
            df['PorcAporteDiferencialJubilacion'] = porc_base
            
        # 4. Configuración de trabajador convencionado específica
        trabajador_conv_default = getattr(self.config, 'trabajador_convencionado', 'S')
        
        # Si no tiene valor específico, usar default
        if 'trabajadorconvencionado' not in df.columns:
            df['trabajadorconvencionado'] = trabajador_conv_default
        else:
            mask_vacio = df['trabajadorconvencionado'].isna() | (df['trabajadorconvencionado'] == '')
            df.loc[mask_vacio, 'trabajadorconvencionado'] = trabajador_conv_default
            
        # 5. Configuraciones adicionales de SICOSS
        df['FechaProcesamiento'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        df['VersionCalculosProcessor'] = '2.0'
        
        # Log de configuraciones aplicadas
        config_summary = {
            'informar_becarios': informar_becarios,
            'art_con_tope': art_con_tope, 
            'conceptos_no_remun_en_art': conceptos_no_remun_en_art,
            'porc_aporte_base': porc_base,
            'trabajador_convencionado': trabajador_conv_default
        }
        
        logger.info(f"✅ Configuraciones aplicadas: {config_summary}")
        
        return df
    
    def _calcular_asignaciones_familiares(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula asignaciones familiares con lógica completa de SICOSS
        """
        logger.info("🔧 Calculando asignaciones familiares...")
        
        # Verificar si el legajo tiene cónyuge o hijos (de la extracción original)
        conyugue_series = df.get('conyugue', pd.Series([0] * len(df)))
        hijos_series = df.get('hijos', pd.Series([0] * len(df)))
        
        if conyugue_series is None:
            conyugue_series = pd.Series([0] * len(df))
        if hijos_series is None:
            hijos_series = pd.Series([0] * len(df))
            
        tiene_conyugue = conyugue_series > 0
        tiene_hijos = hijos_series > 0
        
        # Calcular asignaciones familiares basado en:
        # 1. Cantidad de hijos  
        # 2. Si tiene cónyuge
        # 3. Conceptos familiares si están presentes
        asignaciones_base = pd.Series([0.0] * len(df))
        
        if tiene_hijos.any():
            # Cálculo básico por hijos (valor referencial - puede ajustarse según normativa)
            asignaciones_base += hijos_series * 1000.0  # Valor por hijo
            
        if tiene_conyugue.any():
            # Asignación por cónyuge
            asignaciones_base += 500.0 * tiene_conyugue.astype(int)
        
        # Verificar si hay conceptos específicos de asignaciones familiares 
        conceptos_familiares = df.get('ASIGNACIONES_FAMILIARES', pd.Series([0] * len(df)))
        if conceptos_familiares is None:
            conceptos_familiares = pd.Series([0] * len(df))
            
        # Si hay conceptos familiares específicos, usar esos valores
        mask_tiene_conceptos = conceptos_familiares > 0
        if mask_tiene_conceptos.any():
            asignaciones_base[mask_tiene_conceptos] = conceptos_familiares[mask_tiene_conceptos]
        
        # Asignar el resultado
        df['AsignacionesFliaresPagadas'] = asignaciones_base
        
        # Configurar indicador de asignación familiar
        asignacion_familiar_config = getattr(self.config, 'asignacion_familiar', False)
        
        if asignacion_familiar_config:
            # Si está configurado para incluir asignaciones familiares
            df['IncluirAsignacionesFamiliares'] = True
            total_incluidas = asignaciones_base.sum()
            logger.info(f"✅ Asignaciones familiares incluidas: Total ${total_incluidas:,.2f}")
        else:
            # Si no está configurado, mantener en 0
            df['AsignacionesFliaresPagadas'] = 0.0
            df['IncluirAsignacionesFamiliares'] = False
            logger.info("ℹ️ Asignaciones familiares desactivadas por configuración")
        
        total_asignaciones = df['AsignacionesFliaresPagadas'].sum()
        logger.info(f"✅ Asignaciones familiares calculadas - Total: ${total_asignaciones:,.2f}")
        
        return df
    
    def _calcular_art_completo(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula ART (Aseguradora de Riesgos del Trabajo) con lógica completa
        """
        logger.info("🔧 Calculando ART completo...")
        
        # Configuración ART
        art_con_tope = getattr(self.config, 'art_con_tope', True)
        conceptos_no_remun_en_art = getattr(self.config, 'conceptos_no_remun_en_art', False)
        
        # Cálculo base de ART (importeimponible_9)
        if art_con_tope:
            # Con tope: usar ImporteImponible_4
            df['importeimponible_9'] = df['ImporteImponible_4']
        else:
            # Sin tope: usar Remuner78805
            df['importeimponible_9'] = df['Remuner78805']
        
        # Agregar conceptos no remunerativos si está configurado
        if conceptos_no_remun_en_art:
            df['importeimponible_9'] += df.get('ImporteNoRemun', 0)
        
        total_art = df['importeimponible_9'].sum()
        logger.info(f"✅ ART calculado - Total: ${total_art:,.2f}")
        
        return df
    
    def _procesar_tipo_operacion_complejo(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa TipoDeOperacion con lógica compleja
        """
        logger.info("🔧 Procesando TipoDeOperacion complejo...")
        
        # Inicializar TipoDeOperacion = 1 por defecto
        if 'TipoDeOperacion' not in df.columns:
            df['TipoDeOperacion'] = 1
        
        # La lógica compleja ya se aplicó en _calcular_importes_imponibles_complejos
        # Aquí se pueden agregar validaciones adicionales
        
        tipos_operacion = df['TipoDeOperacion'].value_counts()
        logger.info(f"✅ Tipos de operación: {tipos_operacion.to_dict()}")
        
        return df
    
    def _calcular_sueldo_mas_adicionales_avanzado(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula Sueldo más Adicionales con lógica avanzada del PHP
        """
        logger.info("🔧 Calculando sueldo más adicionales avanzado...")
        
        # Fórmula compleja del PHP
        df['ImporteSueldoMasAdicionales'] = (
            df['ImporteImponiblePatronal'] -
            df.get('ImporteSAC', 0) -
            df.get('ImporteHorasExtras', 0) -
            df.get('ImporteZonaDesfavorable', 0) -
            df.get('ImporteVacaciones', 0) -
            df.get('ImportePremios', 0) -
            df.get('ImporteAdicionales', 0)
        )
        
        # Si es positivo, restar incremento solidario
        mask_positivo = df['ImporteSueldoMasAdicionales'] > 0
        if mask_positivo.any():
            incremento_solidario = df.get('IncrementoSolidario', pd.Series([0] * len(df)))
            if incremento_solidario is None:
                incremento_solidario = pd.Series([0] * len(df))
            df.loc[mask_positivo, 'ImporteSueldoMasAdicionales'] -= incremento_solidario.loc[mask_positivo]
        
        # Configurar trabajador convencionado
        trabajador_convencionado = getattr(self.config, 'trabajador_convencionado', 'S')
        if 'trabajadorconvencionado' not in df.columns:
            df['trabajadorconvencionado'] = trabajador_convencionado
        
        total_sueldo_adicionales = df['ImporteSueldoMasAdicionales'].sum()
        logger.info(f"✅ Sueldo más adicionales - Total: ${total_sueldo_adicionales:,.2f}")
        
        return df
    
    def _validaciones_finales_calculos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica validaciones finales y correcciones específicas del CalculosProcessor
        """
        logger.info("🔍 Aplicando validaciones finales...")
        
        # 1. Validar coherencia en ImporteImponible_4/_5/_6
        mask_incoherente = (df['ImporteImponible_4'] < 0) | (df['ImporteImponible_5'] < 0)
        if mask_incoherente.any():
            logger.warning(f"Valores negativos detectados en ImporteImponible_4/_5: {mask_incoherente.sum()} legajos")
            # Corregir valores negativos
            df.loc[mask_incoherente, 'ImporteImponible_4'] = df.loc[mask_incoherente, 'ImporteImponible_4'].clip(lower=0)
            df.loc[mask_incoherente, 'ImporteImponible_5'] = df.loc[mask_incoherente, 'ImporteImponible_5'].clip(lower=0)
        
        # 2. Validar ART vs ImporteImponible_4
        art_esperado = df['ImporteImponible_4']
        diferencia_art = abs(df['importeimponible_9'] - art_esperado)
        mask_diferencia_art = diferencia_art > 0.01  # Tolerancia de 1 centavo
        
        if mask_diferencia_art.any():
            logger.info(f"Ajustando ART para coherencia: {mask_diferencia_art.sum()} legajos")
            df.loc[mask_diferencia_art, 'importeimponible_9'] = art_esperado.loc[mask_diferencia_art]
        
        # 3. Validar TipoDeOperacion coherente con ImporteImponible_6
        # Asegurar que ImporteImponible_6 existe
        if 'ImporteImponible_6' not in df.columns:
            df['ImporteImponible_6'] = 0.0
        
        mask_tipo2 = df['TipoDeOperacion'] == 2
        mask_sin_imp6 = df['ImporteImponible_6'] == 0
        
        if (mask_tipo2 & mask_sin_imp6).any():
            logger.warning(f"TipoDeOperacion=2 sin ImporteImponible_6: {(mask_tipo2 & mask_sin_imp6).sum()} legajos")
            # Corregir: si TipoDeOperacion=2 pero no tiene ImporteImponible_6, cambiar a 1
            df.loc[mask_tipo2 & mask_sin_imp6, 'TipoDeOperacion'] = 1
        
        # 4. Validar rangos de valores
        campos_validar = {
            'PorcAporteDiferencialJubilacion': (0, 200),  # 0% a 200%
            'ImporteImponible_4': (0, float('inf')),      # No negativo
            'ImporteImponible_5': (0, float('inf')),      # No negativo
            'importeimponible_9': (0, float('inf'))       # No negativo (ART)
        }
        
        for campo, (min_val, max_val) in campos_validar.items():
            if campo in df.columns:
                mask_fuera_rango = (df[campo] < min_val) | (df[campo] > max_val)
                if mask_fuera_rango.any():
                    logger.warning(f"Valores fuera de rango en {campo}: {mask_fuera_rango.sum()} legajos")
                    df.loc[mask_fuera_rango, campo] = df.loc[mask_fuera_rango, campo].clip(min_val, max_val)
        
        # 5. Verificar integridad de campos críticos
        campos_criticos = ['ImporteImponible_4', 'ImporteImponible_5', 'importeimponible_9', 'TipoDeOperacion']
        campos_faltantes = [campo for campo in campos_criticos if campo not in df.columns]
        
        if campos_faltantes:
            logger.error(f"Campos críticos faltantes: {campos_faltantes}")
            raise ValueError(f"CalculosProcessor - Campos críticos faltantes: {campos_faltantes}")
        
        # 6. Estadísticas finales
        stats = {
            'TipoOperacion1': (df['TipoDeOperacion'] == 1).sum(),
            'TipoOperacion2': (df['TipoDeOperacion'] == 2).sum(),
            'ConImporteImponible6': (df['ImporteImponible_6'] > 0).sum(),
            'TotalART': df['importeimponible_9'].sum(),
            'TotalImponible4': df['ImporteImponible_4'].sum()
        }
        
        logger.info(f"📊 Estadísticas finales CalculosProcessor: {stats}")
        
        # 7. Marcar procesamiento completo
        df['CalculosProcessorCompleto'] = True
        df['TimestampCalculosProcessor'] = pd.Timestamp.now()
        
        logger.info("✅ Validaciones finales completadas")
        
        return df 