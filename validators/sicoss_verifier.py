#!/usr/bin/env python3
"""
SicossVerifier - Verificador de consistencia entre Python SICOSS y PHP Legacy

Compara resultados entre el sistema Python nuevo y el PHP legacy para validar
que la migración mantiene la misma lógica y resultados.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import logging
from decimal import Decimal, ROUND_HALF_UP
import json
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class ToleranciaComparacion:
    """Configuración de tolerancias para la comparación"""
    tolerancia_monetaria: float = 0.01  # 1 centavo
    tolerancia_porcentual: float = 0.001  # 0.1%
    tolerancia_booleana: bool = False  # Exacto para booleanos
    tolerancia_enteros: int = 0  # Exacto para enteros
    
@dataclass
class ResultadoComparacion:
    """Resultado de una comparación individual"""
    campo: str
    legajo: int
    valor_python: Any
    valor_php: Any
    diferencia: float
    porcentaje_diferencia: float
    es_coincidente: bool
    tipo_diferencia: str  # 'exacto', 'tolerancia', 'error'

@dataclass
class ReporteVerificacion:
    """Reporte completo de verificación"""
    total_legajos: int
    total_campos: int
    coincidencias_exactas: int
    coincidencias_tolerancia: int
    diferencias_criticas: int
    porcentaje_coincidencia: float
    tiempo_verificacion: float
    detalles_diferencias: List[ResultadoComparacion]
    resumen_estadistico: Dict[str, Any]
    recomendaciones: List[str]

class SicossVerifier:
    """
    Verificador de consistencia entre resultados Python SICOSS y PHP Legacy
    
    Funcionalidades:
    - Comparación campo por campo con tolerancias configurables
    - Análisis estadístico de diferencias
    - Generación de reportes detallados
    - Identificación de patrones de error
    - Recomendaciones para ajustes
    """
    
    def __init__(self, tolerancia: Optional[ToleranciaComparacion] = None):
        self.tolerancia = tolerancia or ToleranciaComparacion()
        self.campos_monetarios = {
            'IMPORTE_BRUTO', 'IMPORTE_IMPON', 'ImporteSAC', 'ImporteImponible_4',
            'ImporteImponible_5', 'ImporteImponible_6', 'Remuner78805',
            'ImporteImponiblePatronal', 'ImporteSACPatronal', 'importeimponible_9',
            'DiferenciaSACImponibleConTope', 'DiferenciaImponibleConTope',
            'ImporteHorasExtras', 'ImporteVacaciones', 'ImporteAdicionales',
            'ImportePremios', 'ImporteNoRemun', 'ImporteZonaDesfavorable'
        }
        self.campos_enteros = {
            'nro_legaj', 'TipoDeOperacion', 'PrioridadTipoDeActividad'
        }
        self.campos_booleanos = {
            'SeguroVidaObligatorio', 'trabajadorconvencionado'
        }
        
    def verificar_resultados(self, 
                           df_python: pd.DataFrame, 
                           df_php: pd.DataFrame,
                           campos_criticos: Optional[List[str]] = None) -> ReporteVerificacion:
        """
        Verifica consistencia entre resultados Python y PHP
        
        Args:
            df_python: DataFrame con resultados del sistema Python
            df_php: DataFrame con resultados del sistema PHP legacy  
            campos_criticos: Lista de campos críticos para verificar
            
        Returns:
            ReporteVerificacion: Reporte completo de la verificación
        """
        logger.info("🔍 Iniciando verificación Python vs PHP...")
        start_time = datetime.now()
        
        # Validaciones iniciales
        if df_python.empty or df_php.empty:
            raise ValueError("Los DataFrames no pueden estar vacíos")
            
        # Preparar datos para comparación
        df_python_prep, df_php_prep = self._preparar_datos(df_python, df_php)
        
        # Determinar campos a comparar
        if campos_criticos is None:
            campos_criticos = self._obtener_campos_criticos(df_python_prep, df_php_prep)
            
        logger.info(f"📊 Comparando {len(campos_criticos)} campos en {len(df_python_prep)} legajos")
        
        # Realizar comparaciones
        resultados_comparacion = []
        
        for campo in campos_criticos:
            if campo not in df_python_prep.columns or campo not in df_php_prep.columns:
                logger.warning(f"Campo {campo} no existe en ambos DataFrames")
                continue
                
            resultados_campo = self._comparar_campo(
                df_python_prep, df_php_prep, campo
            )
            resultados_comparacion.extend(resultados_campo)
        
        # Generar reporte
        elapsed_time = (datetime.now() - start_time).total_seconds()
        reporte = self._generar_reporte(resultados_comparacion, elapsed_time)
        
        logger.info(f"✅ Verificación completada en {elapsed_time:.2f}s")
        logger.info(f"📈 Coincidencia total: {reporte.porcentaje_coincidencia:.2f}%")
        
        return reporte
    
    def _preparar_datos(self, df_python: pd.DataFrame, df_php: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Prepara y normaliza los DataFrames para comparación"""
        logger.info("🔧 Preparando datos para comparación...")
        
        # Copiar DataFrames
        df_py = df_python.copy()
        df_php = df_php.copy()
        
        # Asegurar que nro_legaj sea la clave
        if 'nro_legaj' not in df_py.columns or 'nro_legaj' not in df_php.columns:
            raise ValueError("Ambos DataFrames deben tener columna 'nro_legaj'")
        
        # Ordenar por nro_legaj
        df_py = df_py.sort_values('nro_legaj').reset_index(drop=True)
        df_php = df_php.sort_values('nro_legaj').reset_index(drop=True)
        
        # Obtener legajos comunes
        legajos_comunes = set(df_py['nro_legaj']) & set(df_php['nro_legaj'])
        logger.info(f"📋 Legajos comunes para comparación: {len(legajos_comunes)}")
        
        # Filtrar solo legajos comunes
        df_py = df_py[df_py['nro_legaj'].isin(list(legajos_comunes))]
        df_php = df_php[df_php['nro_legaj'].isin(list(legajos_comunes))]
        
        # Normalizar tipos de datos
        df_py = self._normalizar_tipos(df_py)
        df_php = self._normalizar_tipos(df_php)
        
        return df_py, df_php
    
    def _normalizar_tipos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza tipos de datos para comparación consistente"""
        df = df.copy()
        
        # Convertir campos monetarios a float
        for campo in self.campos_monetarios:
            if campo in df.columns:
                df[campo] = pd.to_numeric(df[campo], errors='coerce')
                df[campo] = df[campo].fillna(0.0)
        
        # Convertir campos enteros
        for campo in self.campos_enteros:
            if campo in df.columns:
                df[campo] = pd.to_numeric(df[campo], errors='coerce')
                df[campo] = df[campo].fillna(0).astype(int)
        
        # Normalizar campos booleanos
        for campo in self.campos_booleanos:
            if campo in df.columns:
                # Convertir diferentes representaciones booleanas
                df[campo] = df[campo].apply(self._normalizar_booleano)
        
        return df
    
    def _normalizar_booleano(self, valor: Any) -> bool:
        """Normaliza diferentes representaciones booleanas"""
        if pd.isna(valor):
            return False
        if isinstance(valor, bool):
            return valor
        if isinstance(valor, (int, float)):
            return valor != 0
        if isinstance(valor, str):
            return valor.upper() in ['TRUE', '1', 'S', 'SÍ', 'SI', 'YES', 'Y']
        return False
    
    def _obtener_campos_criticos(self, df_python: pd.DataFrame, df_php: pd.DataFrame) -> List[str]:
        """Obtiene la lista de campos críticos para comparar"""
        campos_python = set(df_python.columns)
        campos_php = set(df_php.columns)
        campos_comunes = campos_python & campos_php
        
        # Excluir campos no críticos
        campos_excluir = {
            'CalculosProcessorCompleto', 'TimestampCalculosProcessor',
            'ConceptosProcessorCompleto', 'TimestampConceptosProcessor',
            'TopesProcessorCompleto', 'TimestampTopesProcessor'
        }
        
        campos_criticos = list(campos_comunes - campos_excluir)
        return sorted(campos_criticos)
    
    def _comparar_campo(self, df_python: pd.DataFrame, df_php: pd.DataFrame, campo: str) -> List[ResultadoComparacion]:
        """Compara un campo específico entre ambos DataFrames"""
        resultados = []
        
        for idx, row_py in df_python.iterrows():
            nro_legaj = int(row_py['nro_legaj'])
            row_php = df_php[df_php['nro_legaj'] == nro_legaj]
            
            if row_php.empty:
                logger.warning(f"Legajo {nro_legaj} no encontrado en PHP")
                continue
                
            valor_python = row_py[campo] if campo in row_py else None
            valor_php = row_php[campo].values[0] if campo in row_php.columns else None # type: ignore
            
            resultado = self._comparar_valores(campo, nro_legaj, valor_python, valor_php)
            resultados.append(resultado)
        
        return resultados
    
    def _comparar_valores(self, campo: str, legajo: int, valor_python: Any, valor_php: Any) -> ResultadoComparacion:
        """Compara dos valores individuales con tolerancias apropiadas"""
        
        # Manejar valores None/NaN
        if pd.isna(valor_python) and pd.isna(valor_php):
            return ResultadoComparacion(
                campo=campo, legajo=legajo,
                valor_python=valor_python, valor_php=valor_php,
                diferencia=0.0, porcentaje_diferencia=0.0,
                es_coincidente=True, tipo_diferencia='exacto'
            )
        
        if pd.isna(valor_python) or pd.isna(valor_php):
            return ResultadoComparacion(
                campo=campo, legajo=legajo,
                valor_python=valor_python, valor_php=valor_php,
                diferencia=float('inf'), porcentaje_diferencia=float('inf'),
                es_coincidente=False, tipo_diferencia='error'
            )
        
        # Comparación según tipo de campo
        if campo in self.campos_monetarios:
            return self._comparar_monetario(campo, legajo, valor_python, valor_php)
        elif campo in self.campos_enteros:
            return self._comparar_entero(campo, legajo, valor_python, valor_php)
        elif campo in self.campos_booleanos:
            return self._comparar_booleano(campo, legajo, valor_python, valor_php)
        else:
            return self._comparar_generico(campo, legajo, valor_python, valor_php)
    
    def _comparar_monetario(self, campo: str, legajo: int, valor_python: float, valor_php: float) -> ResultadoComparacion:
        """Compara valores monetarios con tolerancia de centavos"""
        diferencia = abs(float(valor_python) - float(valor_php))
        
        # Calcular porcentaje de diferencia
        valor_max = max(abs(float(valor_python)), abs(float(valor_php)))
        porcentaje_diferencia = (diferencia / valor_max * 100) if valor_max > 0 else 0
        
        # Determinar si coincide
        es_exacto = diferencia == 0
        es_tolerancia = diferencia <= self.tolerancia.tolerancia_monetaria
        
        if es_exacto:
            tipo = 'exacto'
        elif es_tolerancia:
            tipo = 'tolerancia'
        else:
            tipo = 'error'
        
        return ResultadoComparacion(
            campo=campo, legajo=legajo,
            valor_python=valor_python, valor_php=valor_php,
            diferencia=diferencia, porcentaje_diferencia=porcentaje_diferencia,
            es_coincidente=es_tolerancia, tipo_diferencia=tipo
        )
    
    def _comparar_entero(self, campo: str, legajo: int, valor_python: int, valor_php: int) -> ResultadoComparacion:
        """Compara valores enteros (debe ser exacto)"""
        diferencia = abs(int(valor_python) - int(valor_php))
        porcentaje_diferencia = (diferencia / max(abs(int(valor_python)), abs(int(valor_php))) * 100) if max(abs(int(valor_python)), abs(int(valor_php))) > 0 else 0
        
        es_coincidente = diferencia <= self.tolerancia.tolerancia_enteros
        tipo = 'exacto' if diferencia == 0 else 'error'
        
        return ResultadoComparacion(
            campo=campo, legajo=legajo,
            valor_python=valor_python, valor_php=valor_php,
            diferencia=float(diferencia), porcentaje_diferencia=porcentaje_diferencia,
            es_coincidente=es_coincidente, tipo_diferencia=tipo
        )
    
    def _comparar_booleano(self, campo: str, legajo: int, valor_python: bool, valor_php: bool) -> ResultadoComparacion:
        """Compara valores booleanos (debe ser exacto)"""
        bool_py = self._normalizar_booleano(valor_python)
        bool_php = self._normalizar_booleano(valor_php)
        
        es_coincidente = bool_py == bool_php
        diferencia = 0.0 if es_coincidente else 1.0
        tipo = 'exacto' if es_coincidente else 'error'
        
        return ResultadoComparacion(
            campo=campo, legajo=legajo,
            valor_python=bool_py, valor_php=bool_php,
            diferencia=diferencia, porcentaje_diferencia=diferencia * 100,
            es_coincidente=es_coincidente, tipo_diferencia=tipo
        )
    
    def _comparar_generico(self, campo: str, legajo: int, valor_python: Any, valor_php: Any) -> ResultadoComparacion:
        """Compara valores genéricos"""
        try:
            # Intentar comparación numérica
            val_py = float(valor_python)
            val_php = float(valor_php)
            diferencia = abs(val_py - val_php)
            porcentaje_diferencia = (diferencia / max(abs(val_py), abs(val_php)) * 100) if max(abs(val_py), abs(val_php)) > 0 else 0
            es_coincidente = diferencia <= self.tolerancia.tolerancia_porcentual
        except (ValueError, TypeError):
            # Comparación como string
            es_coincidente = str(valor_python) == str(valor_php)
            diferencia = 0.0 if es_coincidente else 1.0
            porcentaje_diferencia = diferencia * 100
        
        tipo = 'exacto' if diferencia == 0 else ('tolerancia' if es_coincidente else 'error')
        
        return ResultadoComparacion(
            campo=campo, legajo=legajo,
            valor_python=valor_python, valor_php=valor_php,
            diferencia=diferencia, porcentaje_diferencia=porcentaje_diferencia,
            es_coincidente=es_coincidente, tipo_diferencia=tipo
        )
    
    def _generar_reporte(self, resultados: List[ResultadoComparacion], tiempo_verificacion: float) -> ReporteVerificacion:
        """Genera reporte completo de verificación"""
        
        total_comparaciones = len(resultados)
        coincidencias_exactas = sum(1 for r in resultados if r.tipo_diferencia == 'exacto')
        coincidencias_tolerancia = sum(1 for r in resultados if r.es_coincidente)
        diferencias_criticas = sum(1 for r in resultados if not r.es_coincidente)
        
        porcentaje_coincidencia = (coincidencias_tolerancia / total_comparaciones * 100) if total_comparaciones > 0 else 0
        
        # Generar estadísticas
        resumen_estadistico = self._generar_estadisticas(resultados)
        
        # Generar recomendaciones
        recomendaciones = self._generar_recomendaciones(resultados, porcentaje_coincidencia)
        
        # Obtener conteos únicos
        legajos_unicos = len(set(r.legajo for r in resultados))
        campos_unicos = len(set(r.campo for r in resultados))
        
        return ReporteVerificacion(
            total_legajos=legajos_unicos,
            total_campos=campos_unicos,
            coincidencias_exactas=coincidencias_exactas,
            coincidencias_tolerancia=coincidencias_tolerancia,
            diferencias_criticas=diferencias_criticas,
            porcentaje_coincidencia=porcentaje_coincidencia,
            tiempo_verificacion=tiempo_verificacion,
            detalles_diferencias=[r for r in resultados if not r.es_coincidente],
            resumen_estadistico=resumen_estadistico,
            recomendaciones=recomendaciones
        )
    
    def _generar_estadisticas(self, resultados: List[ResultadoComparacion]) -> Dict[str, Any]:
        """Genera estadísticas detalladas de las comparaciones"""
        if not resultados:
            return {}
        
        diferencias_numericas = [r.diferencia for r in resultados if not np.isinf(r.diferencia)]
        porcentajes_diferencia = [r.porcentaje_diferencia for r in resultados if not np.isinf(r.porcentaje_diferencia)]
        
        # Estadísticas por campo
        campos_con_errores = {}
        for resultado in resultados:
            if not resultado.es_coincidente:
                if resultado.campo not in campos_con_errores:
                    campos_con_errores[resultado.campo] = 0
                campos_con_errores[resultado.campo] += 1
        
        return {
            'diferencia_promedio': np.mean(diferencias_numericas) if diferencias_numericas else 0,
            'diferencia_maxima': np.max(diferencias_numericas) if diferencias_numericas else 0,
            'diferencia_mediana': np.median(diferencias_numericas) if diferencias_numericas else 0,
            'porcentaje_diferencia_promedio': np.mean(porcentajes_diferencia) if porcentajes_diferencia else 0,
            'campos_con_mas_errores': sorted(campos_con_errores.items(), key=lambda x: x[1], reverse=True)[:5],
            'total_comparaciones': len(resultados),
            'comparaciones_exitosas': sum(1 for r in resultados if r.es_coincidente)
        }
    
    def _generar_recomendaciones(self, resultados: List[ResultadoComparacion], porcentaje_coincidencia: float) -> List[str]:
        """Genera recomendaciones basadas en los resultados"""
        recomendaciones = []
        
        if porcentaje_coincidencia >= 99.5:
            recomendaciones.append("✅ Excelente: Sistema Python altamente consistente con PHP legacy")
        elif porcentaje_coincidencia >= 95.0:
            recomendaciones.append("🟡 Bueno: Consistencia aceptable, revisar diferencias menores")
        else:
            recomendaciones.append("🔴 Crítico: Revisar inconsistencias significativas antes de producción")
        
        # Recomendaciones específicas por errores
        errores_criticos = [r for r in resultados if not r.es_coincidente]
        if errores_criticos:
            campos_problematicos = set(r.campo for r in errores_criticos)
            if len(campos_problematicos) <= 3:
                recomendaciones.append(f"🔧 Revisar cálculos en campos: {', '.join(campos_problematicos)}")
            else:
                recomendaciones.append(f"🔧 Revisar cálculos en {len(campos_problematicos)} campos problemáticos")
        
        return recomendaciones
    
    def generar_reporte_html(self, reporte: ReporteVerificacion, archivo_salida: Optional[str] = None) -> str:
        """Genera reporte HTML detallado"""
        if archivo_salida is None:
            archivo_salida = f"reporte_verificacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        html_content = self._construir_html_reporte(reporte)
        
        with open(archivo_salida, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"📄 Reporte HTML generado: {archivo_salida}")
        return archivo_salida
    
    def _construir_html_reporte(self, reporte: ReporteVerificacion) -> str:
        """Construye el contenido HTML del reporte"""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Reporte Verificación SICOSS</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #2E8B57; color: white; padding: 20px; border-radius: 5px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 15px; background: #f0f0f0; border-radius: 5px; }}
                .error {{ background: #ffebee; }}
                .success {{ background: #e8f5e8; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🔍 Reporte Verificación SICOSS Python vs PHP</h1>
                <p>Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="metric success">
                <strong>Porcentaje Coincidencia:</strong><br>
                {reporte.porcentaje_coincidencia:.2f}%
            </div>
            
            <div class="metric">
                <strong>Total Legajos:</strong><br>
                {reporte.total_legajos}
            </div>
            
            <div class="metric">
                <strong>Total Campos:</strong><br>
                {reporte.total_campos}
            </div>
            
            <div class="metric {'error' if reporte.diferencias_criticas > 0 else 'success'}">
                <strong>Diferencias Críticas:</strong><br>
                {reporte.diferencias_criticas}
            </div>
            
            <h2>📊 Estadísticas</h2>
            <ul>
                <li>Diferencia promedio: {reporte.resumen_estadistico.get('diferencia_promedio', 0):.4f}</li>
                <li>Diferencia máxima: {reporte.resumen_estadistico.get('diferencia_maxima', 0):.4f}</li>
                <li>Tiempo verificación: {reporte.tiempo_verificacion:.2f}s</li>
            </ul>
            
            <h2>🎯 Recomendaciones</h2>
            <ul>
                {''.join(f'<li>{rec}</li>' for rec in reporte.recomendaciones)}
            </ul>
            
            {'<h2>🔴 Diferencias Críticas</h2>' + self._tabla_diferencias_html(reporte.detalles_diferencias) if reporte.detalles_diferencias else ''}
        </body>
        </html>
        """
    
    def _tabla_diferencias_html(self, diferencias: List[ResultadoComparacion]) -> str:
        """Genera tabla HTML de diferencias"""
        if not diferencias:
            return "<p>No hay diferencias críticas.</p>"
        
        filas = []
        for diff in diferencias[:50]:  # Limitar a 50 para evitar reportes muy largos
            filas.append(f"""
                <tr>
                    <td>{diff.legajo}</td>
                    <td>{diff.campo}</td>
                    <td>{diff.valor_python}</td>
                    <td>{diff.valor_php}</td>
                    <td>{diff.diferencia:.4f}</td>
                    <td>{diff.porcentaje_diferencia:.2f}%</td>
                </tr>
            """)
        
        return f"""
        <table>
            <tr>
                <th>Legajo</th>
                <th>Campo</th>
                <th>Valor Python</th>
                <th>Valor PHP</th>
                <th>Diferencia</th>
                <th>% Diferencia</th>
            </tr>
            {''.join(filas)}
        </table>
        {'<p><em>Mostrando primeras 50 diferencias...</em></p>' if len(diferencias) > 50 else ''}
        """ 