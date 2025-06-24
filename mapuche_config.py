import psycopg2
from typing import Any, Optional, Dict
from datetime import datetime
import logging

class MapucheConfig:
    """
    Clase de configuración para el sistema Mapuche que replica la funcionalidad 
    de MapucheConfig.php en Python
    """
    
    def __init__(self, parametros_conexion: Dict[str, str]):
        """
        Inicializa la configuración con parámetros de conexión a la base de datos Mapuche
        
        Args:
            connection_params: Diccionario con host, database, user, password, port
            :type parametros_conexion: Dict[str, str]
        """
        self.connection_params = parametros_conexion
        self.logger = logging.getLogger(__name__)
    
    def get_parametro_rrhh(self, section: str, parameter: str, default: Any = None) -> Optional[str]:
        """
        Obtiene el valor de un parámetro de RRHH desde la tabla rrhhini
        
        Args:
            section: Sección principal del parámetro
            parameter: Nombre del parámetro
            default: Valor por defecto si el parámetro no se encuentra
            
        Returns:
            Valor del parámetro o valor por defecto
        """
        try:
            query = """
            SELECT dato_parametro 
            FROM mapuche.rrhhini 
            WHERE nombre_seccion = %s AND nombre_parametro = %s
            """
            
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (section, parameter))
                    result = cursor.fetchone()
                    return result[0] if result and result[0] is not None else default
                    
        except Exception as e:
            self.logger.error(f"Error obteniendo parámetro RRHH {section}.{parameter}: {str(e)}")
            return default

    
    # Métodos de fechas del período fiscal
    def get_fecha_inicio_periodo_corriente(self) -> Optional[str]:
        """
        Obtiene la fecha de inicio del período fiscal corriente
        
        Returns:
            Fecha en formato Y-m-d
        """
        try:
            query = "SELECT mapuche.map_get_fecha_inicio_periodo() as fecha_inicio"
            
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    return result[0] if result else None
                    
        except Exception as e:
            self.logger.error(f"Error obteniendo fecha inicio período: {str(e)}")
            return None
    
    def get_fecha_fin_periodo_corriente(self) -> Optional[str]:
        """
        Obtiene la fecha de fin del período fiscal corriente
        
        Returns:
            Fecha en formato Y-m-d
        """
        try:
            query = "SELECT mapuche.map_get_fecha_fin_periodo() as fecha_fin"
            
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    return result[0] if result else None
                    
        except Exception as e:
            self.logger.error(f"Error obteniendo fecha fin período: {str(e)}")
            return None
    
    def get_periodo_fiscal_from_database(self) -> Dict[str, str]:
        """
        Obtiene el período fiscal actual desde la tabla dh99
        
        Returns:
            Diccionario con 'year' y 'month'
        """
        try:
            query = "SELECT per_anoct, per_mesct FROM mapuche.dh99 LIMIT 1"
            
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    if result:
                        return {
                            'year': str(result[0]),
                            'month': f"{result[1]:02d}"
                        }
                    return {
                        'year': str(datetime.now().year),
                        'month': f"{datetime.now().month:02d}"
                    }
                    
        except Exception as e:
            self.logger.error(f"Error obteniendo período fiscal desde BD: {str(e)}")
            return {
                'year': str(datetime.now().year),
                'month': f"{datetime.now().month:02d}"
            }
    
    def get_anio_fiscal(self) -> str:
        """Obtiene el año fiscal actual"""
        periodo = self.get_periodo_fiscal_from_database()
        return periodo['year']
    
    def get_mes_fiscal(self) -> str:
        """Obtiene el mes fiscal actual"""
        periodo = self.get_periodo_fiscal_from_database()
        return periodo['month']
    
    def get_periodo_fiscal(self) -> str:
        """
        Obtiene el período fiscal en formato YYYYMM
        
        Returns:
            Período fiscal como string YYYYMM
        """
        periodo = self.get_periodo_fiscal_from_database()
        return periodo['year'] + periodo['month']
    
    def get_periodo_corriente(self) -> Dict[str, str]:
        """
        Obtiene el período corriente completo
        
        Returns:
            Diccionario con 'year' y 'month'
        """
        return self.get_periodo_fiscal_from_database()
    
    # Métodos para obtener configuraciones específicas
    def get_porcentaje_aporte_diferencial_jubilacion(self) -> float:
        """Obtiene el porcentaje de aporte diferencial de jubilación"""
        value = self.get_parametro_rrhh('Porcentaje', 'PorcAporteDiferencialJubilacion', '0')
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def get_sicoss_informar_becarios(self) -> bool:
        """Obtiene si se deben informar becarios en SICOSS"""
        value = self.get_parametro_rrhh('Sicoss', 'InformarBecario', '0')
        return value == '1' or value == 'true' or value == 'True'
    
    def get_sicoss_art_tope(self) -> str:
        """Obtiene el valor de ART con tope"""
        return self.get_parametro_rrhh('Sicoss', 'ARTconTope', '1')
    
    def get_sicoss_conceptos_no_remunerativos_en_art(self) -> str:
        """Obtiene los conceptos no remunerativos incluidos en el ART"""
        return self.get_parametro_rrhh('Sicoss', 'ConceptosNoRemuEnART', '0')
    
    def get_sicoss_categorias_aportes_diferenciales(self) -> str:
        """Obtiene las categorías de aportes diferenciales"""
        return self.get_parametro_rrhh('Sicoss', 'CategoriasAportesDiferenciales', '0')
    
    def get_sicoss_horas_extras_novedades(self) -> int:
        """Obtiene el número de horas extras para novedades"""
        value = self.get_parametro_rrhh('Sicoss', 'HorasExtrasNovedades', '0')
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0
    
    def get_parametros_ajustes_imp_contable(self) -> str:
        """Obtiene los parámetros de ajustes de imputaciones contables"""
        return self.get_parametro_rrhh('Presupuesto', 'GestionAjustesImputacionesPresupuestarias', 'Deshabilitada')
    
    # Métodos para licencias
    def get_var_licencias_10_dias(self) -> str:
        """Obtiene las variantes de licencias de 10 días"""
        return self.get_parametro_rrhh('Licencias', 'VariantesILTPrimerTramo', '')
    
    def get_var_licencias_11_dias_siguientes(self) -> str:
        """Obtiene las variantes de licencias de 11 días siguientes"""
        return self.get_parametro_rrhh('Licencias', 'VariantesILTSegundoTramo', '')
    
    def get_var_licencias_maternidad_down(self) -> str:
        """Obtiene las variantes de licencias de maternidad down"""
        return self.get_parametro_rrhh('Licencias', 'VariantesMaternidadDown', '')
    
    def get_var_licencia_excedencia(self) -> str:
        """Obtiene las variantes de licencias de excedencia"""
        return self.get_parametro_rrhh('Licencias', 'VariantesExcedencia', '')
    
    def get_var_licencia_vacaciones(self) -> str:
        """Obtiene las variantes de licencias de vacaciones"""
        return self.get_parametro_rrhh('Licencias', 'VariantesVacaciones', '')
    
    def get_var_licencia_protec_integral(self) -> str:
        """Obtiene las variantes de licencias de protección integral"""
        return self.get_parametro_rrhh('Licencias', 'VariantesProtecIntegral', '')
    
    def get_categorias_diferencial(self) -> str:
        """Obtiene las categorías diferenciales"""
        return self.get_parametro_rrhh('Sicoss', 'CategoriaDiferencial', '')
    
    # Métodos para configuración de obra social
    def get_defaults_obra_social(self) -> str:
        """Obtiene el código de obra social por defecto"""
        return self.get_parametro_rrhh('Defaults', 'ObraSocial', '')
    
    def get_conceptos_obra_social_aporte_adicional(self) -> str:
        """Obtiene conceptos de obra social aporte adicional"""
        return self.get_parametro_rrhh('Conceptos', 'ObraSocialAporteAdicional', '')
    
    def get_conceptos_obra_social_aporte(self) -> str:
        """Obtiene conceptos de obra social aporte"""
        return self.get_parametro_rrhh('Conceptos', 'ObraSocialAporte', '')
    
    def get_conceptos_obra_social_retro(self) -> str:
        """Obtiene conceptos de obra social retro"""
        return self.get_parametro_rrhh('Conceptos', 'ObraSocialRetro', '')
    
    def get_conceptos_obra_social(self) -> str:
        """Obtiene conceptos de obra social"""
        return self.get_parametro_rrhh('Conceptos', 'ObraSocial', '')
    
    def get_conceptos_obra_social_fliar_adherente(self) -> str:
        """Obtiene conceptos de obra social familiar adherente"""
        return self.get_parametro_rrhh('Conceptos', 'ObraSocialFliarAdherente', '')
    
    # Métodos para topes
    def get_topes_jubilacion_voluntario(self) -> str:
        """Obtiene topes de jubilación voluntarios"""
        return self.get_parametro_rrhh('Conceptos', 'JubilacionVoluntario', '')
    
    def get_topes_jubilatorio_patronal(self) -> str:
        """Obtiene topes jubilatorio patronal"""
        return self.get_parametro_rrhh('Topes', 'TopeJubilatorioPatronal', '')
    
    def get_topes_jubilatorio_personal(self) -> str:
        """Obtiene topes jubilatorio personal"""
        return self.get_parametro_rrhh('Topes', 'TopeJubilatorioPersonal', '')
    
    def get_topes_otros_aportes_personales(self) -> str:
        """Obtiene topes de otros aportes personales"""
        return self.get_parametro_rrhh('Topes', 'TopeOtrosAportesPersonales', '')
    
    # Métodos para datos de la universidad
    def get_datos_universidad_cuit(self) -> str:
        """Obtiene el CUIT de la universidad"""
        return self.get_parametro_rrhh('Datos Universidad', 'CUIT', '')
    
    def get_datos_universidad_direccion(self) -> str:
        """Obtiene la dirección de la universidad"""
        return self.get_parametro_rrhh('Datos Universidad', 'Direccion', '')
    
    def get_datos_codc_reparto(self) -> str:
        """Obtiene el código de régimen de reparto"""
        return self.get_parametro_rrhh('Datos Universidad', 'Cod.Régimen de Reparto', '')
    
    def get_datos_universidad_ciudad(self) -> str:
        """Obtiene la ciudad de la universidad"""
        return self.get_parametro_rrhh('Datos Universidad', 'Ciudad', '')
    
    def get_datos_universidad_sigla(self) -> str:
        """Obtiene la sigla de la universidad"""
        return self.get_parametro_rrhh('Datos Universidad', 'Sigla', '')
    
    def get_datos_universidad_tipo_empresa(self) -> str:
        """Obtiene el tipo de empresa de la universidad"""
        return self.get_parametro_rrhh('Datos Universidad', 'TipoEmpresa', '')
    
    def get_datos_universidad_trabajador_convencionado(self) -> str:
        """Obtiene si el trabajador es convencionado"""
        return self.get_parametro_rrhh('Datos Universidad', 'TrabajadorConvencionado', '')
    
    # Métodos para conceptos específicos
    def get_conceptos_informar_adherentes_sicoss(self) -> str:
        """Obtiene si informar adherentes desde dh09"""
        return self.get_parametro_rrhh('Conceptos', 'AdherenteSicossDesdeH09', '0')
    
    def get_conceptos_acumular_asig_familiar(self) -> str:
        """Obtiene si acumular asignación familiar"""
        return self.get_parametro_rrhh('Conceptos', 'AcumularAsigFamiliar', '1')


# Función para crear una instancia de configuración
def create_mapuche_config(parametros_conexion: Dict[str, str]) -> MapucheConfig:
    """
    Factory function para crear una instancia de MapucheConfig
    
    Args:
        connection_params: Diccionario con parámetros de conexión
        
    Returns:
        Instancia configurada de MapucheConfig
        :type parametros_conexion: Dict[str, str]
    """
    return MapucheConfig(parametros_conexion)


# Ejemplo de uso
if __name__ == "__main__":
    # Configuración de ejemplo
    connection_params = {
        'host': 'localhost',
        'database': 'liqui',
        'user': 'postgres',
        'password': 'postgres',
        'port': '5432'
    }
    
    # Crear instancia de configuración
    config = create_mapuche_config(connection_params)
    
    # Ejemplos de uso
    print("=== Período Fiscal ===")
    print(f"Año fiscal: {config.get_anio_fiscal()}")
    print(f"Mes fiscal: {config.get_mes_fiscal()}")
    print(f"Período fiscal: {config.get_periodo_fiscal()}")
    print(f"Fecha inicio: {config.get_fecha_inicio_periodo_corriente()}")
    print(f"Fecha fin: {config.get_fecha_fin_periodo_corriente()}")
    
    print("\n=== Licencias ===")
    print(f"Vacaciones: {config.get_var_licencia_vacaciones()}")
    print(f"Protección integral: {config.get_var_licencia_protec_integral()}")
    print(f"Excedencia: {config.get_var_licencia_excedencia()}")
    
    print("\n=== SICOSS ===")
    print(f"Informar becarios: {config.get_sicoss_informar_becarios()}")
    print(f"ART con tope: {config.get_sicoss_art_tope()}")
    print(f"Horas extras novedades: {config.get_sicoss_horas_extras_novedades()}")
    
    print("\n=== Universidad ===")
    print(f"CUIT: {config.get_datos_universidad_cuit()}")
    print(f"Sigla: {config.get_datos_universidad_sigla()}")
    print(f"Tipo empresa: {config.get_datos_universidad_tipo_empresa()}")