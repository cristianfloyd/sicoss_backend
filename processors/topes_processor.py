from .base_processor import BaseProcessor
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class TopesProcessor(BaseProcessor):
    """Procesador especializado para aplicación de topes"""
    
    def process(self, df_legajos: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Aplica topes jubilatorios con lógica completa del PHP legacy"""
        logger.info("Aplicando topes jubilatorios con lógica completa...")
        
        df = df_legajos.copy()
        
        if not self.config.trunca_tope:
            logger.info("Topes desactivados en configuración")
            return df
        
        # Pipeline completo de topes (siguiendo orden PHP legacy)
        df = self._aplicar_topes_patronales(df)
        df = self._aplicar_topes_personales_complejos(df)
        df = self._aplicar_categorias_diferenciales(df)
        df = self._aplicar_topes_otra_actividad(df)
        df = self._aplicar_topes_otros_aportes(df)
        df = self._aplicar_casos_especiales(df)
        df = self._calcular_campos_finales(df)
        df = self._recalcular_importe_bruto(df)
        
        self._log_process_info("TopesProcessor", len(df_legajos), len(df))
        return df
    
    def _aplicar_topes_patronales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica topes patronales (SAC + Imponible sin SAC)"""
        # Asegurar que las columnas requeridas existan
        for campo in ['ImporteSAC', 'ImporteImponibleSinSAC', 'ImporteImponiblePatronal', 
                      'DiferenciaSACImponibleConTope', 'DiferenciaImponibleConTope', 'ImporteSACPatronal']:
            if campo not in df.columns:
                df[campo] = 0.0
        
        # 1. Tope SAC patronal
        tope_sac = self.config.tope_sac_jubilatorio_patr
        mask_excede_sac = df['ImporteSAC'] > tope_sac
        
        if mask_excede_sac.any():
            logger.info(f"Aplicando tope SAC patronal: {mask_excede_sac.sum()} legajos afectados")
            
            df.loc[mask_excede_sac, 'DiferenciaSACImponibleConTope'] = (
                df.loc[mask_excede_sac, 'ImporteSAC'] - tope_sac
            )
            df.loc[mask_excede_sac, 'ImporteImponiblePatronal'] -= (
                df.loc[mask_excede_sac, 'DiferenciaSACImponibleConTope']
            )
            df.loc[mask_excede_sac, 'ImporteSACPatronal'] = tope_sac
        
        # 2. Tope imponible sin SAC
        tope_imponible = self.config.tope_jubilatorio_patronal
        mask_excede_imponible = df['ImporteImponibleSinSAC'] > tope_imponible
        
        if mask_excede_imponible.any():
            logger.info(f"Aplicando tope imponible: {mask_excede_imponible.sum()} legajos afectados")
            
            df.loc[mask_excede_imponible, 'DiferenciaImponibleConTope'] = (
                df.loc[mask_excede_imponible, 'ImporteImponibleSinSAC'] - tope_imponible
            )
            df.loc[mask_excede_imponible, 'ImporteImponiblePatronal'] -= (
                df.loc[mask_excede_imponible, 'DiferenciaImponibleConTope']
            )
        
        return df
    
    def _aplicar_topes_personales_complejos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica topes personales complejos (PHP líneas 1135-1165)"""
        if not self.config.trunca_tope:
            return df
        
        # Inicializar campos necesarios
        for campo in ['ImporteSACNoDocente', 'IMPORTE_IMPON', 'ImporteImponible_6', 'ImporteNoRemun']:
            if campo not in df.columns:
                df[campo] = 0.0
        
        # Configurar ImporteSACNoDocente si no existe
        if df['ImporteSACNoDocente'].sum() == 0:
            df['ImporteSACNoDocente'] = df.get('ImporteSAC', 0)
        
        # Configurar IMPORTE_IMPON inicial si no existe
        if df['IMPORTE_IMPON'].sum() == 0:
            df['IMPORTE_IMPON'] = df.get('ImporteImponiblePatronal', 0)
        
        tope_jubil_personal_base = self.config.tope_jubilatorio_personal
        tope_sac_personal = self.config.tope_sac_jubilatorio_pers
        
        # Calcular tope personal dinámico basado en si tiene SAC
        df['tope_jubil_personal_dinamico'] = tope_jubil_personal_base
        mask_tiene_sac = df['ImporteSAC'] > 0
        df.loc[mask_tiene_sac, 'tope_jubil_personal_dinamico'] = tope_jubil_personal_base + tope_sac_personal
        
        # Caso 1: ImporteSACNoDocente excede tope personal
        mask_excede_tope_personal = df['ImporteSACNoDocente'] > df['tope_jubil_personal_dinamico']
        
        if mask_excede_tope_personal.any():
            logger.info(f"Aplicando tope personal SAC: {mask_excede_tope_personal.sum()} legajos")
            
            df.loc[mask_excede_tope_personal, 'DiferenciaSACImponibleConTope'] = (
                df.loc[mask_excede_tope_personal, 'ImporteSACNoDocente'] - tope_sac_personal
            )
            df.loc[mask_excede_tope_personal, 'IMPORTE_IMPON'] -= (
                df.loc[mask_excede_tope_personal, 'DiferenciaSACImponibleConTope']
            )
            df.loc[mask_excede_tope_personal, 'ImporteSACNoDocente'] = tope_sac_personal
        
        # Caso 2: Lógica compleja cuando NO excede tope personal (PHP líneas 1147-1160)
        mask_no_excede = ~mask_excede_tope_personal
        
        if mask_no_excede.any():
            # Calcular bruto_nodo_sin_sac
            bruto_nodo_sin_sac = (
                df.loc[mask_no_excede, 'IMPORTE_BRUTO'] - 
                df.loc[mask_no_excede, 'ImporteImponible_6'] - 
                df.loc[mask_no_excede, 'ImporteSACNoDocente']
            )
            
            sac = df.loc[mask_no_excede, 'ImporteSACNoDocente']
            
            # Aplicar fórmula compleja del PHP
            tope_sueldo = (bruto_nodo_sin_sac - df.loc[mask_no_excede, 'ImporteNoRemun']).clip(upper=tope_jubil_personal_base)
            tope_sac = sac.clip(upper=tope_sac_personal)
            
            df.loc[mask_no_excede, 'IMPORTE_IMPON'] = tope_sueldo + tope_sac
        
        # Limpiar columna temporal
        df.drop('tope_jubil_personal_dinamico', axis=1, inplace=True)
        
        return df
    
    def _aplicar_categorias_diferenciales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica categorías diferenciales (PHP líneas 1167-1170)"""
        try:
            # Obtener categorías diferenciales de configuración
            categorias_dif = self._obtener_categorias_diferenciales()
            
            if not categorias_dif:
                logger.info("No hay categorías diferenciales configuradas")
                return df
            
            # Verificar qué legajos tienen categorías diferenciales
            df['es_categoria_diferencial'] = df['nro_legaj'].apply(
                lambda legajo: self._es_categoria_diferencial(legajo, categorias_dif)
            )
            
            mask_diferencial = df['es_categoria_diferencial']
            
            if mask_diferencial.any():
                logger.info(f"Aplicando categorías diferenciales: {mask_diferencial.sum()} legajos")
                logger.info(f"Legajos afectados: {df.loc[mask_diferencial, 'nro_legaj'].tolist()}")
                
                # Según PHP: si es categoría diferencial → IMPORTE_IMPON = 0
                df.loc[mask_diferencial, 'IMPORTE_IMPON'] = 0
            
            # Limpiar columna temporal
            df.drop('es_categoria_diferencial', axis=1, inplace=True)
            
        except Exception as e:
            logger.warning(f"Error aplicando categorías diferenciales: {e}")
        
        return df
    
    def _obtener_categorias_diferenciales(self) -> list:
        """Obtiene las categorías diferenciales desde la configuración"""
        try:
            # Intentar obtener desde MapucheConfig
            from mapuche_config import create_mapuche_config, ConnectionParams
            import configparser
            
            config_ini = configparser.ConfigParser()
            config_ini.read('database.ini')
            db_params = config_ini['postgresql']
            
            connection_params: ConnectionParams = {
                'host': db_params.get('host', 'localhost'),
                'database': db_params.get('database', ''),
                'user': db_params.get('user', ''),
                'password': db_params.get('password', ''),
                'port': db_params.get('port', '5432')
            }
            
            config = create_mapuche_config(connection_params)
            categorias_str = config.get_categorias_diferencial()
            
            if categorias_str:
                return [cat.strip() for cat in categorias_str.split(',') if cat.strip()]
            
        except Exception as e:
            logger.warning(f"No se pudieron obtener categorías diferenciales: {e}")
        
        return []
    
    def _es_categoria_diferencial(self, nro_legaj: int, categorias_dif: list) -> bool:
        """Verifica si un legajo tiene categoría diferencial"""
        try:
            from database.database_connection import DatabaseConnection
            
            # Crear conexión temporal para la consulta
            db = DatabaseConnection()
            
            # Consultar la categoría del legajo
            categorias_in_clause = "','".join(categorias_dif)
            query = f"""
            SELECT COUNT(*) as count
            FROM mapuche.dh01 dh01
            INNER JOIN mapuche.dh03 dh03 ON dh01.nro_legaj = dh03.nro_legaj
            WHERE dh01.nro_legaj = {nro_legaj}
              AND dh03.codc_categ IN ('{categorias_in_clause}')
              AND mapuche.map_es_cargo_activo(dh03.nro_cargo)
            """
            
            resultado = db.execute_query(query)
            db.close()
            
            if not resultado.empty:
                return resultado.iloc[0]['count'] > 0
                
        except Exception as e:
            logger.warning(f"Error verificando categoría diferencial para legajo {nro_legaj}: {e}")
        
        return False
    
    def _aplicar_topes_otra_actividad(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica topes de otra actividad (PHP líneas 1192-1220)"""
        # Inicializar campos de otra actividad si no existen
        for campo in ['ImporteBrutoOtraActividad', 'ImporteSACOtraActividad']:
            if campo not in df.columns:
                df[campo] = 0.0
        
        # TODO: En implementación real, consultar sicoss::otra_actividad($legajo)
        # Por ahora usamos los valores ya existentes en el DataFrame
        
        mask_tiene_otra_actividad = (
            (df['ImporteBrutoOtraActividad'] != 0) | 
            (df['ImporteSACOtraActividad'] != 0)
        )
        
        if mask_tiene_otra_actividad.any():
            logger.info(f"Procesando otra actividad: {mask_tiene_otra_actividad.sum()} legajos")
            
            tope_sac_pers = self.config.tope_sac_jubilatorio_pers
            tope_jubil_patronal = self.config.tope_jubilatorio_patronal
            tope_total = tope_sac_pers + tope_jubil_patronal
            
            # Suma de otra actividad
            suma_otra_actividad = (
                df.loc[mask_tiene_otra_actividad, 'ImporteBrutoOtraActividad'] +
                df.loc[mask_tiene_otra_actividad, 'ImporteSACOtraActividad']
            )
            
            # Caso 1: Otra actividad excede topes totales → IMPORTE_IMPON = 0
            mask_excede_total = suma_otra_actividad >= tope_total
            indices_excede = df.loc[mask_tiene_otra_actividad].loc[mask_excede_total].index
            
            if len(indices_excede) > 0:
                logger.info(f"Otra actividad excede total: {len(indices_excede)} legajos")
                df.loc[indices_excede, 'IMPORTE_IMPON'] = 0.0
            
            # Caso 2: Calcular topes proporcionales
            mask_no_excede = ~mask_excede_total
            indices_no_excede = df.loc[mask_tiene_otra_actividad].loc[mask_no_excede].index
            
            if len(indices_no_excede) > 0:
                # Tope disponible para sueldo
                tope_disponible_sueldo = (
                    tope_jubil_patronal - df.loc[indices_no_excede, 'ImporteBrutoOtraActividad']
                ).clip(lower=0)
                
                # Tope disponible para SAC
                tope_disponible_sac = (
                    tope_sac_pers - df.loc[indices_no_excede, 'ImporteSACOtraActividad']
                ).clip(lower=0)
                
                # Aplicar topes
                importe_sueldo_limitado = df.loc[indices_no_excede, 'ImporteImponibleSinSAC'].clip(upper=tope_disponible_sueldo)
                importe_sac_limitado = df.loc[indices_no_excede, 'ImporteSACPatronal'].clip(upper=tope_disponible_sac)
                
                df.loc[indices_no_excede, 'IMPORTE_IMPON'] = importe_sueldo_limitado + importe_sac_limitado
        
        return df
    
    def _aplicar_topes_otros_aportes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica topes de otros aportes (PHP líneas 1226-1240)"""
        # Inicializar campos necesarios
        for campo in ['ImporteSACOtroAporte', 'ImporteImponible_4', 'DifSACImponibleConOtroTope', 'DifImponibleConOtroTope']:
            if campo not in df.columns:
                df[campo] = 0.0
        
        if df['ImporteSACOtroAporte'].sum() == 0:
            df['ImporteSACOtroAporte'] = df.get('ImporteSAC', 0)
        
        if df['ImporteImponible_4'].sum() == 0:
            df['ImporteImponible_4'] = df.get('IMPORTE_IMPON', 0)
        
        # Tope SAC otros aportes
        tope_sac_otro_aporte = self.config.tope_sac_jubilatorio_otro_ap
        mask_excede_sac_otro = df['ImporteSACOtroAporte'] > tope_sac_otro_aporte
        
        if mask_excede_sac_otro.any():
            logger.info(f"Aplicando tope SAC otros aportes: {mask_excede_sac_otro.sum()} legajos")
            
            df.loc[mask_excede_sac_otro, 'DifSACImponibleConOtroTope'] = (
                df.loc[mask_excede_sac_otro, 'ImporteSACOtroAporte'] - tope_sac_otro_aporte
            )
            df.loc[mask_excede_sac_otro, 'ImporteImponible_4'] -= (
                df.loc[mask_excede_sac_otro, 'DifSACImponibleConOtroTope']
            )
            df.loc[mask_excede_sac_otro, 'ImporteSACOtroAporte'] = tope_sac_otro_aporte
        
        # Calcular OtroImporteImponibleSinSAC
        df['OtroImporteImponibleSinSAC'] = df['ImporteImponible_4'] - df['ImporteSACOtroAporte']
        
        # Tope otros aportes sin SAC
        tope_otros_aportes = self.config.tope_otros_aportes_personales
        mask_excede_otros = df['OtroImporteImponibleSinSAC'] > tope_otros_aportes
        
        if mask_excede_otros.any():
            logger.info(f"Aplicando tope otros aportes: {mask_excede_otros.sum()} legajos")
            
            df.loc[mask_excede_otros, 'DifImponibleConOtroTope'] = (
                df.loc[mask_excede_otros, 'OtroImporteImponibleSinSAC'] - tope_otros_aportes
            )
            df.loc[mask_excede_otros, 'ImporteImponible_4'] -= (
                df.loc[mask_excede_otros, 'DifImponibleConOtroTope']
            )
        
        return df
    
    def _aplicar_casos_especiales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica casos especiales (PHP líneas 1243-1246)"""
        # Inicializar campos si no existen
        for campo in ['ImporteImponible_6', 'TipoDeOperacion']:
            if campo not in df.columns:
                df[campo] = 0.0 if campo.startswith('Importe') else 1
        
        # Caso especial: ImporteImponible_6 != 0 && TipoDeOperacion == 1 → IMPORTE_IMPON = 0
        mask_caso_especial = (df['ImporteImponible_6'] != 0) & (df['TipoDeOperacion'] == 1)
        
        if mask_caso_especial.any():
            logger.info(f"Aplicando caso especial ImporteImponible_6: {mask_caso_especial.sum()} legajos")
            df.loc[mask_caso_especial, 'IMPORTE_IMPON'] = 0
        
        return df
    
    def _calcular_campos_finales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula campos finales (PHP líneas 1248-1268)"""
        # Recalcular ImporteImponibleSinSAC después de todos los ajustes
        df['ImporteImponibleSinSAC'] = df.get('IMPORTE_IMPON', 0) - df.get('ImporteSACNoDocente', 0)
        
        # Aplicar tope final a ImporteImponibleSinSAC si es necesario
        tope_jubil_personal = self.config.tope_jubilatorio_personal
        tope_sac_pers = self.config.tope_sac_jubilatorio_pers
        
        # Calcular tope dinámico final
        df['tope_final'] = tope_jubil_personal
        mask_tiene_sac = df.get('ImporteSAC', 0) > 0
        df.loc[mask_tiene_sac, 'tope_final'] = tope_jubil_personal + tope_sac_pers
        df.loc[~mask_tiene_sac, 'tope_final'] = tope_jubil_personal
        
        # Aplicar tope final
        mask_excede_final = df['ImporteImponibleSinSAC'] > df['tope_final']
        
        if mask_excede_final.any():
            logger.info(f"Aplicando tope final ImporteImponibleSinSAC: {mask_excede_final.sum()} legajos")
            
            df.loc[mask_excede_final, 'DiferenciaImponibleConTope'] = (
                df.loc[mask_excede_final, 'ImporteImponibleSinSAC'] - tope_jubil_personal
            )
            df.loc[mask_excede_final, 'IMPORTE_IMPON'] -= (
                df.loc[mask_excede_final, 'DiferenciaImponibleConTope']
            )
        
        # Limpiar columna temporal
        df.drop('tope_final', axis=1, inplace=True)
        
        return df
    
    def _recalcular_importe_bruto(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recalcula importe bruto después de aplicar topes"""
        df['IMPORTE_BRUTO'] = (
            df['ImporteImponiblePatronal'] + df.get('ImporteNoRemun', 0)
        )
        return df 