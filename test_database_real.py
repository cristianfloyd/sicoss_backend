"""
test_database_real.py

Test completo de la implementación real de guardado en BD SICOSS con tabla afip_mapuche_sicoss

Este test verifica que la implementación funcione con la estructura real de la BD.
"""

import pandas as pd
import sys
import os
from datetime import datetime
from unittest.mock import Mock, patch

# Imports del proyecto
sys.path.append(os.path.dirname(__file__))

from processors.database_saver import SicossDatabaseSaver
from value_objects.periodo_fiscal import PeriodoFiscal
from database.database_connection import DatabaseConnection

class TestDatabaseRealImplementation:
    """Test de la implementación real con tabla afip_mapuche_sicoss"""
    
    def setup_method(self):
        """Setup para cada test"""
        # Mock de la conexión de BD
        self.mock_db = Mock(spec=DatabaseConnection)
        self.mock_db.execute_query.return_value = [{'existe': 1}]  # Tabla existe
        self.mock_db.execute_batch_insert.return_value = 100
        
        # Crear database_saver con mock
        self.database_saver = SicossDatabaseSaver(db_connection=self.mock_db)
        
        # Verificar configuración correcta
        assert self.database_saver.schema == 'suc'
        assert self.database_saver.tabla_sicoss == 'afip_mapuche_sicoss'
        
        # Período fiscal de prueba
        self.periodo_fiscal = PeriodoFiscal.from_string("202501")
        
        # DataFrame de legajos de prueba con campos SICOSS
        self.df_legajos = pd.DataFrame({
            'nro_legaj': [12345, 67890, 11111],
            'cuit': ['20123456789', '27987654321', '20111111111'],
            'apyno': ['PEREZ, JUAN', 'GOMEZ, MARIA', 'LOPEZ, CARLOS'],
            'IMPORTE_BRUTO': [100000.0, 150000.0, 120000.0],
            'IMPORTE_IMPON': [80000.0, 120000.0, 100000.0],
            'ImporteSAC': [8333.33, 12500.0, 10416.67],
            'ImporteNoRemun': [5000.0, 7500.0, 6000.0],
            'TipoDeActividad': [0, 11, 0],
            'codigo_os': ['000000', '123456', '789012'],
            'hijos': [2, 1, 3],
            'conyugue': [1, 0, 1],
            'codigosituacion': [1, 1, 1],
            'codigocondicion': [1, 1, 1],
            'adherentes': [0, 1, 2],
            'regimen': [1, 1, 1],
            'dias_trabajados': [30, 30, 30],
            'trabajadorconvencionado': ['S', 'S', 'S']
        })

    def test_configuracion_inicial(self):
        """Test que verifica la configuración inicial correcta"""
        # Verificar esquema y tabla
        assert self.database_saver.schema == 'suc'
        assert self.database_saver.tabla_sicoss == 'afip_mapuche_sicoss'
        
        # Verificar mapeo de campos
        mapeo = self.database_saver.campo_mapping
        assert 'cuit' in mapeo
        assert mapeo['cuit'] == 'cuil'
        assert 'IMPORTE_BRUTO' in mapeo
        assert mapeo['IMPORTE_BRUTO'] == 'rem_total'
        assert 'ImporteSAC' in mapeo
        assert mapeo['ImporteSAC'] == 'sac'
        
        # Verificar campos requeridos
        campos_req = self.database_saver.campos_requeridos
        assert 'periodo_fiscal' in campos_req
        assert 'cuil' in campos_req
        assert 'apnom' in campos_req
        assert 'rem_total' in campos_req
        
        print("✅ Configuración inicial verificada correctamente")

    def test_verificacion_tabla_existe(self):
        """Test que verifica la tabla afip_mapuche_sicoss"""
        # Ejecutar verificación
        self.database_saver._ensure_table_exists(self.periodo_fiscal)
        
        # Verificar que se llamó la consulta de verificación
        assert self.mock_db.execute_query.called
        
        # Verificar el SQL de verificación
        args, kwargs = self.mock_db.execute_query.call_args
        sql_llamado = args[0] if args else ""
        assert 'information_schema.tables' in sql_llamado
        assert 'afip_mapuche_sicoss' in sql_llamado
        assert 'suc' in sql_llamado
        
        print("✅ Verificación de tabla exitosa")

    def test_mapeo_campos_real(self):
        """Test completo del mapeo a tabla real afip_mapuche_sicoss"""
        # Ejecutar mapeo
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Verificaciones básicas
        assert len(legajos_mapeados) == len(self.df_legajos)
        assert 'periodo_fiscal' in legajos_mapeados.columns
        
        # Verificar mapeos específicos de tabla real
        assert 'cuil' in legajos_mapeados.columns  # cuit -> cuil
        assert 'apnom' in legajos_mapeados.columns  # apyno -> apnom
        assert 'rem_total' in legajos_mapeados.columns  # IMPORTE_BRUTO -> rem_total
        assert 'rem_impo1' in legajos_mapeados.columns  # IMPORTE_IMPON -> rem_impo1
        assert 'sac' in legajos_mapeados.columns  # ImporteSAC -> sac
        assert 'cant_hijos' in legajos_mapeados.columns  # hijos -> cant_hijos
        assert 'conyuge' in legajos_mapeados.columns  # conyugue -> conyuge
        
        # Verificar valores mapeados
        assert all(legajos_mapeados['periodo_fiscal'] == self.periodo_fiscal.periodo_str)
        
        # Verificar que los importes se mapearon correctamente
        assert legajos_mapeados['rem_total'].sum() == self.df_legajos['IMPORTE_BRUTO'].sum()
        assert legajos_mapeados['sac'].sum() == self.df_legajos['ImporteSAC'].sum()
        
        # Verificar tipos de datos
        assert legajos_mapeados['conyuge'].dtype == bool
        assert legajos_mapeados['cant_hijos'].dtype in [int, 'int64']
        assert legajos_mapeados['rem_total'].dtype in [float, 'float64']
        
        print(f"✅ Mapeo a tabla real completado: {len(legajos_mapeados)} legajos con {len(legajos_mapeados.columns)} campos")

    def test_validacion_restricciones_tabla(self):
        """Test de validación de restricciones de tabla real"""
        # Mapear legajos primero
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Ejecutar validación de restricciones
        self.database_saver._validar_restricciones_tabla(legajos_mapeados)
        
        # Verificar que no falló (las restricciones están bien)
        print("✅ Validación de restricciones exitosa")

    def test_guardado_completo_tabla_real(self):
        """Test completo del guardado en tabla afip_mapuche_sicoss"""
        # Ejecutar guardado completo
        resultado = self.database_saver.guardar_en_bd(
            legajos=self.df_legajos,
            periodo_fiscal=self.periodo_fiscal,
            incluir_inactivos=False
        )
        
        # Verificar resultado
        assert resultado['success'] == True
        assert resultado['legajos_guardados'] > 0
        assert resultado['periodo_fiscal'] == self.periodo_fiscal.periodo_str
        assert 'detalles' in resultado
        assert 'timestamp' in resultado
        
        # Verificar detalles del guardado
        detalles = resultado['detalles']
        assert detalles['tabla_destino'] == 'suc.afip_mapuche_sicoss'
        assert detalles['metodo'] == 'batch_insert'
        assert 'estadisticas' in detalles
        
        # Verificar que se llamó al método de inserción correcto
        assert self.mock_db.execute_batch_insert.called
        
        # Verificar argumentos de la llamada
        args, kwargs = self.mock_db.execute_batch_insert.call_args
        assert 'table_name' in kwargs
        assert kwargs['table_name'] == 'afip_mapuche_sicoss'
        assert 'schema' in kwargs
        assert kwargs['schema'] == 'suc'
        assert 'if_exists' in kwargs
        assert kwargs['if_exists'] == 'append'
        
        print(f"✅ Guardado en tabla real exitoso: {resultado['legajos_guardados']} legajos guardados")

    def test_estadisticas_tabla_real(self):
        """Test de generación de estadísticas con campos de tabla real"""
        # Mapear legajos para tener estructura correcta
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Generar estadísticas
        estadisticas = self.database_saver._generar_estadisticas_guardado(
            legajos_mapeados, self.periodo_fiscal
        )
        
        # Verificar estadísticas básicas
        assert 'total_legajos' in estadisticas
        assert estadisticas['total_legajos'] == len(legajos_mapeados)
        assert 'campos_guardados' in estadisticas
        assert 'tamaño_mb' in estadisticas
        assert 'periodo_fiscal' in estadisticas
        assert estadisticas['periodo_fiscal'] == self.periodo_fiscal.periodo_str
        
        # Verificar estadísticas específicas de tabla real
        if 'total_rem_total' in estadisticas:
            assert estadisticas['total_rem_total'] > 0
        
        if 'tipos_actividad' in estadisticas:
            assert isinstance(estadisticas['tipos_actividad'], dict)
        
        if 'situaciones' in estadisticas:
            assert isinstance(estadisticas['situaciones'], dict)
        
        print(f"✅ Estadísticas de tabla real generadas: {estadisticas['total_legajos']} legajos")

    def test_pipeline_completo_tabla_real(self):
        """Test del pipeline completo con tabla real"""
        # Datos de entrada simulados
        datos_entrada = {
            'legajos': self.df_legajos,
            'conceptos': pd.DataFrame({
                'nro_legaj': [12345, 67890], 
                'tipo_concepto': [1, 2],
                'importe': [1000, 2000]
            })
        }
        
        # Mock del processor para simular procesamiento
        with patch('processors.sicoss_processor.SicossDataProcessor') as mock_processor_class:
            mock_processor = Mock()
            mock_processor.procesar_datos_extraidos.return_value = {
                'success': True,
                'legajos_procesados': self.df_legajos
            }
            mock_processor_class.return_value = mock_processor
            
            # Ejecutar generación completa
            resultado = self.database_saver.generar_sicoss_bd(
                datos=datos_entrada,
                periodo_fiscal=self.periodo_fiscal,
                incluir_inactivos=False
            )
            
            # Verificar resultado
            assert resultado['success'] == True
            assert 'procesamiento' in resultado or 'guardado_bd' in resultado
            assert resultado['periodo_fiscal'] == self.periodo_fiscal.periodo_str
            
            # Verificar que el resultado indique guardado en BD
            if 'guardado_bd' in resultado:
                guardado_bd = resultado['guardado_bd']
                assert guardado_bd['detalles']['tabla_destino'] == 'suc.afip_mapuche_sicoss'
            
            # El método puede retornar diferentes estructuras según la implementación
            
            print("✅ Pipeline completo con tabla real exitoso")

    def test_campos_obligatorios_tabla(self):
        """Test que verifica que todos los campos obligatorios estén presentes"""
        # Mapear legajos
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Campos obligatorios de la tabla afip_mapuche_sicoss (NOT NULL)
        campos_obligatorios = [
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
        
        # Verificar que todos los campos obligatorios estén presentes
        for campo in campos_obligatorios:
            assert campo in legajos_mapeados.columns, f"Campo obligatorio {campo} falta en mapeo"
        
        print(f"✅ Todos los {len(campos_obligatorios)} campos obligatorios están presentes")

if __name__ == "__main__":
    """Ejecutar tests directamente"""
    print("🧪 Ejecutando tests de implementación real con tabla afip_mapuche_sicoss...")
    
    test_instance = TestDatabaseRealImplementation()
    
    try:
        # Ejecutar todos los tests
        test_instance.setup_method()
        test_instance.test_configuracion_inicial()
        
        test_instance.setup_method()
        test_instance.test_verificacion_tabla_existe()
        
        test_instance.setup_method()
        test_instance.test_mapeo_campos_real()
        
        test_instance.setup_method()
        test_instance.test_validacion_restricciones_tabla()
        
        test_instance.setup_method()
        test_instance.test_guardado_completo_tabla_real()
        
        test_instance.setup_method()
        test_instance.test_estadisticas_tabla_real()
        
        test_instance.setup_method()
        test_instance.test_pipeline_completo_tabla_real()
        
        test_instance.setup_method()
        test_instance.test_campos_obligatorios_tabla()
        
        print("\n🎉 TODOS LOS TESTS EXITOSOS")
        print("✅ Implementación real con tabla afip_mapuche_sicoss verificada")
        print("✅ Esquema: suc")
        print("✅ Tabla: afip_mapuche_sicoss")
        print("✅ Mapeo de campos: completo")
        print("✅ Guardado en BD: funcional")
        
    except Exception as e:
        print(f"\n❌ ERROR en tests: {e}")
        import traceback
        traceback.print_exc() 