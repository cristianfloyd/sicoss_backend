"""
test_database_complete.py

Test completo de la implementación real de guardado en BD SICOSS

Este test verifica que la implementación completa funcione correctamente.
"""

import pandas as pd
import pytest
import sys
import os
from datetime import datetime
from unittest.mock import Mock, patch

# Imports del proyecto
sys.path.append(os.path.dirname(__file__))

from processors.database_saver import SicossDatabaseSaver
from value_objects.periodo_fiscal import PeriodoFiscal
from database.database_connection import DatabaseConnection

class TestDatabaseCompleteImplementation:
    """Test de la implementación completa de BD"""
    
    def setup_method(self):
        """Setup para cada test"""
        # Mock de la conexión de BD para evitar dependencias reales
        self.mock_db = Mock(spec=DatabaseConnection)
        self.mock_db.execute_query.return_value = True
        self.mock_db.execute_batch_insert.return_value = 100
        
        # Crear database_saver con mock
        self.database_saver = SicossDatabaseSaver(db_connection=self.mock_db)
        
        # Período fiscal de prueba
        self.periodo_fiscal = PeriodoFiscal.from_string("202501")
        
        # DataFrame de legajos de prueba
        self.df_legajos = pd.DataFrame({
            'nro_legaj': [12345, 67890, 11111],
            'cuit': ['20123456789', '27987654321', '20111111111'],
            'apyno': ['PEREZ, JUAN', 'GOMEZ, MARIA', 'LOPEZ, CARLOS'],
            'IMPORTE_BRUTO': [100000.0, 150000.0, 120000.0],
            'IMPORTE_IMPON': [80000.0, 120000.0, 100000.0],
            'ImporteImponiblePatronal': [85000.0, 125000.0, 105000.0],
            'ImporteSAC': [8333.33, 12500.0, 10416.67],
            'ImporteNoRemun': [5000.0, 7500.0, 6000.0],
            'TipoDeActividad': [0, 11, 0],
            'codigo_os': ['000000', '123456', '789012']
        })

    def test_esquema_completo_definido(self):
        """Test que verifica que el esquema SICOSS esté completamente definido"""
        # Verificar que el esquema tenga todos los campos esperados
        esquema = self.database_saver.sicoss_schema
        
        # Campos obligatorios
        campos_obligatorios = [
            'id', 'periodo_fiscal', 'fecha_procesamiento', 'nro_legaj',
            'importe_bruto', 'importe_impon', 'importe_imponible_patronal'
        ]
        
        for campo in campos_obligatorios:
            assert campo in esquema, f"Campo obligatorio {campo} falta en esquema"
        
        print(f"✅ Esquema SICOSS tiene {len(esquema)} campos definidos")

    def test_mapeo_campos_completo(self):
        """Test que verifica el mapeo completo de campos"""
        mapeo = self.database_saver.campo_mapping
        
        # Verificar mapeos importantes
        mapeos_importantes = {
            'nro_legaj': 'nro_legaj',
            'IMPORTE_BRUTO': 'importe_bruto',
            'ImporteImponiblePatronal': 'importe_imponible_patronal',
            'ImporteSAC': 'importe_sac'
        }
        
        for campo_origen, campo_bd in mapeos_importantes.items():
            assert campo_origen in mapeo, f"Campo origen {campo_origen} falta en mapeo"
            assert mapeo[campo_origen] == campo_bd, f"Mapeo incorrecto para {campo_origen}"
        
        print(f"✅ Mapeo de campos tiene {len(mapeo)} mapeos definidos")

    def test_ensure_table_exists(self):
        """Test que verifica la creación de tabla"""
        # Ejecutar creación de tabla
        self.database_saver._ensure_table_exists(self.periodo_fiscal)
        
        # Verificar que se llamaron los métodos correctos
        assert self.mock_db.execute_query.called
        
        # Verificar que se creó el esquema
        calls = self.mock_db.execute_query.call_args_list
        schema_call = any("CREATE SCHEMA" in str(call) for call in calls)
        table_call = any("CREATE TABLE" in str(call) for call in calls)
        index_calls = [call for call in calls if "CREATE INDEX" in str(call)]
        
        assert schema_call, "No se creó el esquema"
        assert table_call, "No se creó la tabla"
        assert len(index_calls) >= 3, "No se crearon suficientes índices"
        
        print("✅ Creación de tabla y esquema verificada")

    def test_mapear_legajos_completo(self):
        """Test completo del mapeo de legajos"""
        # Ejecutar mapeo
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Verificaciones básicas
        assert len(legajos_mapeados) == len(self.df_legajos)
        assert 'periodo_fiscal' in legajos_mapeados.columns
        assert 'fecha_procesamiento' in legajos_mapeados.columns
        assert 'estado_procesamiento' in legajos_mapeados.columns
        
        # Verificar que se mapearon los campos principales
        assert 'nro_legaj' in legajos_mapeados.columns
        assert 'importe_bruto' in legajos_mapeados.columns
        assert 'importe_imponible_patronal' in legajos_mapeados.columns
        
        # Verificar valores mapeados
        assert all(legajos_mapeados['periodo_fiscal'] == self.periodo_fiscal.periodo_str)
        assert all(legajos_mapeados['estado_procesamiento'] == 'PROCESADO')
        
        # Verificar que los importes se mapearon correctamente
        assert legajos_mapeados['importe_bruto'].sum() == self.df_legajos['IMPORTE_BRUTO'].sum()
        
        print(f"✅ Mapeo completado: {len(legajos_mapeados)} legajos con {len(legajos_mapeados.columns)} campos")

    def test_validar_datos_completo(self):
        """Test completo de validación de datos"""
        # Mapear legajos primero
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Ejecutar validación
        resultado = self.database_saver._validar_datos_para_bd(legajos_mapeados)
        
        assert resultado == True
        print("✅ Validación de datos exitosa")

    def test_filtrar_legajos_activos(self):
        """Test del filtrado de legajos activos"""
        # Mapear legajos primero
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Ejecutar filtrado
        legajos_activos = self.database_saver._filtrar_legajos_activos(legajos_mapeados)
        
        # Verificar que se mantuvieron los legajos válidos
        assert len(legajos_activos) <= len(legajos_mapeados)
        assert len(legajos_activos) > 0  # Debe haber al menos algunos activos
        
        print(f"✅ Filtrado: {len(legajos_mapeados)} → {len(legajos_activos)} legajos activos")

    def test_guardar_en_bd_completo(self):
        """Test completo del guardado en BD"""
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
        
        # Verificar que se llamó al método de inserción
        assert self.mock_db.execute_batch_insert.called
        
        # Verificar argumentos de la llamada
        args, kwargs = self.mock_db.execute_batch_insert.call_args
        assert 'schema' in kwargs
        assert kwargs['schema'] == 'sicoss'
        assert 'if_exists' in kwargs
        assert kwargs['if_exists'] == 'append'
        
        print(f"✅ Guardado BD exitoso: {resultado['legajos_guardados']} legajos guardados")

    def test_estadisticas_guardado(self):
        """Test de generación de estadísticas"""
        # Mapear legajos para tener estructura correcta
        legajos_mapeados = self.database_saver._mapear_legajos_a_modelo(
            self.df_legajos, self.periodo_fiscal
        )
        
        # Generar estadísticas
        estadisticas = self.database_saver._generar_estadisticas_guardado(legajos_mapeados)
        
        # Verificar estadísticas básicas
        assert 'total_legajos' in estadisticas
        assert estadisticas['total_legajos'] == len(legajos_mapeados)
        assert 'campos_guardados' in estadisticas
        assert 'tamaño_mb' in estadisticas
        
        print(f"✅ Estadísticas generadas: {estadisticas['total_legajos']} legajos, {estadisticas['campos_guardados']} campos")

    def test_generar_sicoss_bd_completo(self):
        """Test completo de generación SICOSS directa a BD"""
        # Datos de entrada simulados
        datos_entrada = {
            'legajos': self.df_legajos,
            'conceptos': pd.DataFrame({
                'nro_legaj': [12345, 67890], 
                'tipo_concepto': [1, 2],
                'importe': [1000, 2000]
            })
        }
        
        # Mock del processor
        with patch('processors.database_saver.SicossDataProcessor') as mock_processor_class:
            mock_processor = Mock()
            mock_processor.procesar_datos_extraidos.return_value = {
                'success': True,
                'legajos_procesados': self.df_legajos
            }
            mock_processor_class.return_value = mock_processor
            
            # Ejecutar generación
            resultado = self.database_saver.generar_sicoss_bd(
                datos=datos_entrada,
                periodo_fiscal=self.periodo_fiscal,
                incluir_inactivos=False
            )
            
            # Verificar resultado
            assert resultado['success'] == True
            assert 'procesamiento' in resultado
            assert 'guardado_bd' in resultado
            assert resultado['periodo_fiscal'] == self.periodo_fiscal.periodo_str
            
            print("✅ Generación SICOSS directa a BD exitosa")

if __name__ == "__main__":
    """Ejecutar tests directamente"""
    print("🧪 Ejecutando tests de implementación completa de BD...")
    
    test_instance = TestDatabaseCompleteImplementation()
    
    try:
        # Ejecutar todos los tests
        test_instance.setup_method()
        test_instance.test_esquema_completo_definido()
        
        test_instance.setup_method()
        test_instance.test_mapeo_campos_completo()
        
        test_instance.setup_method()
        test_instance.test_ensure_table_exists()
        
        test_instance.setup_method()
        test_instance.test_mapear_legajos_completo()
        
        test_instance.setup_method()
        test_instance.test_validar_datos_completo()
        
        test_instance.setup_method()
        test_instance.test_filtrar_legajos_activos()
        
        test_instance.setup_method()
        test_instance.test_guardar_en_bd_completo()
        
        test_instance.setup_method()
        test_instance.test_estadisticas_guardado()
        
        test_instance.setup_method()
        test_instance.test_generar_sicoss_bd_completo()
        
        print("\n🎉 TODOS LOS TESTS EXITOSOS")
        print("✅ Implementación completa de BD verificada")
        
    except Exception as e:
        print(f"\n❌ ERROR en tests: {e}")
        import traceback
        traceback.print_exc() 