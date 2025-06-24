"""
test_extractor.py

Script de prueba para validar el SicossDataExtractor
"""

from SicossDataExtractor import *
import time

def test_extractor_legajo_unico():
    """Prueba el extractor con un legajo específico"""
    print("=== TEST: EXTRACTOR CON LEGAJO ÚNICO ===")
    
    # Configuración para pruebas
    config = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=700000.0,
        trunca_tope=True,
        check_lic=False,
        asignacion_familiar=False
    )
    
    try:
        # Conexión a BD
        db = DatabaseConnection('database.ini')
        extractor = SicossDataExtractor(db)
        procesador = SicossDataProcessor(config)
        
        # Extraer datos para un legajo específico
        inicio = time.time()
        
        datos = extractor.extraer_datos_completos(
            config=config,
            per_anoct=2024,
            per_mesct=12,
            nro_legajo=10001  # Cambiar por un legajo que exista
        )
        
        tiempo_extraccion = time.time() - inicio
        
        # Procesar datos
        inicio_procesamiento = time.time()
        resultado = procesador.procesar_datos_extraidos(datos)
        tiempo_procesamiento = time.time() - inicio_procesamiento
        
        # Mostrar resultados
        print(f"\n⏱️ TIEMPOS:")
        print(f"  Extracción: {tiempo_extraccion:.2f}s")
        print(f"  Procesamiento: {tiempo_procesamiento:.2f}s")
        print(f"  Total: {tiempo_extraccion + tiempo_procesamiento:.2f}s")
        
        print(f"\n📊 ESTADÍSTICAS:")
        stats = resultado['estadisticas']
        print(f"  Total legajos: {stats['total_legajos']}")
        print(f"  Legajos válidos: {stats['legajos_validos']}")
        print(f"  Legajos rechazados: {stats['legajos_rechazados']}")
        
        if stats['legajos_validos'] > 0:
            print(f"\n💰 TOTALES:")
            totales = resultado['totales']
            for concepto, valor in totales.items():
                if valor > 0:
                    print(f"  {concepto}: ${valor:,.2f}")
            
            # Mostrar detalles del legajo procesado
            df_procesado = resultado['legajos_procesados']
            if not df_procesado.empty:
                print(f"\n👤 DETALLE DEL LEGAJO:")
                legajo = df_procesado.iloc[0]
                print(f"  Legajo: {legajo['nro_legaj']}")
                print(f"  Nombre: {legajo.get('apyno', 'N/A')}")
                print(f"  CUIT: {legajo.get('cuit', 'N/A')}")
                print(f"  Importe Bruto: ${legajo.get('IMPORTE_BRUTO', 0):,.2f}")
                print(f"  Importe Imponible: ${legajo.get('IMPORTE_IMPON', 0):,.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def test_extractor_multiples_legajos():
    """Prueba el extractor con múltiples legajos"""
    print("\n=== TEST: EXTRACTOR CON MÚLTIPLES LEGAJOS ===")
    
    config = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=700000.0,
        trunca_tope=True
    )
    
    try:
        db = DatabaseConnection('database.ini')
        extractor = SicossDataExtractor(db)
        procesador = SicossDataProcessor(config)
        
        # Extraer datos para todos los legajos activos (limitado para prueba)
        inicio = time.time()
        
        datos = extractor.extraer_datos_completos(
            config=config,
            per_anoct=2024,
            per_mesct=12,
            nro_legajo=None  # Todos los legajos
        )
        
        tiempo_extraccion = time.time() - inicio
        
        # Procesar solo si hay datos
        if datos['legajos'].empty:
            print("⚠️ No se encontraron legajos para procesar")
            return False
        
        # Limitar a los primeros 100 legajos para prueba
        if len(datos['legajos']) > 100:
            print(f"🔧 Limitando a 100 legajos (de {len(datos['legajos'])} encontrados)")
            for key in datos:
                if not datos[key].empty:
                    if key == 'legajos':
                        legajos_muestra = datos[key].head(100)['nro_legaj'].tolist()
                        datos[key] = datos[key].head(100)
                    else:
                        datos[key] = datos[key][datos[key]['nro_legaj'].isin(legajos_muestra)]
        
        inicio_procesamiento = time.time()
        resultado = procesador.procesar_datos_extraidos(datos)
        tiempo_procesamiento = time.time() - inicio_procesamiento
        
        # Estadísticas
        print(f"\n⏱️ TIEMPOS:")
        print(f"  Extracción: {tiempo_extraccion:.2f}s")
        print(f"  Procesamiento: {tiempo_procesamiento:.2f}s")
        print(f"  Total: {tiempo_extraccion + tiempo_procesamiento:.2f}s")
        
        stats = resultado['estadisticas']
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"  Total legajos: {stats['total_legajos']}")
        print(f"  Legajos válidos: {stats['legajos_validos']}")
        print(f"  Legajos rechazados: {stats['legajos_rechazados']}")
        print(f"  Tasa de éxito: {(stats['legajos_validos']/stats['total_legajos']*100) if stats['total_legajos'] > 0 else 0:.1f}%")
        
        if stats['legajos_validos'] > 0:
            totales = resultado['totales']
            print(f"\n💰 TOTALES GENERALES:")
            for concepto, valor in totales.items():
                if valor > 0:
                    print(f"  {concepto}: ${valor:,.2f}")
            
            # Estadísticas por legajo
            df_procesado = resultado['legajos_procesados']
            if not df_procesado.empty:
                print(f"\n📈 ESTADÍSTICAS POR LEGAJO:")
                print(f"  Promedio importe bruto: ${df_procesado['IMPORTE_BRUTO'].mean():,.2f}")
                print(f"  Máximo importe bruto: ${df_procesado['IMPORTE_BRUTO'].max():,.2f}")
                print(f"  Mínimo importe bruto: ${df_procesado['IMPORTE_BRUTO'].min():,.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def test_consultas_sql():
    """Prueba las consultas SQL individualmente"""
    print("\n=== TEST: CONSULTAS SQL INDIVIDUALES ===")
    
    try:
        db = DatabaseConnection('database.ini')
        
        # Test consulta legajos
        print("📋 Probando consulta de legajos...")
        query_legajos = SicossSQLQueries.get_legajos_query(2025, 5, "'REPA'", "dh01.nro_legaj = 10001")
        df_legajos = db.execute_query(query_legajos)
        print(f"   Resultado: {len(df_legajos)} legajos encontrados")
        
        if not df_legajos.empty:
            # Test consulta conceptos
            print("💰 Probando consulta de conceptos...")
            legajo_test = df_legajos['nro_legaj'].iloc[0]
            query_conceptos = SicossSQLQueries.get_conceptos_liquidados_query(
                2024, 12, f"dh21.nro_legaj = {legajo_test}"
            )
            df_conceptos = db.execute_query(query_conceptos)
            print(f"   Resultado: {len(df_conceptos)} conceptos encontrados")
            
            # Test otra actividad
            print("🏢 Probando consulta otra actividad...")
            query_otra = SicossSQLQueries.get_otra_actividad_query([legajo_test])
            df_otra = db.execute_query(query_otra)
            print(f"   Resultado: {len(df_otra)} registros de otra actividad")
            
            # Test obra social
            print("🏥 Probando consulta obra social...")
            query_os = SicossSQLQueries.get_codigos_obra_social_query([legajo_test])
            df_os = db.execute_query(query_os)
            print(f"   Resultado: {len(df_os)} códigos de obra social")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR en consultas: {e}")
        return False

def main():
    """Ejecuta todas las pruebas"""
    print("🚀 INICIANDO TESTS DEL SICOSS DATA EXTRACTOR")
    print("=" * 50)
    
    resultados = []
    
    # Test 1: Consultas SQL
    resultados.append(("Consultas SQL", test_consultas_sql()))
    
    # Test 2: Legajo único
    resultados.append(("Legajo único", test_extractor_legajo_unico()))
    
    # Test 3: Múltiples legajos
    resultados.append(("Múltiples legajos", test_extractor_multiples_legajos()))
    
    # Resumen
    print("\n" + "=" * 50)
    print("📋 RESUMEN DE TESTS:")
    for nombre, resultado in resultados:
        estado = "✅ PASS" if resultado else "❌ FAIL"
        print(f"  {nombre}: {estado}")
    
    total_pass = sum(1 for _, r in resultados if r)
    print(f"\nTotal: {total_pass}/{len(resultados)} tests exitosos")
    
    if total_pass == len(resultados):
        print("🎉 ¡Todos los tests pasaron exitosamente!")
    else:
        print("⚠️ Algunos tests fallaron. Revisa la configuración y conexión a BD.")

if __name__ == "__main__":
    main() 