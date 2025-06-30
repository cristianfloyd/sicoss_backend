#!/usr/bin/env python3
"""
quick_test.py - Prueba rápida ConceptosProcessor
Uso: python quick_test.py 123456
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

def quick_test(nro_legaj):
    """Prueba rápida de ConceptosProcessor"""
    try:
        from test_conceptos_processor import ConceptosProcessorTester
        
        print(f"🧪 Prueba rápida para legajo {nro_legaj}")
        print("-" * 50)
        
        tester = ConceptosProcessorTester()
        tester.setup()
        
        # Usar período actual
        from datetime import datetime
        now = datetime.now()
        
        resultados = tester.test_legajo(nro_legaj, now.year, now.month)
        
        if not resultados.get('legajo_encontrado', False):
            print(f"❌ Legajo {nro_legaj} no encontrado")
            return
        
        if 'error' in resultados:
            print(f"❌ Error: {resultados['error']}")
            return
        
        # Mostrar resumen rápido
        info = resultados['legajo_info']
        stats = resultados['estadisticas']
        campos = resultados['campos_sicoss_salida']
        
        print(f"✅ {info['apyno']} ({info['nro_legaj']})")
        print(f"📊 {stats['total_conceptos']} conceptos → {stats['campos_con_valor']} campos SICOSS")
        print(f"💰 Total: ${stats['total_importe']:,.2f}")
        
        if campos:
            print("\n🎯 Campos principales:")
            for campo, valor in sorted(campos.items())[:5]:  # Top 5
                if isinstance(valor, float):
                    print(f"   {campo}: ${valor:,.2f}")
                else:
                    print(f"   {campo}: {valor}")
        
        print("\n✅ Prueba completada")
        
        tester.cleanup()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python quick_test.py NUMERO_LEGAJO")
        sys.exit(1)
    
    try:
        legajo = int(sys.argv[1])
        quick_test(legajo)
    except ValueError:
        print("❌ El número de legajo debe ser un entero")
        sys.exit(1) 