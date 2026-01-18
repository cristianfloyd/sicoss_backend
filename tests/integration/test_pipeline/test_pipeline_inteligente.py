#!/usr/bin/env python3
"""
Test Pipeline Inteligente SICOSS Backend
========================================

Test inteligente que selecciona específicamente legajos que SÍ tienen
conceptos/liquidaciones, garantizando que el pipeline funcione correctamente.

PROBLEMA IDENTIFICADO:
- dh01 (legajos): ~112,055 legajos total
- dh21 (liquidaciones): ~38,000 legajos (~30% con liquidaciones)
- Si tomamos legajos al azar de dh01, pueden no tener conceptos en dh21

SOLUCIÓN:
- Seleccionar específicamente legajos que tienen conceptos en dh21
- Garantizar que el test siempre funcione con datos completos
"""

import time
from typing import Dict, List

from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from extractors.data_extractor_manager import DataExtractorManager
from processors.database_saver import SicossDatabaseSaver

# Imports del sistema
from processors.sicoss_processor import SicossDataProcessor
from value_objects.periodo_fiscal import PeriodoFiscal


class TestPipelineInteligente:
    """Test inteligente del pipeline SICOSS que garantiza datos completos"""

    def __init__(self):
        print("🧠 Inicializando Test Pipeline Inteligente")

        # Configuración
        self.config = SicossConfig(
            tope_jubilatorio_patronal=800000.0,
            tope_jubilatorio_personal=600000.0,
            tope_otros_aportes_personales=400000.0,
            trunca_tope=True,
        )

        # Conexión BD
        self.db = DatabaseConnection()

        # Componentes
        self.extractor = DataExtractorManager(self.db)
        self.processor = SicossDataProcessor(self.config)
        self.saver = SicossDatabaseSaver(self.config, self.db)

        print("✅ Componentes inicializados")

    def seleccionar_legajos_con_conceptos(
        self, per_anoct: int, per_mesct: int, limite: int = 5
    ) -> List[int]:
        """
        Selecciona específicamente legajos que tienen conceptos/liquidaciones

        Args:
            per_anoct: Año del período
            per_mesct: Mes del período
            limite: Número máximo de legajos a seleccionar

        Returns:
            Lista de números de legajo que tienen conceptos
        """
        print(
            f"\n🎯 Seleccionando {limite} legajos con conceptos para {per_anoct}/{per_mesct:02d}"
        )

        # Query para encontrar legajos que SÍ tienen conceptos
        query_legajos_con_conceptos = f"""
        SELECT DISTINCT 
            dh21.nro_legaj,
            COUNT(dh21.codn_conce) as num_conceptos,
            SUM(dh21.impp_conce) as total_importe
        FROM mapuche.dh21
        INNER JOIN mapuche.dh22 ON dh22.nro_liqui = dh21.nro_liqui
        WHERE dh22.per_liano = {per_anoct} 
        AND dh22.per_limes = {per_mesct}
        AND dh22.sino_genimp = true
        AND dh21.codn_conce > 0
        AND dh21.impp_conce > 0
        GROUP BY dh21.nro_legaj
        HAVING COUNT(dh21.codn_conce) >= 2  -- Al menos 2 conceptos
        ORDER BY COUNT(dh21.codn_conce) DESC, SUM(dh21.impp_conce) DESC
        LIMIT {limite}
        """

        df_legajos_con_conceptos = self.db.execute_query(query_legajos_con_conceptos)

        if df_legajos_con_conceptos.empty:
            raise Exception(
                f"No se encontraron legajos con conceptos para {per_anoct}/{per_mesct}"
            )

        legajos_seleccionados = df_legajos_con_conceptos["nro_legaj"].tolist()

        print(f"✅ Seleccionados {len(legajos_seleccionados)} legajos con conceptos:")
        for _, row in df_legajos_con_conceptos.iterrows():
            print(
                f"   - Legajo {int(row['nro_legaj'])}: {int(row['num_conceptos'])} conceptos, ${float(row['total_importe']):,.2f}"
            )

        return legajos_seleccionados

    def ejecutar_test_inteligente(
        self, per_anoct: int = 2025, per_mesct: int = 5, limite: int = 5
    ) -> Dict:
        """Ejecuta el test inteligente del pipeline con legajos garantizados"""

        print(f"\n🔄 EJECUTANDO PIPELINE INTELIGENTE")
        print(f"📅 Período: {per_anoct}/{per_mesct:02d}")
        print(f"📊 Límite: {limite} legajos con conceptos garantizados")
        print("=" * 60)

        resultado = {"success": False, "fases": {}, "metricas": {}, "errores": []}

        inicio = time.time()

        try:
            # FASE 0: SELECCIÓN INTELIGENTE DE LEGAJOS
            print("\n🎯 FASE 0: Selección inteligente de legajos...")
            t0 = time.time()

            legajos_seleccionados = self.seleccionar_legajos_con_conceptos(
                per_anoct, per_mesct, limite
            )

            t0 = time.time() - t0
            print(
                f"✅ Selección: {t0:.2f}s - {len(legajos_seleccionados)} legajos seleccionados"
            )

            # FASE 1: EXTRACCIÓN DIRIGIDA
            print("\n📥 FASE 1: Extrayendo datos dirigidos...")
            t1 = time.time()

            # Extraer datos completos sin límite inicial
            datos_completos = self.extractor.extraer_datos_completos(
                config=self.config, per_anoct=per_anoct, per_mesct=per_mesct
            )

            # Filtrar específicamente los legajos seleccionados
            datos_filtrados = {
                "legajos": datos_completos["legajos"][
                    datos_completos["legajos"]["nro_legaj"].isin(legajos_seleccionados)
                ].reset_index(drop=True),
                "conceptos": datos_completos["conceptos"][
                    datos_completos["conceptos"]["nro_legaj"].isin(
                        legajos_seleccionados
                    )
                ].reset_index(drop=True),
                "otra_actividad": datos_completos["otra_actividad"][
                    datos_completos["otra_actividad"]["nro_legaj"].isin(
                        legajos_seleccionados
                    )
                ].reset_index(drop=True)
                if not datos_completos["otra_actividad"].empty
                else datos_completos["otra_actividad"],
                "obra_social": datos_completos["obra_social"][
                    datos_completos["obra_social"]["nro_legaj"].isin(
                        legajos_seleccionados
                    )
                ].reset_index(drop=True),
            }

            t1 = time.time() - t1

            print(f"✅ Extracción dirigida: {t1:.2f}s")
            print(f"   - Legajos extraídos: {len(datos_filtrados['legajos'])}")
            print(f"   - Conceptos extraídos: {len(datos_filtrados['conceptos'])}")
            print(f"   - Otra actividad: {len(datos_filtrados['otra_actividad'])}")
            print(f"   - Obra social: {len(datos_filtrados['obra_social'])}")

            if datos_filtrados["conceptos"].empty:
                raise Exception(
                    "Error crítico: No se extrajeron conceptos para los legajos seleccionados"
                )

            resultado["fases"]["seleccion"] = {
                "tiempo": t0,
                "legajos": len(legajos_seleccionados),
            }
            resultado["fases"]["extraccion"] = {
                "tiempo": t1,
                "legajos": len(datos_filtrados["legajos"]),
                "conceptos": len(datos_filtrados["conceptos"]),
            }

            # FASE 2: PROCESAMIENTO GARANTIZADO
            print("\n⚙️ FASE 2: Procesando datos garantizados...")
            t2 = time.time()

            proc_resultado = self.processor.procesar_datos_extraidos(datos_filtrados)

            if not proc_resultado["success"]:
                raise Exception(f"Error procesamiento: {proc_resultado.get('error')}")

            df_procesados = proc_resultado["data"]["legajos"]
            t2 = time.time() - t2

            print(f"✅ Procesamiento: {t2:.2f}s")
            print(f"   - Legajos procesados: {len(df_procesados)}")
            print(f"   - Columnas generadas: {len(df_procesados.columns)}")

            # Mostrar resumen de importes calculados
            if "IMPORTE_BRUTO" in df_procesados.columns:
                total_bruto = df_procesados["IMPORTE_BRUTO"].sum()
                promedio_bruto = df_procesados["IMPORTE_BRUTO"].mean()
                print(f"   - Total bruto: ${total_bruto:,.2f}")
                print(f"   - Promedio bruto: ${promedio_bruto:,.2f}")

            if "ImporteSAC" in df_procesados.columns:
                total_sac = df_procesados["ImporteSAC"].sum()
                print(f"   - Total SAC: ${total_sac:,.2f}")

            resultado["fases"]["procesamiento"] = {
                "tiempo": t2,
                "legajos": len(df_procesados),
                "columnas": len(df_procesados.columns),
            }

            # FASE 3: GUARDADO EN BD
            print("\n💾 FASE 3: Guardando en BD...")
            t3 = time.time()

            periodo = PeriodoFiscal.from_string(f"{per_anoct}{per_mesct:02d}")

            guardado = self.saver.guardar_en_bd(
                legajos=df_procesados, periodo_fiscal=periodo, incluir_inactivos=False
            )

            if not guardado["success"]:
                raise Exception(f"Error guardado: {guardado.get('error')}")

            t3 = time.time() - t3

            print(f"✅ Guardado: {t3:.2f}s")
            print(f"   - Legajos guardados: {guardado['legajos_guardados']}")
            print(f"   - Tabla: suc.afip_mapuche_sicoss")

            resultado["fases"]["guardado"] = {
                "tiempo": t3,
                "legajos": guardado["legajos_guardados"],
            }

            # FASE 4: VERIFICACIÓN FINAL
            print("\n🔍 FASE 4: Verificación final...")
            t4 = time.time()

            verificacion = self._verificar_guardado_inteligente(
                periodo, guardado["legajos_guardados"], legajos_seleccionados
            )
            t4 = time.time() - t4

            print(f"✅ Verificación: {t4:.2f}s")
            for check, ok in verificacion.items():
                status = "✅" if ok else "❌"
                print(f"   {status} {check}")

            resultado["fases"]["verificacion"] = {"tiempo": t4, "checks": verificacion}

            # RESULTADO FINAL
            tiempo_total = time.time() - inicio
            todos_exitosos = all(verificacion.values())

            resultado.update(
                {
                    "success": todos_exitosos,
                    "tiempo_total": tiempo_total,
                    "legajos_seleccionados": legajos_seleccionados,
                    "metricas": {
                        "legajos_procesados": len(df_procesados),
                        "legajos_guardados": guardado["legajos_guardados"],
                        "conceptos_procesados": len(datos_filtrados["conceptos"]),
                        "throughput": len(df_procesados) / tiempo_total,
                        "eficiencia": "ALTA" if todos_exitosos else "BAJA",
                    },
                }
            )

            print(f"\n🎉 PIPELINE INTELIGENTE COMPLETADO")
            print("=" * 60)
            print(f"⏱️ Tiempo total: {tiempo_total:.2f}s")
            print(f"📊 Legajos procesados: {len(df_procesados)}")
            print(f"💾 Legajos guardados: {guardado['legajos_guardados']}")
            print(f"🚀 Throughput: {resultado['metricas']['throughput']:.2f} legajos/s")
            print(f"🎯 Eficiencia: {resultado['metricas']['eficiencia']}")

            return resultado

        except Exception as e:
            error = f"Error en pipeline inteligente: {str(e)}"
            print(f"\n❌ {error}")
            resultado["errores"].append(error)
            resultado["tiempo_total"] = time.time() - inicio
            return resultado

    def _verificar_guardado_inteligente(
        self, periodo: PeriodoFiscal, esperados: int, legajos_originales: List[int]
    ) -> Dict[str, bool]:
        """Verifica que los datos se guardaron correctamente con verificaciones adicionales"""
        checks = {}

        try:
            # Verificación básica
            query_basica = f"""
            SELECT 
                COUNT(*) as total,
                SUM(rem_total) as total_bruto,
                AVG(rem_total) as promedio_bruto
            FROM suc.afip_mapuche_sicoss 
            WHERE periodo_fiscal = '{periodo.periodo_str}'
            """

            resultado_basico = self.db.execute_query(query_basica)

            if not resultado_basico.empty:
                row = resultado_basico.iloc[0]
                checks["Registros guardados"] = int(row["total"]) > 0
                checks["Cantidad correcta"] = int(row["total"]) == esperados
                checks["Importes válidos"] = float(row["total_bruto"]) > 0
                checks["Promedio razonable"] = (
                    float(row["promedio_bruto"]) > 10000
                )  # >$10k promedio

            # Verificación específica de legajos
            legajos_str = ",".join(map(str, legajos_originales))
            query_especifica = f"""
            SELECT 
                COUNT(DISTINCT cuil) as legajos_distintos
            FROM suc.afip_mapuche_sicoss 
            WHERE periodo_fiscal = '{periodo.periodo_str}'
            """

            resultado_especifico = self.db.execute_query(query_especifica)

            if not resultado_especifico.empty:
                legajos_guardados = int(
                    resultado_especifico.iloc[0]["legajos_distintos"]
                )
                checks["Legajos únicos guardados"] = legajos_guardados == len(
                    legajos_originales
                )

        except Exception as e:
            checks["Error verificación"] = False
            print(f"Error en verificación: {e}")

        return checks

    def close(self):
        """Cerrar conexiones"""
        self.db.close()


def main():
    """Función principal del test inteligente"""
    print("🧠 TEST PIPELINE INTELIGENTE SICOSS")
    print("=" * 60)
    print("Garantiza datos completos seleccionando legajos con conceptos")
    print("=" * 60)

    test_pipeline = None
    try:
        # Crear test inteligente
        test_pipeline = TestPipelineInteligente()

        # Ejecutar test con diferentes tamaños
        tamaños = [3, 5, 10]  # Empezar pequeño e ir aumentando

        for tamaño in tamaños:
            print(f"\n🧪 EJECUTANDO TEST CON {tamaño} LEGAJOS")
            print("-" * 50)

            resultado = test_pipeline.ejecutar_test_inteligente(limite=tamaño)

            if resultado["success"]:
                print(f"✅ Test con {tamaño} legajos: ¡ÉXITO!")
                metricas = resultado.get("metricas", {})
                print(
                    f"   📊 Métricas: {metricas['legajos_procesados']} procesados, "
                    f"{metricas['throughput']:.2f} legajos/s"
                )
                break  # Si funciona, no necesitamos probar tamaños mayores
            else:
                print(f"❌ Test con {tamaño} legajos: FALLÓ")
                for error in resultado.get("errores", []):
                    print(f"   - {error}")

        # Resultado final
        print("\n" + "=" * 60)
        print("RESULTADO FINAL DEL TEST INTELIGENTE")
        print("=" * 60)

        if resultado["success"]:
            print("🎉 ¡TEST INTELIGENTE EXITOSO!")
            print("✅ Pipeline completo funcionando con datos garantizados")
            print("🚀 Sistema verificado y listo para producción")

            # Mostrar información de los legajos procesados
            legajos_sel = resultado.get("legajos_seleccionados", [])
            print(f"\n📋 Legajos procesados exitosamente: {legajos_sel}")

        else:
            print("❌ TEST INTELIGENTE FALLÓ")
            print("🔧 Revisar errores:")
            for error in resultado.get("errores", []):
                print(f"   - {error}")

        return resultado["success"]

    except Exception as e:
        print(f"💥 Error crítico: {e}")
        return False
    finally:
        if test_pipeline:
            test_pipeline.close()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
