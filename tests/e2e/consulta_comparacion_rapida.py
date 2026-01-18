#!/usr/bin/env python3
"""
Consulta Rápida de Comparación SICOSS
=====================================

Script rápido para verificar datos en ambas tablas SICOSS
y hacer una comparación básica entre Python y PHP Legacy.
"""

import pandas as pd

from database.database_connection import DatabaseConnection


def main():
    """Función principal de consulta rápida"""

    print("🔍 CONSULTA RÁPIDA COMPARACIÓN SICOSS")
    print("=" * 50)

    db = DatabaseConnection()

    try:
        # 1. Verificar qué períodos tenemos en cada tabla
        print("\n📊 VERIFICANDO PERÍODOS DISPONIBLES:")
        print("-" * 40)

        # Tabla Python (nueva)
        query_python = """
        SELECT 
            periodo_fiscal,
            COUNT(*) as total_registros,
            MIN(cuil) as primer_cuil,
            MAX(cuil) as ultimo_cuil
        FROM suc.afip_mapuche_sicoss 
        GROUP BY periodo_fiscal 
        ORDER BY periodo_fiscal DESC
        """

        print("✅ Tabla Python (suc.afip_mapuche_sicoss):")
        df_python = db.execute_query(query_python)
        if not df_python.empty:
            for _, row in df_python.iterrows():
                print(
                    f"   Período {row['periodo_fiscal']}: {row['total_registros']} registros"
                )
        else:
            print("   ❌ Sin datos")

        # Tabla PHP Legacy
        query_legacy = """
        SELECT 
            periodo_fiscal,
            COUNT(*) as total_registros,
            MIN(cuil) as primer_cuil,
            MAX(cuil) as ultimo_cuil
        FROM suc.afip_mapuche_sicoss_bkp 
        GROUP BY periodo_fiscal 
        ORDER BY periodo_fiscal DESC
        """

        print("\n✅ Tabla PHP Legacy (suc.afip_mapuche_sicoss_bkp):")
        df_legacy = db.execute_query(query_legacy)
        if not df_legacy.empty:
            for _, row in df_legacy.iterrows():
                print(
                    f"   Período {row['periodo_fiscal']}: {row['total_registros']} registros"
                )
        else:
            print("   ❌ Sin datos")

        # 2. Buscar períodos comunes
        if not df_python.empty and not df_legacy.empty:
            periodos_python = set(df_python["periodo_fiscal"])
            periodos_legacy = set(df_legacy["periodo_fiscal"])
            periodos_comunes = periodos_python.intersection(periodos_legacy)

            print(f"\n🔍 PERÍODOS COMUNES: {len(periodos_comunes)}")
            for periodo in sorted(periodos_comunes, reverse=True):
                print(f"   ✓ {periodo}")

            # 3. Si hay períodos comunes, comparar uno
            if periodos_comunes:
                periodo_target = sorted(periodos_comunes, reverse=True)[0]
                print(f"\n📊 COMPARANDO PERÍODO: {periodo_target}")
                print("-" * 40)

                # Buscar CUILs comunes
                query_comunes = f"""
                SELECT 
                    p.cuil,
                    p.apnom,
                    p.rem_total as python_rem_total,
                    l.rem_total as legacy_rem_total,
                    p.rem_impo1 as python_rem_impo1,
                    l.rem_impo1 as legacy_rem_impo1,
                    p.sac as python_sac,
                    l.sac as legacy_sac
                FROM suc.afip_mapuche_sicoss p
                INNER JOIN suc.afip_mapuche_sicoss_bkp l ON p.cuil = l.cuil
                WHERE p.periodo_fiscal = '{periodo_target}'
                AND l.periodo_fiscal = '{periodo_target}'
                LIMIT 5
                """

                df_comparacion = db.execute_query(query_comunes)

                if not df_comparacion.empty:
                    print(f"✅ Encontrados {len(df_comparacion)} registros comunes")
                    print("\n🔍 MUESTRA DE COMPARACIÓN:")

                    for i, row in df_comparacion.iterrows():
                        print(f"\n👤 CUIL: {row['cuil']} - {row['apnom']}")

                        # Comparar campos principales
                        campos_comparar = [
                            ("rem_total", "Remuneración Total"),
                            ("rem_impo1", "Rem. Imponible 1"),
                            ("sac", "SAC"),
                        ]

                        for campo, nombre in campos_comparar:
                            val_python = row[f"python_{campo}"]
                            val_legacy = row[f"legacy_{campo}"]

                            if pd.isna(val_python):
                                val_python = 0
                            if pd.isna(val_legacy):
                                val_legacy = 0

                            diferencia = abs(float(val_python) - float(val_legacy))
                            coinciden = diferencia < 0.01

                            status = "✅" if coinciden else "❌"
                            print(f"   {status} {nombre}:")
                            print(f"      Python: ${val_python:,.2f}")
                            print(f"      Legacy: ${val_legacy:,.2f}")
                            if not coinciden:
                                print(f"      Diferencia: ${diferencia:,.2f}")

                        if i >= 2:  # Limitar a 3 registros
                            break

                    # 4. Estadísticas generales
                    print(f"\n📈 ESTADÍSTICAS DEL PERÍODO {periodo_target}:")
                    query_stats = f"""
                    SELECT 
                        'Python' as sistema,
                        COUNT(*) as total_registros,
                        SUM(rem_total) as suma_rem_total,
                        AVG(rem_total) as promedio_rem_total,
                        SUM(rem_impo1) as suma_rem_impo1,
                        SUM(sac) as suma_sac
                    FROM suc.afip_mapuche_sicoss 
                    WHERE periodo_fiscal = '{periodo_target}'
                    
                    UNION ALL
                    
                    SELECT 
                        'Legacy' as sistema,
                        COUNT(*) as total_registros,
                        SUM(rem_total) as suma_rem_total,
                        AVG(rem_total) as promedio_rem_total,
                        SUM(rem_impo1) as suma_rem_impo1,
                        SUM(sac) as suma_sac
                    FROM suc.afip_mapuche_sicoss_bkp 
                    WHERE periodo_fiscal = '{periodo_target}'
                    """

                    df_stats = db.execute_query(query_stats)

                    if len(df_stats) == 2:
                        python_stats = df_stats[df_stats["sistema"] == "Python"].iloc[0]
                        legacy_stats = df_stats[df_stats["sistema"] == "Legacy"].iloc[0]

                        print(f"\n📊 Sistema Python:")
                        print(f"   Registros: {python_stats['total_registros']:,}")
                        print(
                            f"   Suma Rem Total: ${python_stats['suma_rem_total']:,.2f}"
                        )
                        print(f"   Suma SAC: ${python_stats['suma_sac']:,.2f}")

                        print(f"\n📊 Sistema Legacy:")
                        print(f"   Registros: {legacy_stats['total_registros']:,}")
                        print(
                            f"   Suma Rem Total: ${legacy_stats['suma_rem_total']:,.2f}"
                        )
                        print(f"   Suma SAC: ${legacy_stats['suma_sac']:,.2f}")

                        # Diferencias totales
                        diff_registros = int(python_stats["total_registros"]) - int(
                            legacy_stats["total_registros"]
                        )
                        diff_rem_total = float(python_stats["suma_rem_total"]) - float(
                            legacy_stats["suma_rem_total"]
                        )
                        diff_sac = float(python_stats["suma_sac"]) - float(
                            legacy_stats["suma_sac"]
                        )

                        print(f"\n🔍 DIFERENCIAS:")
                        print(f"   Registros: {diff_registros:+,}")
                        print(f"   Rem Total: ${diff_rem_total:+,.2f}")
                        print(f"   SAC: ${diff_sac:+,.2f}")

                        # Conclusión
                        tolerancia_registros = abs(diff_registros) <= 5
                        tolerancia_rem_total = abs(diff_rem_total) < 1000
                        tolerancia_sac = abs(diff_sac) < 1000

                        if (
                            tolerancia_registros
                            and tolerancia_rem_total
                            and tolerancia_sac
                        ):
                            print(f"\n🎉 ¡SISTEMAS EQUIVALENTES!")
                            print(
                                "✅ Las diferencias están dentro de tolerancias aceptables"
                            )
                        else:
                            print(f"\n⚠️ SISTEMAS PRESENTAN DIFERENCIAS SIGNIFICATIVAS")
                            print("🔧 Se requiere investigación adicional")

                else:
                    print("❌ No se encontraron registros comunes para comparar")
            else:
                print("\n❌ No hay períodos comunes entre las tablas")
        else:
            print("\n❌ Una o ambas tablas están vacías")

    except Exception as e:
        print(f"\n💥 Error: {e}")

    finally:
        db.close()

    print("\n" + "=" * 50)
    print("🔍 Consulta completada")


if __name__ == "__main__":
    main()
