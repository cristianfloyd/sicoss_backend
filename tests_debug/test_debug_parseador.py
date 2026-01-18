#!/usr/bin/env python3
"""
test_debug_parseador.py - Debug del parseador de tipos_grupos
"""

import pandas as pd


def test_parseador():
    """Prueba el parseador de tipos_grupos"""

    # Simular datos reales
    tipos_grupos_ejemplos = [
        [4, 29, 89],
        [4, 25, 89],
        [4, 30, 89],
        [4, 6, 30, 89],
        [4, 33, 68],
        [4, 5],
        [4, 58],
        [4, 77],
        [4],
    ]

    print("🔍 Testing parseador de tipos_grupos")
    print("=" * 50)

    # Función parseador (corregida)
    def parse_tipos_grupos(tipos):
        try:
            if tipos is None:
                return []

            # Si es una lista de Python (caso más común)
            if isinstance(tipos, list):
                if len(tipos) == 0:
                    return []
                # Usar verificación simple para enteros en listas
                result = []
                for t in tipos:
                    if t is not None and isinstance(t, (int, float)):
                        result.append(int(t))
                return result

            # Si es string vacío o array vacío
            if tipos in ["{}", "", "[]"]:
                return []

            if isinstance(tipos, str):
                # Formato "{1,2,3}" -> [1,2,3]
                tipos_clean = tipos.strip("{}[]").replace(" ", "")
                if not tipos_clean:
                    return []
                parts = tipos_clean.split(",")
                return [int(t.strip()) for t in parts if t.strip().isdigit()]

            # Si es un número directamente
            if isinstance(tipos, (int, float)):
                return [int(tipos)]

            return []
        except (ValueError, AttributeError, TypeError) as e:
            print(f"❌ Error parseando {tipos}: {e}")
            return []

    # Probar parseador
    for i, tipos in enumerate(tipos_grupos_ejemplos):
        resultado = parse_tipos_grupos(tipos)
        print(f"{i + 1:2d}. {tipos} → {resultado}")

        if resultado != tipos:
            print(f"    ⚠️  DIFERENCIA: esperado {tipos}, obtenido {resultado}")
        else:
            print(f"    ✅ OK")

    print("\n" + "=" * 50)

    # Crear DataFrame de prueba
    print("\n🧪 Probando con DataFrame:")
    df_test = pd.DataFrame(
        {
            "nro_legaj": [110830] * len(tipos_grupos_ejemplos),
            "impp_conce": [1000.0] * len(tipos_grupos_ejemplos),
            "codn_conce": [101] * len(tipos_grupos_ejemplos),
            "codigoescalafon": ["NODO"] * len(tipos_grupos_ejemplos),
            "tipos_grupos": tipos_grupos_ejemplos,
        }
    )

    print("Datos originales:")
    print(df_test[["tipos_grupos"]].head())

    # Aplicar parseador
    df_test["tipos_grupos_parsed"] = df_test["tipos_grupos"].apply(parse_tipos_grupos)

    print("\nDatos parseados:")
    print(df_test[["tipos_grupos", "tipos_grupos_parsed"]].head())

    # Explotar
    df_exploded = df_test.explode("tipos_grupos_parsed")
    print(f"\nFilas después de explode: {len(df_exploded)}")

    # Filtrar válidos
    mask_valid = (df_exploded["tipos_grupos_parsed"].notna()) & (
        df_exploded["tipos_grupos_parsed"] != ""
    )
    df_valid = df_exploded[mask_valid].copy()
    print(f"Filas válidas: {len(df_valid)}")

    if not df_valid.empty:
        df_valid["tipo_grupo"] = pd.to_numeric(
            df_valid["tipos_grupos_parsed"], errors="coerce"
        )
        df_valid = df_valid[pd.notna(df_valid["tipo_grupo"])].copy()

        print(f"Filas con tipo_grupo válido: {len(df_valid)}")
        print("\nTipos únicos encontrados:")
        print(sorted(df_valid["tipo_grupo"].unique()))


if __name__ == "__main__":
    test_parseador()
