"""
Configuración compartida para todos los tests
"""

import sys
from pathlib import Path

import pytest

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))


# Fixtures compartidas
@pytest.fixture
def sample_config():
    """Configuración SICOSS de ejemplo"""
    from config.sicoss_config import SicossConfig

    return SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=400000.0,
        trunca_tope=True,
    )


@pytest.fixture
def db_connection():
    """Conexión a base de datos para tests"""
    from database.database_connection import DatabaseConnection

    return DatabaseConnection()
