"""
Módulo de Processors para SICOSS

Contiene clases especializadas para procesar datos:
- BaseProcessor: Clase base abstracta
- ConceptosProcessor: Procesador de conceptos
- CalculosSicossProcessor: Procesador de cálculos
- TopesProcessor: Procesador de topes
- LegajosValidator: Validador de legajos
- SicossDataProcessor: Coordinador principal
"""

from .base_processor import BaseProcessor
from .conceptos_processor import ConceptosProcessor
from .calculos_processor import CalculosSicossProcessor
from .topes_processor import TopesProcessor
from .validator import LegajosValidator
from .sicoss_processor import SicossDataProcessor

__all__ = [
    'BaseProcessor',
    'ConceptosProcessor',
    'CalculosSicossProcessor', 
    'TopesProcessor',
    'LegajosValidator',
    'SicossDataProcessor'
] 