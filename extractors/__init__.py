"""
Módulo de Extractors para SICOSS

Contiene clases especializadas para extraer datos de la base de datos:
- BaseExtractor: Clase base abstracta
- LegajosExtractor: Extractor de datos de legajos
- ConceptosExtractor: Extractor de conceptos liquidados
- DataExtractorManager: Manager coordinador
"""

from .base_extractor import BaseExtractor
from .legajos_extractor import LegajosExtractor
from .conceptos_extractor import ConceptosExtractor
from .data_extractor_manager import DataExtractorManager

__all__ = [
    'BaseExtractor',
    'LegajosExtractor', 
    'ConceptosExtractor',
    'DataExtractorManager'
] 