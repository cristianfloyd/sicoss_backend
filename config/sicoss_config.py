from dataclasses import dataclass
from typing import Optional

@dataclass
class SicossConfig:
    """Configuración para el procesamiento de SICOSS"""
    tope_jubilatorio_patronal: float
    tope_jubilatorio_personal: float
    tope_otros_aportes_personales: float
    trunca_tope: bool
    check_lic: bool = False
    check_retro: bool = False
    check_sin_activo: bool = False
    asignacion_familiar: bool = False
    trabajador_convencionado: str = "S"
    
    @property
    def tope_sac_jubilatorio_pers(self) -> float:
        return self.tope_jubilatorio_personal / 2
    
    @property 
    def tope_sac_jubilatorio_patr(self) -> float:
        return self.tope_jubilatorio_patronal / 2
    
    @property
    def tope_sac_jubilatorio_otro_ap(self) -> float:
        return self.tope_otros_aportes_personales / 2
