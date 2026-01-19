from dataclasses import dataclass, field
from typing import Optional, List

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
    variantes_vacaciones: Optional[str] = None
    variantes_protecintegral: Optional[str] = None
    categorias_diferenciales: List[str] = field(default_factory=list)
    
    @property
    def tope_sac_jubilatorio_pers(self) -> float:
        return self.tope_jubilatorio_personal / 2
    
    @property 
    def tope_sac_jubilatorio_patr(self) -> float:
        return self.tope_jubilatorio_patronal / 2
    
    @property
    def tope_sac_jubilatorio_otro_ap(self) -> float:
        return self.tope_otros_aportes_personales / 2
