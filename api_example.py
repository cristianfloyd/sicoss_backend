#!/usr/bin/env python3
"""
api_example.py

Ejemplo de FastAPI para integrar SICOSS Backend
Para ser consumido desde Laravel PHP

INSTRUCCIONES:
1. pip install fastapi uvicorn
2. uvicorn api_example:app --reload
3. Acceder a http://localhost:8000/docs para Swagger UI
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import logging
import asyncio
import pandas as pd

# Imports del backend SICOSS
from config.sicoss_config import SicossConfig
from database.database_connection import DatabaseConnection
from extractors.data_extractor_manager import DataExtractorManager
from processors.sicoss_processor import SicossDataProcessor
from value_objects.periodo_fiscal import PeriodoFiscal

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title="SICOSS Backend API",
    description="API para procesamiento de nóminas SICOSS - Backend Python",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS para Laravel
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:8000", "*"],  # Ajustar según Laravel
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelos Pydantic para requests
class SicossProcessRequest(BaseModel):
    """Request para procesamiento SICOSS"""
    periodo_fiscal: str  # Formato YYYYMM
    legajo_especifico: Optional[int] = None
    formato_respuesta: str = "completo"  # completo, resumen, solo_totales
    incluir_inactivos: bool = False
    guardar_en_bd: bool = False
    config_topes: Optional[Dict[str, float]] = None

class SicossProcessResponse(BaseModel):
    """Response del procesamiento SICOSS"""
    success: bool
    message: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str

# Estado global de la aplicación
app_state = {
    "sicoss_config": None,
    "database_connection": None,
    "processing_count": 0
}

@app.on_event("startup")
async def startup_event():
    """Inicialización de la aplicación"""
    logger.info("🚀 Iniciando SICOSS Backend API...")
    
    # Configuración por defecto
    app_state["sicoss_config"] = SicossConfig(
        tope_jubilatorio_patronal=800000.0,
        tope_jubilatorio_personal=600000.0,
        tope_otros_aportes_personales=700000.0,
        trunca_tope=True
    )
    
    # Conexión a base de datos
    try:
        app_state["database_connection"] = DatabaseConnection()
        logger.info("✅ Conexión a base de datos inicializada")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo conectar a BD: {e}")
        app_state["database_connection"] = None
    
    logger.info("✅ SICOSS Backend API inicializada correctamente")

@app.get("/")
async def root():
    """Endpoint raíz - información de la API"""
    return {
        "api": "SICOSS Backend",
        "version": "1.0.0",
        "status": "operational",
        "backend": "python",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
async def health_check():
    """Health check para monitoreo"""
    return {
        "status": "healthy",
        "database": "connected" if app_state["database_connection"] else "disconnected",
        "processing_count": app_state["processing_count"],
        "uptime": "running"
    }

@app.post("/sicoss/process", response_model=SicossProcessResponse)
async def process_sicoss(request: SicossProcessRequest, background_tasks: BackgroundTasks):
    """
    Endpoint principal para procesamiento SICOSS
    
    Procesa nóminas SICOSS y retorna resultados estructurados
    para consumo desde Laravel PHP
    """
    logger.info(f"🔄 Procesando SICOSS período: {request.periodo_fiscal}")
    
    try:
        # Validar período fiscal
        try:
            periodo_fiscal = PeriodoFiscal.from_string(request.periodo_fiscal)
        except Exception as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Período fiscal inválido: {request.periodo_fiscal}"
            )
        
        # Configuración personalizada si se proporciona
        config = app_state["sicoss_config"]
        if request.config_topes:
            config = SicossConfig(
                tope_jubilatorio_patronal=request.config_topes.get(
                    'tope_jubilatorio_patronal', config.tope_jubilatorio_patronal
                ),
                tope_jubilatorio_personal=request.config_topes.get(
                    'tope_jubilatorio_personal', config.tope_jubilatorio_personal
                ),
                tope_otros_aportes_personales=request.config_topes.get(
                    'tope_otros_aportes_personales', config.tope_otros_aportes_personales
                ),
                trunca_tope=request.config_topes.get('trunca_tope', config.trunca_tope)
            )
        
        # Verificar conexión BD
        db_connection = app_state["database_connection"]
        if not db_connection:
            raise HTTPException(
                status_code=503, 
                detail="Base de datos no disponible"
            )
        
        # Extracción de datos
        extractor_manager = DataExtractorManager(db_connection)
        datos_extraidos = extractor_manager.extraer_datos_completos(
            config=config,
            per_anoct=periodo_fiscal.year,
            per_mesct=periodo_fiscal.month,
            nro_legajo=request.legajo_especifico
        )
        
        # Verificar que hay datos
        if datos_extraidos['legajos'].empty:
            return SicossProcessResponse(
                success=False,
                message=f"No se encontraron datos para el período {request.periodo_fiscal}",
                data={},
                metadata={"periodo": request.periodo_fiscal, "legajos_encontrados": 0},
                timestamp=pd.Timestamp.now().isoformat()
            )
        
        # Procesamiento SICOSS
        sicoss_processor = SicossDataProcessor(config)
        resultado_procesamiento = sicoss_processor.procesar_datos_extraidos(
            datos=datos_extraidos,
            validate_input=True,
            guardar_en_bd=request.guardar_en_bd,
            formato_respuesta=request.formato_respuesta,
            periodo_fiscal=periodo_fiscal
        )
        
        # Generar respuesta API
        respuesta_api = sicoss_processor.generar_respuesta_api(
            resultado_procesamiento, 
            formato="fastapi"
        )
        
        # Incrementar contador
        app_state["processing_count"] += 1
        
        # Background task para logging
        background_tasks.add_task(
            log_processing_stats, 
            periodo_fiscal.periodo_str, 
            len(datos_extraidos['legajos'])
        )
        
        logger.info(f"✅ Procesamiento SICOSS completado: {request.periodo_fiscal}")
        
        return SicossProcessResponse(**respuesta_api)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error procesando SICOSS: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error interno del servidor: {str(e)}"
        )

@app.post("/sicoss/process-sample")
async def process_sicoss_sample():
    """
    Endpoint para procesar datos de muestra
    Útil para testing y demostración
    """
    logger.info("🧪 Procesando datos de muestra SICOSS...")
    
    try:
        # Datos simulados para demostración
        config = app_state["sicoss_config"]
        sicoss_processor = SicossDataProcessor(config)
        
        # Datos mínimos simulados
        datos_simulados = {
            'legajos': pd.DataFrame({
                'nro_legaj': [12345, 67890],
                'apyno': ['EMPLEADO DEMO A', 'EMPLEADO DEMO B'],
                'cuit': ['20123456789', '20987654321']
            }),
            'conceptos': pd.DataFrame({
                'nro_legaj': [12345, 67890],
                'codn_conce': [100, 100],
                'impp_conce': [150000.0, 200000.0],
                'tipos_grupos': [[1], [1]]
            }),
            'otra_actividad': pd.DataFrame(),
            'obra_social': pd.DataFrame()
        }
        
        # Procesamiento
        resultado = sicoss_processor.procesar_datos_extraidos(
            datos=datos_simulados,
            formato_respuesta="completo"
        )
        
        # Respuesta API
        respuesta_api = sicoss_processor.generar_respuesta_api(resultado, "fastapi")
        
        return SicossProcessResponse(**respuesta_api)
        
    except Exception as e:
        logger.error(f"❌ Error procesando muestra: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error procesando muestra: {str(e)}"
        )

@app.get("/sicoss/config")
async def get_config():
    """Obtener configuración actual del sistema"""
    config = app_state["sicoss_config"]
    return {
        "tope_jubilatorio_patronal": config.tope_jubilatorio_patronal,
        "tope_jubilatorio_personal": config.tope_jubilatorio_personal,
        "tope_otros_aportes_personales": config.tope_otros_aportes_personales,
        "trunca_tope": config.trunca_tope
    }

@app.put("/sicoss/config")
async def update_config(config_data: Dict[str, Any]):
    """Actualizar configuración del sistema"""
    try:
        app_state["sicoss_config"] = SicossConfig(**config_data)
        return {"message": "Configuración actualizada exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Configuración inválida: {e}")

async def log_processing_stats(periodo: str, legajos_count: int):
    """Background task para logging de estadísticas"""
    logger.info(f"📊 Stats - Período: {periodo}, Legajos: {legajos_count}")

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Iniciando SICOSS Backend API Server...")
    print("📋 Endpoints disponibles:")
    print("   - GET  /         : Información de la API")
    print("   - GET  /health   : Health check")
    print("   - POST /sicoss/process : Procesamiento principal")
    print("   - POST /sicoss/process-sample : Datos de muestra")
    print("   - GET  /sicoss/config : Configuración actual")
    print("   - PUT  /sicoss/config : Actualizar configuración")
    print("   - GET  /docs     : Swagger UI")
    print("🌐 Acceder a: http://localhost:8000")
    
    uvicorn.run("api_example:app", host="0.0.0.0", port=8000, reload=True) 