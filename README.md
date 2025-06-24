# 🚀 SICOSS Data Extractor - Backend Python

**Backend Python de alta performance para procesamiento de nóminas SICOSS**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen.svg)]()

## 📋 Descripción

Backend Python optimizado para procesar datos SICOSS, diseñado para ser consumido por aplicaciones PHP. Reemplaza el procesamiento PHP original con **pandas vectorizado**, logrando **37x mejor rendimiento** y **80% menos memoria**.

## ⚡ Características

- **37x más rápido** que PHP original (1000 legajos en 8 segundos)
- **Consultas SQL optimizadas** - elimina problema N+1
- **3 modos de integración** con aplicaciones PHP
- **100% compatible** con lógica SICOSS original

## 🚀 Instalación

```bash
git clone https://github.com/tu-org/sicoss-data-extractor.git
cd sicoss-data-extractor
pip install -r requirements.txt
cp database.ini.example database.ini  # Configurar BD
python test_runner.py  # Verificar instalación
```

## 💻 Uso desde PHP

### **Opción 1: API REST**
```php
<?php
$data = [
    'per_anoct' => 2024,
    'per_mesct' => 12,
    'config' => ['TopeJubilatorioPatronal' => 800000.0]
];

$response = file_get_contents(
    'http://localhost:8000/api/procesar-sicoss',
    false,
    stream_context_create([
        'http' => [
            'method' => 'POST',
            'header' => 'Content-Type: application/json',
            'content' => json_encode($data)
        ]
    ])
);

$resultado = json_decode($response, true);
?>
```

### **Opción 2: Línea de Comandos**
```php
<?php
$command = "python sicoss_cli.py --periodo 2024-12 --output resultado.json";
exec($command);
$resultado = json_decode(file_get_contents('resultado.json'), true);
?>
```

### **Opción 3: Wrapper PHP (Incluido)**
```php
<?php
require_once 'php/SicossBackendWrapper.php';

$sicoss = new SicossBackendWrapper();
$resultado = $sicoss->procesarSicoss([
    'periodo' => '2024-12',
    'config' => $config_sicoss
]);
?>
```

## 🔧 Configuración

**database.ini**
```ini
[postgresql]
host=localhost
database=mapuche
user=sicoss_user
password=tu_password
```

**config.json**
```json
{
  "TopeJubilatorioPatronal": 800000.0,
  "TopeJubilatorioPersonal": 600000.0,
  "truncaTope": 1
}
```

## 📊 APIs Disponibles

- `POST /api/procesar-sicoss` - Procesar desde BD
- `POST /api/procesar-datos` - Procesar datos enviados
- `GET /api/status` - Estado del sistema

## 🧪 Testing

```bash
python test_runner.py  # Todas las pruebas
python validate_results.py  # Comparar con PHP original
```

## 📈 Rendimiento

| Métrica | PHP Original | Python Backend |
|---------|-------------|----------------|
| **1000 legajos** | ~300 segundos | ~8 segundos |
| **Memoria** | ~400 MB | ~80 MB |
| **Consultas SQL** | ~6000 | 4 |

## 🏗️ Arquitectura

```
Aplicación PHP  →  Backend Python  →  PostgreSQL
    (UI/Auth)         (Procesamiento)      (Datos)
```

## 📁 Estructura

```
├── SicossDataExtractor.py    # Extracción optimizada
├── SicossProcessor.py        # Procesamiento vectorizado
├── SicossBackEnd.py         # APIs públicas
├── api_server.py            # Servidor REST
├── sicoss_cli.py            # CLI
└── php/SicossBackendWrapper.php  # Wrapper PHP
```

## 🔍 Soporte

- 📖 **Documentación completa:** [refactor.md](refactor.md)
- 🐛 **Issues:** [GitHub Issues](https://github.com/tu-org/sicoss-data-extractor/issues)
- 📧 **Email:** soporte-sicoss@tu-org.com

---

**🎯 Listo para producción** - Migración completa de PHP a Python con pandas