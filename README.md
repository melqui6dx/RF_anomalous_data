# Sistema de Corrección Automática de Datos RF

Sistema automatizado para corrección de inconsistencias en datos de sitios radio base.

## Instalación Rápida
```bash
# 1. Clonar o descargar el proyecto
cd rf_data_correction

# 2. Crear entorno virtual
python -m venv venv

# 3. Activar entorno virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 4. Instalar dependencias
pip install -r requirements.txt
```

## Uso

1. Colocar archivos de entrada en `data/input/`:
   - `table_physical_parameters.xlsx`
   - `[date]_table_cell_monitoring.xlsx`

2. Ejecutar:
```bash
python main.py
```

3. Revisar resultados en `data/output/`:
   - `corrected/` - Archivo corregido
   - `reports/` - Reportes de correcciones y validación
   - `backups/` - Backup del archivo original

## Configuración

Editar `config/settings.yaml` para ajustar:
- Rangos geográficos válidos
- Umbrales de validación
- Rutas de archivos
- Opciones de procesamiento

## Soporte

Para problemas o dudas, revisar:
1. Logs en carpeta `logs/`
2. Hoja "manual_review_required" en reportes
3. Documentación en código fuente

## Versión

1.0.0