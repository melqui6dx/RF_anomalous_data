#!/usr/bin/env python3
"""
Sistema de Corrección Automática de Datos RF
Script Principal de Ejecución
"""

import sys
import os

# Agregar src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.correction_engine import RFDataCorrectionEngine
from src.validators import DataValidator
from src.utils import (
    load_config, 
    setup_logging, 
    create_backup, 
    get_timestamp,
    ensure_directories_exist
)
import pandas as pd

def main():
    """
    Función principal de ejecución
    """
    print("="*70)
    print(" SISTEMA DE CORRECCIÓN AUTOMÁTICA DE DATOS RF")
    print("="*70)
    print()
    
    # 1. Cargar configuración
    print("📋 Cargando configuración...")
    try:
        config = load_config()
        print("   ✓ Configuración cargada correctamente")
    except Exception as e:
        print(f"   ✗ Error cargando configuración: {e}")
        return
    
    # 2. Asegurar que existen los directorios
    print("\n📁 Verificando directorios...")
    try:
        ensure_directories_exist(config)
        print("   ✓ Directorios verificados")
    except Exception as e:
        print(f"   ✗ Error creando directorios: {e}")
        return
    
    # 3. Configurar logging
    print("\n📝 Configurando sistema de logs...")
    try:
        logger = setup_logging(config['output_files']['logs_dir'])
        logger.info("="*70)
        logger.info("INICIO DE EJECUCIÓN")
        logger.info("="*70)
        print("   ✓ Sistema de logs configurado")
    except Exception as e:
        print(f"   ✗ Error configurando logs: {e}")
        return
    
    # 4. Verificar archivos de entrada
    print("\n📂 Verificando archivos de entrada...")
    physical_params_file = config['input_files']['physical_parameters']
    anomalous_file = config['input_files']['anomalous_data']
    
    if not os.path.exists(physical_params_file):
        print(f"   ✗ Error: No se encuentra {physical_params_file}")
        logger.error(f"Archivo no encontrado: {physical_params_file}")
        return
    
    if not os.path.exists(anomalous_file):
        print(f"   ✗ Error: No se encuentra {anomalous_file}")
        logger.error(f"Archivo no encontrado: {anomalous_file}")
        return
    
    print("   ✓ Archivos de entrada encontrados")
    
    # 5. Crear backup
    if config['processing']['create_backup']:
        print("\n💾 Creando backup del archivo original...")
        try:
            backup_path = create_backup(
                physical_params_file, 
                config['output_files']['backups_dir']
            )
            logger.info(f"Backup creado: {backup_path}")
            print(f"   ✓ Backup guardado en: {os.path.basename(backup_path)}")
        except Exception as e:
            print(f"   ✗ Error creando backup: {e}")
            logger.error(f"Error creando backup: {e}")
            return
    

    # 6. Inicializar motor de corrección
    print("\n⚙️  Inicializando motor de corrección...")
    try:
        # NUEVO: Cargar template si está disponible
        template_file = config['input_files'].get('template_reference')
        
        if template_file and os.path.exists(template_file):
            print(f"   📘 Template de referencia encontrado: {os.path.basename(template_file)}")
            engine = RFDataCorrectionEngine(
                physical_params_file, 
                config,
                template_file=template_file  # NUEVO
            )
        else:
            if config['processing']['use_template_as_reference']:
                print(f"   ⚠️  Advertencia: Template no encontrado, continuando sin él")
            engine = RFDataCorrectionEngine(physical_params_file, config)
        
        print("   ✓ Motor inicializado correctamente")
    except Exception as e:
        print(f"   ✗ Error inicializando motor: {e}")
        logger.error(f"Error inicializando motor: {e}")
        return

    
    # 7. Procesar anomalías
    print("\n🔍 Procesando estaciones anómalas...")
    try:
        corrections = engine.process_anomalous_file(
            anomalous_file,
            config['input_files']['anomalous_sheet']
        )
        print(f"   ✓ Procesamiento completado: {len(corrections)} correcciones realizadas")
    except Exception as e:
        print(f"   ✗ Error procesando anomalías: {e}")
        logger.error(f"Error procesando anomalías: {e}")
        return
    
    # 8. Guardar archivo corregido
    print("\n💿 Guardando archivo corregido...")
    timestamp = get_timestamp()
    corrected_file = os.path.join(
        config['output_files']['corrected_data_dir'],
        f'{timestamp}_table_physical_parameters_corrected.xlsx'
    )
    
    try:
        engine.save_corrected_data(corrected_file)
        print(f"   ✓ Archivo guardado: {os.path.basename(corrected_file)}")
    except Exception as e:
        print(f"   ✗ Error guardando archivo: {e}")
        logger.error(f"Error guardando archivo: {e}")
        return
    
    # 9. Generar reporte de correcciones
    print("\n📊 Generando reporte de correcciones...")
    report_file = os.path.join(
        config['output_files']['reports_dir'],
        f'{timestamp}_correction_report.xlsx'
    )
    
    try:
        engine.generate_correction_report(report_file)
        print(f"   ✓ Reporte guardado: {os.path.basename(report_file)}")
    except Exception as e:
        print(f"   ✗ Error generando reporte: {e}")
        logger.error(f"Error generando reporte: {e}")
    
    # 10. Ejecutar validaciones
    print("\n✅ Ejecutando validaciones de calidad...")
    try:
        validator = DataValidator(config)
        
        # Cargar archivos para validación
        df_original = pd.read_excel(physical_params_file)
        df_corrected = pd.read_excel(corrected_file)
        
        validation_report_file = os.path.join(
            config['output_files']['reports_dir'],
            f'{timestamp}_validation_report.xlsx'
        )
        
        validator.generate_validation_report(
            df_original,
            df_corrected,
            validation_report_file
        )
        
        print(f"   ✓ Reporte de validación guardado: {os.path.basename(validation_report_file)}")
        
    except Exception as e:
        print(f"   ⚠️  Advertencia en validaciones: {e}")
        logger.warning(f"Error en validaciones: {e}")

    # Al final de main.py, antes del resumen final:

    # 11. (OPCIONAL) Rellenar campos en blanco
    print("\n🔧 ¿Desea rellenar campos en blanco? (structure_owner, structure_type, tx_type)")
    user_input = input("Responder (s/n): ").strip().lower()
    
    if user_input in ['s', 'y', 'yes', 'si', 'sí']:
        print("\n🔧 Rellenando campos en blanco...")
        try:
            from src.blank_filler import BlankFieldFiller
            
            filler = BlankFieldFiller(config, template_file)
            
            filled_file = os.path.join(
                config['output_files']['corrected_data_dir'],
                f'{timestamp}_table_physical_parameters_complete.xlsx'
            )
            
            filler.process_file(corrected_file, filled_file, physical_params_file)
            
            print(f"   ✓ Archivo completado: {os.path.basename(filled_file)}")
            
        except Exception as e:
            print(f"   ⚠️  Error rellenando campos: {e}")
            logger.warning(f"Error en rellenado de campos: {e}")
    
    # 12. Resumen final
    logger.info("="*70)
    logger.info("FIN DE EJECUCIÓN")
    logger.info("="*70)
    
    print("\n" + "="*70)
    print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("="*70)
    print(f"\nArchivos generados:")
    print(f"  📄 Datos corregidos: {os.path.basename(corrected_file)}")
    print(f"  📊 Reporte correcciones: {os.path.basename(report_file)}")
    print(f"  ✅ Reporte validación: {os.path.basename(validation_report_file)}")
    
    if config['processing']['create_backup']:
        print(f"  💾 Backup: {os.path.basename(backup_path)}")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error fatal: {e}")
        sys.exit(1)