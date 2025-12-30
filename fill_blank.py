#!/usr/bin/env python3
"""
Script para rellenar campos en blanco en el archivo corregido
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.blank_filler import BlankFieldFiller
from src.utils import load_config, setup_logging, get_timestamp
import argparse

def main():
    """
    Función principal
    """
    parser = argparse.ArgumentParser(
        description='Rellena campos en blanco (structure_owner, structure_type, tx_type)'
    )
    
    parser.add_argument(
        '--input',
        '-i',
        required=True,
        help='Archivo Excel corregido (con posibles blancos)'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        help='Archivo de salida (por defecto: agrega _filled al nombre de entrada)'
    )
    
    parser.add_argument(
        '--report-only',
        '-r',
        action='store_true',
        help='Solo generar reporte sin modificar archivo'
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("RELLENADOR DE CAMPOS EN BLANCO")
    print("="*70)
    print()
    
    # Cargar configuración
    print("📋 Cargando configuración...")
    try:
        config = load_config()
        print("   ✓ Configuración cargada")
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return 1
    
    # Configurar logging
    logger = setup_logging(config['output_files']['logs_dir'])
    logger.info("Iniciando proceso de rellenado de campos en blanco")
    
    # Determinar archivo de salida
    if not args.output:
        base_name = os.path.splitext(args.input)[0]
        args.output = f"{base_name}_filled.xlsx"
    
    # Obtener archivos necesarios
    physical_file = config['input_files']['physical_parameters']
    template_file = config['input_files'].get('template_reference')
    
    # Verificar que existan
    if not os.path.exists(args.input):
        print(f"❌ Error: Archivo no encontrado: {args.input}")
        return 1
    
    if not os.path.exists(physical_file):
        print(f"⚠️  Advertencia: Physical parameters no encontrado: {physical_file}")
        physical_file = None
    
    # Inicializar filler
    print("\n⚙️  Inicializando rellenador...")
    filler = BlankFieldFiller(config, template_file)
    print("   ✓ Rellenador inicializado")
    
    if args.report_only:
        # Solo generar reporte
        print("\n📊 Generando reporte de campos en blanco...")
        timestamp = get_timestamp()
        report_file = os.path.join(
            config['output_files']['reports_dir'],
            f'{timestamp}_blank_fields_report.xlsx'
        )
        filler.generate_blank_report(args.input, report_file)
    else:
        # Procesar y rellenar
        print("\n🔧 Procesando archivo...")
        filler.process_file(args.input, args.output, physical_file)
        
        print("\n" + "="*70)
        print("✅ PROCESO COMPLETADO")
        print("="*70)
        print(f"\nArchivo generado: {args.output}")
        print("="*70)
    
    logger.info("Proceso de rellenado completado")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)