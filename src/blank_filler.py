import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import Counter, defaultdict

class BlankFieldFiller:
    """
    Rellena campos en blanco en el archivo corregido 
    
    Campos objetivo: structure_owner, structure_type, tx_type
    """
    
    def __init__(self, config, template_file=None):
        """
        Inicializa el rellenador de campos
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Campos que deben ser idénticos por sitio
        self.target_fields = ['structure_owner', 'structure_type', 'tx_type']
        
        # NUEVO: Cache para valores ya encontrados
        self.value_cache = {}  # {(station_id, field): value}
        
        # Cargar template (una sola vez)
        self.df_template = None
        self.template_by_station = None
        
        if template_file:
            try:
                self.logger.info(f"Cargando template: {template_file}")
                template_sheets = pd.read_excel(template_file, sheet_name=None)
                self.df_template = pd.concat(template_sheets.values(), ignore_index=True)
                
                if 'station_id' in self.df_template.columns:
                    self.template_by_station = self.df_template.groupby('station_id')
                    self.logger.info(f"✓ Template cargado: {len(self.df_template)} filas")
                else:
                    self.logger.warning("Columna 'station_id' no encontrada en template")
            except Exception as e:
                self.logger.warning(f"No se pudo cargar template: {e}")
        
        # NUEVO: Physical parameters cargado una sola vez
        self.df_physical = None
        self.physical_by_station = None
        
        # Estadísticas
        self.stats = {
            'total_blanks_found': 0,
            'filled_from_same_site': 0,
            'filled_from_template': 0,
            'filled_from_physical': 0,
            'still_blank': 0
        }
    
    def load_physical_parameters(self, physical_file: str):
        """
        Carga physical parameters UNA SOLA VEZ
        """
        if self.df_physical is not None:
            return  # Ya está cargado
        
        try:
            self.logger.info(f"Cargando physical parameters: {physical_file}")
            physical_sheets = pd.read_excel(physical_file, sheet_name=None)
            self.df_physical = pd.concat(physical_sheets.values(), ignore_index=True)
            
            if 'station_id' in self.df_physical.columns:
                self.physical_by_station = self.df_physical.groupby('station_id')
                self.logger.info(f"✓ Physical parameters cargado: {len(self.df_physical)} filas")
            else:
                self.logger.warning("Columna 'station_id' no encontrada en physical")
        except Exception as e:
            self.logger.warning(f"No se pudo cargar physical parameters: {e}")
    
    def is_blank(self, value) -> bool:
        """
        Verifica si un valor está en blanco
        """
        if pd.isna(value):
            return True
        if value is None:
            return True
        if str(value).strip() == '':
            return True
        if str(value).strip() == '-':
            return True
        return False
    
    def get_most_common_value(self, values: List[Any]) -> Optional[Any]:
        """
        Obtiene el valor más frecuente de una lista
        """
        if not values:
            return None
        
        counter = Counter(values)
        most_common = counter.most_common(1)[0][0]
        return most_common
    
    def find_value_for_station_field(self, station_id: str, field: str, 
                                     all_sheets: Dict) -> Optional[Any]:
        """
        Busca valor para un station_id y field en TODAS las fuentes
        Usa caché para evitar búsquedas repetidas
        
        Args:
            station_id: ID de la estación
            field: Nombre del campo
            all_sheets: Diccionario con todas las hojas del archivo actual
            
        Returns:
            Valor encontrado o None
        """
        # Verificar caché primero
        cache_key = (station_id, field)
        if cache_key in self.value_cache:
            return self.value_cache[cache_key]
        
        value = None
        
        # 1. Buscar en todas las hojas del archivo actual
        all_values = []
        for sheet_name, df_sheet in all_sheets.items():
            if field not in df_sheet.columns:
                continue
            
            mask = df_sheet['station_id'] == station_id
            sheet_values = df_sheet.loc[mask, field]
            
            # Agregar valores no vacíos
            non_blank = [v for v in sheet_values if not self.is_blank(v)]
            all_values.extend(non_blank)
        
        if all_values:
            value = self.get_most_common_value(all_values)
            self.value_cache[cache_key] = value
            return value
        
        # 2. Buscar en template
        if self.template_by_station is not None:
            try:
                station_data = self.template_by_station.get_group(station_id)
                
                if field in station_data.columns:
                    non_blank_values = [v for v in station_data[field] if not self.is_blank(v)]
                    
                    if non_blank_values:
                        value = self.get_most_common_value(non_blank_values)
                        self.value_cache[cache_key] = value
                        return value
            except KeyError:
                pass
        
        # 3. Buscar en physical parameters
        if self.physical_by_station is not None:
            try:
                station_data = self.physical_by_station.get_group(station_id)
                
                if field in station_data.columns:
                    non_blank_values = [v for v in station_data[field] if not self.is_blank(v)]
                    
                    if non_blank_values:
                        value = self.get_most_common_value(non_blank_values)
                        self.value_cache[cache_key] = value
                        return value
            except KeyError:
                pass
        
        # No se encontró valor
        self.value_cache[cache_key] = None
        return None
    
    def fill_blanks_in_sheet(self, sheet_name: str, df_sheet: pd.DataFrame, 
                            all_sheets: Dict) -> pd.DataFrame:
        """
        Rellena campos en blanco en una hoja específica (OPTIMIZADO)
        
        Args:
            sheet_name: Nombre de la hoja
            df_sheet: DataFrame de la hoja
            all_sheets: Diccionario con todas las hojas
            
        Returns:
            DataFrame con campos rellenados
        """
        self.logger.info(f"Procesando hoja: {sheet_name}")
        
        df_filled = df_sheet.copy()
        
        # Verificar columnas necesarias
        if 'station_id' not in df_filled.columns:
            self.logger.warning(f"  ⚠️  Hoja {sheet_name} no tiene columna 'station_id', saltando")
            return df_filled
        
        # OPTIMIZACIÓN: Pre-identificar qué campos tienen blancos por estación
        stations_with_blanks = defaultdict(list)  # {station_id: [fields_with_blanks]}
        
        for field in self.target_fields:
            if field not in df_filled.columns:
                continue
            
            # Identificar estaciones con blancos en este campo
            for station_id in df_filled['station_id'].unique():
                mask = df_filled['station_id'] == station_id
                blank_mask = mask & df_filled[field].apply(self.is_blank)
                
                if blank_mask.any():
                    stations_with_blanks[station_id].append(field)
        
        if not stations_with_blanks:
            self.logger.info(f"  ✓ Hoja {sheet_name}: sin campos en blanco")
            return df_filled
        
        # Procesar solo estaciones con blancos
        fill_count = 0
        
        for station_id, fields_to_fill in stations_with_blanks.items():
            for field in fields_to_fill:
                mask = df_filled['station_id'] == station_id
                blank_mask = mask & df_filled[field].apply(self.is_blank)
                blank_count = blank_mask.sum()
                
                self.stats['total_blanks_found'] += blank_count
                
                # Buscar valor (usa caché automáticamente)
                new_value = self.find_value_for_station_field(station_id, field, all_sheets)
                
                if new_value:
                    df_filled.loc[blank_mask, field] = new_value
                    fill_count += blank_count
                    
                    # Determinar fuente para estadísticas
                    if any(not self.is_blank(v) for df in all_sheets.values() 
                          if field in df.columns 
                          for v in df.loc[df['station_id'] == station_id, field]):
                        self.stats['filled_from_same_site'] += blank_count
                        source = "mismo archivo"
                    elif self.template_by_station is not None:
                        self.stats['filled_from_template'] += blank_count
                        source = "template"
                    else:
                        self.stats['filled_from_physical'] += blank_count
                        source = "physical params"
                    
                    self.logger.info(
                        f"  ✓ {station_id}.{field}: {blank_count} valores → '{new_value}' ({source})"
                    )
                else:
                    self.stats['still_blank'] += blank_count
                    self.logger.warning(
                        f"  ⚠️  {station_id}.{field}: {blank_count} valores sin fuente"
                    )
        
        self.logger.info(f"  ✓ Hoja {sheet_name}: {fill_count} campos rellenados")
        
        return df_filled
    
    def process_file(self, input_file: str, output_file: str, physical_file: str):
        """
        Procesa el archivo completo rellenando campos en blanco (OPTIMIZADO)
        
        Args:
            input_file: Ruta al archivo corregido (con posibles blancos)
            output_file: Ruta donde guardar el archivo completado
            physical_file: Ruta al archivo physical parameters original
        """
        self.logger.info("="*70)
        self.logger.info("RELLENANDO CAMPOS EN BLANCO")
        self.logger.info("="*70)
        
        # Cargar physical parameters UNA SOLA VEZ
        if physical_file:
            self.load_physical_parameters(physical_file)
        
        # Cargar todas las hojas del archivo a procesar
        self.logger.info(f"Cargando archivo: {input_file}")
        all_sheets = pd.read_excel(input_file, sheet_name=None)
        self.logger.info(f"Hojas encontradas: {list(all_sheets.keys())}")
        
        # Procesar cada hoja
        filled_sheets = {}
        
        for sheet_name, df_sheet in all_sheets.items():
            filled_sheets[sheet_name] = self.fill_blanks_in_sheet(
                sheet_name, 
                df_sheet, 
                all_sheets
            )
        
        # Guardar archivo completado
        self.logger.info(f"\nGuardando archivo completado: {output_file}")
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, df_sheet in filled_sheets.items():
                # Actualizar metadatos
                if 'db_modified_by_user' in df_sheet.columns:
                    df_sheet['db_modified_by_user'] = 'blank_field_filler'
                if 'db_modification_datetime' in df_sheet.columns:
                    df_sheet['db_modification_datetime'] = timestamp
                
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
                self.logger.info(f"  ✓ Hoja '{sheet_name}' guardada")
        
        self.logger.info(f"✓ Archivo guardado: {output_file}")
        
        # Mostrar estadísticas
        self.print_statistics()
    
    def print_statistics(self):
        """
        Imprime estadísticas del proceso
        """
        print("\n" + "="*70)
        print("ESTADÍSTICAS DE RELLENADO")
        print("="*70)
        print(f"Campos en blanco encontrados:       {self.stats['total_blanks_found']}")
        print(f"  ✓ Rellenados desde mismo archivo: {self.stats['filled_from_same_site']}")
        print(f"  ✓ Rellenados desde template:      {self.stats['filled_from_template']}")
        print(f"  ✓ Rellenados desde physical:      {self.stats['filled_from_physical']}")
        print(f"  ⚠️  Aún en blanco:                 {self.stats['still_blank']}")
        
        if self.stats['total_blanks_found'] > 0:
            fill_rate = ((self.stats['total_blanks_found'] - self.stats['still_blank']) / 
                        self.stats['total_blanks_found'] * 100)
            print(f"\nTasa de éxito: {fill_rate:.1f}%")
        
        print("="*70)
    
    def generate_blank_report(self, input_file: str, report_file: str):
        """
        Genera un reporte de campos en blanco sin modificar el archivo
        """
        self.logger.info("Generando reporte de campos en blanco...")
        
        all_sheets = pd.read_excel(input_file, sheet_name=None)
        
        blank_records = []
        
        for sheet_name, df_sheet in all_sheets.items():
            if 'station_id' not in df_sheet.columns:
                continue
            
            # OPTIMIZACIÓN: Usar vectorización
            for field in self.target_fields:
                if field not in df_sheet.columns:
                    continue
                
                # Agrupar y contar de una vez
                grouped = df_sheet.groupby('station_id').apply(
                    lambda x: pd.Series({
                        'blank_count': x[field].apply(self.is_blank).sum(),
                        'total_count': len(x)
                    })
                )
                
                for station_id, row in grouped.iterrows():
                    if row['blank_count'] > 0:
                        blank_records.append({
                            'sheet': sheet_name,
                            'station_id': station_id,
                            'field': field,
                            'blank_count': int(row['blank_count']),
                            'total_sectors': int(row['total_count']),
                            'blank_percentage': (row['blank_count'] / row['total_count'] * 100)
                        })
        
        df_report = pd.DataFrame(blank_records)
        
        if len(df_report) > 0:
            df_report = df_report.sort_values(['station_id', 'field'])
            df_report.to_excel(report_file, index=False)
            self.logger.info(f"✓ Reporte de blancos guardado: {report_file}")
            
            print("\n" + "="*70)
            print("REPORTE DE CAMPOS EN BLANCO")
            print("="*70)
            print(f"Total de estaciones con blancos: {df_report['station_id'].nunique()}")
            print(f"Total de campos en blanco: {df_report['blank_count'].sum()}")
            print("\nPor campo:")
            for field in self.target_fields:
                field_blanks = df_report[df_report['field'] == field]['blank_count'].sum()
                if field_blanks > 0:
                    print(f"  - {field}: {field_blanks}")
            print("="*70)
        else:
            self.logger.info("✓ No se encontraron campos en blanco")