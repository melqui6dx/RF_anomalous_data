import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
import re
import logging

# Importar los nuevos módulos con manejo de errores
try:
    from src.template_manager import TemplateManager
    from src.extended_cell_detector import ExtendedCellDetector
    MODULES_AVAILABLE = True
except ImportError:
    MODULES_AVAILABLE = False
    logging.warning("TemplateManager y/o ExtendedCellDetector no disponibles")

class RFDataCorrectionEngine:
    """
    Motor de corrección automática de datos de sitios RF
    """
    
    def __init__(self, physical_params_file, config, template_file=None):
        """
        Inicializa el motor con el archivo de parámetros físicos

        Args:
            physical_params_file: Ruta al archivo Excel de parámetros físicos
            config: Diccionario con configuración del sistema
            template_file: Ruta al archivo template de referencia (opcional)
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.physical_params_file = physical_params_file

        self.logger.info(f"Cargando archivo: {physical_params_file}")

        # Cargar TODAS las hojas del archivo
        self.all_sheets = pd.read_excel(physical_params_file, sheet_name=None)
        self.logger.info(f"Hojas encontradas: {list(self.all_sheets.keys())}")

        # Combinar todas las hojas en un solo DataFrame
        self.df_physical = pd.concat(self.all_sheets.values(), ignore_index=True)
        self.logger.info(f"Total de filas cargadas: {len(self.df_physical)}")

        self.corrections_log = []
        self.manual_review_required = []
<<<<<<< Updated upstream
=======
        self.extended_cells_detected = []
        
        # Inicializar gestor de template
        self.template_manager = None
        if MODULES_AVAILABLE and template_file and config['processing'].get('use_template_as_reference', False):
            try:
                self.template_manager = TemplateManager(template_file, config)
                if self.template_manager.is_available():
                    self.logger.info("✓ Template Manager inicializado correctamente")
                else:
                    self.logger.warning("Template Manager cargado pero sin datos")
                    self.template_manager = None
            except Exception as e:
                self.logger.warning(f"No se pudo cargar template: {e}")
                self.template_manager = None
        
        # Inicializar detector de extended cells
        self.extended_detector = None
        if MODULES_AVAILABLE and config['processing'].get('detect_extended_cells', False):
            try:
                self.extended_detector = ExtendedCellDetector(config)
                self.logger.info("✓ Extended Cell Detector inicializado correctamente")
            except Exception as e:
                self.logger.warning(f"No se pudo inicializar detector de extended cells: {e}")
>>>>>>> Stashed changes

        # Validación de columnas requeridas
        required_columns = ['station_id', 'name', 'latitude', 'longitude',
                          'structure_height', 'structure_owner', 'structure_type']
        missing_columns = [col for col in required_columns if col not in self.df_physical.columns]

        if missing_columns:
            raise ValueError(f"Columnas faltantes en el archivo: {missing_columns}")
    
    def parse_list_values(self, value_str):
        """
        Convierte strings de listas a listas Python
        
        Args:
            value_str: String con formato de lista o valor único
            
        Returns:
            Lista de valores parseados
        """
        if pd.isna(value_str) or value_str == '[]':
            return []
        
        if isinstance(value_str, str):
            # Intentar parsear como lista
            matches = re.findall(r"'([^']*)'|\"([^\"]*)\"|(-?\d+\.?\d*)", value_str)
            values = [m[0] or m[1] or m[2] for m in matches if any(m)]
            return [v for v in values if v]
        
        return [value_str]
    
    def normalize_owner(self, owner):
        """
        Normaliza nombres de propietarios de estructuras
        
        Args:
            owner: Nombre del propietario
            
        Returns:
            Nombre normalizado en mayúsculas
        """
        if not owner:
            return None
        return owner.upper().strip()
    
    def select_best_latitude(self, lat_list):
        """
        Selecciona la mejor latitud del conjunto usando validación geográfica
        
        Args:
            lat_list: Lista de valores de latitud
            
        Returns:
            Latitud seleccionada o None
        """
        if not lat_list:
            return None
        
        try:
            lats = [float(x) for x in lat_list if x]
        except ValueError:
            self.logger.warning(f"Valores de latitud inválidos: {lat_list}")
            return None
        
        # Validar rango geográfico
        lat_min = self.config['geographic_validation']['latitude_min']
        lat_max = self.config['geographic_validation']['latitude_max']
        valid_lats = [x for x in lats if lat_min <= x <= lat_max]
        
        if not valid_lats:
            self.logger.warning(f"Ninguna latitud dentro del rango válido: {lats}")
            return None
        
        # Si valores muy cercanos, usar mediana
        threshold = self.config['coordinate_threshold']
        if max(valid_lats) - min(valid_lats) < threshold:
            return round(np.median(valid_lats), 6)
        
        # Usar valor más frecuente o mediana
        return round(np.median(valid_lats), 6)
    
    def select_best_longitude(self, lon_list):
        """
        Selecciona la mejor longitud del conjunto usando validación geográfica
        
        Args:
            lon_list: Lista de valores de longitud
            
        Returns:
            Longitud seleccionada o None
        """
        if not lon_list:
            return None
        
        try:
            lons = [float(x) for x in lon_list if x]
        except ValueError:
            self.logger.warning(f"Valores de longitud inválidos: {lon_list}")
            return None
        
        lon_min = self.config['geographic_validation']['longitude_min']
        lon_max = self.config['geographic_validation']['longitude_max']
        valid_lons = [x for x in lons if lon_min <= x <= lon_max]
        
        if not valid_lons:
            self.logger.warning(f"Ninguna longitud dentro del rango válido: {lons}")
            return None
        
        threshold = self.config['coordinate_threshold']
        if max(valid_lons) - min(valid_lons) < threshold:
            return round(np.median(valid_lons), 6)
        
        return round(np.median(valid_lons), 6)
    
    def select_best_name(self, name_list, station_id=None):
        """
        Selecciona el mejor nombre (más completo), con soporte de template
        
        Args:
            name_list: Lista de nombres
            station_id: ID de la estación (para consultar template)
            
        Returns:
            Nombre seleccionado o None
        """
        if not name_list:
            return None
        
        valid_names = [str(n).strip() for n in name_list if n and str(n).strip()]
        
        if not valid_names:
            return None
        
        # Consultar template si está disponible
        if self.template_manager and station_id:
            template_name = self.template_manager.get_reference_name(
                station_id, 
                valid_names
            )
            
            if template_name:
                return template_name
        
        # Algoritmo por defecto: seleccionar el más largo (generalmente más completo)
        return max(valid_names, key=len)
    
    def select_best_structure_height(self, height_list):
        """
        Selecciona la mejor altura de estructura
        
        Args:
            height_list: Lista de alturas
            
        Returns:
            Altura seleccionada o None
        """
        if not height_list:
            return None
        
        try:
            heights = [float(x) for x in height_list if x]
        except ValueError:
            self.logger.warning(f"Valores de altura inválidos: {height_list}")
            return None
        
        height_min = self.config['structure_validation']['height_min']
        height_max = self.config['structure_validation']['height_max']
        valid_heights = [h for h in heights if height_min <= h <= height_max]
        
        if not valid_heights:
            self.logger.warning(f"Ninguna altura dentro del rango válido: {heights}")
            return None
        
        # Retornar altura máxima (altura de la estructura, no de antenas)
        return max(valid_heights)
    
    def select_best_structure_owner(self, owner_list):
        """
        Selecciona el mejor propietario (más frecuente después de normalizar)
        
        Args:
            owner_list: Lista de propietarios
            
        Returns:
            Propietario seleccionado o None
        """
        if not owner_list:
            return None
        
        normalized = [self.normalize_owner(o) for o in owner_list if o]
        
        if not normalized:
            return None
        
        # Retornar el más frecuente
        counter = Counter(normalized)
        return counter.most_common(1)[0][0]
    
    def select_best_structure_type(self, type_list):
        """
        Selecciona el mejor tipo de estructura según prioridad
        
        Args:
            type_list: Lista de tipos de estructura
            
        Returns:
            Tipo seleccionado o None
        """
        if not type_list:
            return None
        
        priority = self.config['structure_type_priority']
        
        valid_types = [str(t).strip() for t in type_list if t and str(t).strip() != '-']
        
        if not valid_types:
            return None
        
        # Seleccionar por prioridad más alta
        return max(valid_types, key=lambda x: priority.get(x, -1))
    
    def calculate_discrepancy_score(self, values_list):
        """
        Calcula un score de discrepancia (0-1, donde 1 = máxima discrepancia)

        Args:
            values_list: Lista de valores

        Returns:
            Score de discrepancia
        """
        if not values_list or len(values_list) <= 1:
            return 0.0

        unique_values = len(set(str(v) for v in values_list if v))
        total_values = len([v for v in values_list if v])

        if total_values == 0:
            return 0.0

        return (unique_values - 1) / total_values

    def detect_extended_cells(self, station_id):
        """
<<<<<<< Updated upstream
        Detecta celdas extendidas en la hoja LTE
=======
        Detecta celdas extendidas en la hoja LTE (método legacy - mantener compatibilidad)
>>>>>>> Stashed changes

        Args:
            station_id: ID de la estación

        Returns:
            Lista de sectores que son celdas extendidas
        """
        extended_cells = []

        # Buscar en la hoja LTE
        if 'lte' in self.all_sheets or 'LTE' in self.all_sheets:
            sheet_name = 'lte' if 'lte' in self.all_sheets else 'LTE'
            df_lte = self.all_sheets[sheet_name]

            # Buscar sectores de esta estación
            mask = df_lte['station_id'] == station_id
            station_sectors = df_lte[mask]

            # Detectar celdas extendidas (pueden tener sufijo _1, _2, etc. o carrier adicional)
            for idx, row in station_sectors.iterrows():
                sector_id = row.get('sector_id', '')
                cell_name = row.get('name', '')

                # Detectar patrones de celdas extendidas
                if '_' in str(sector_id) or 'extended' in str(cell_name).lower():
                    extended_cells.append({
                        'sector_id': sector_id,
                        'name': cell_name,
                        'row_index': idx
                    })

        return extended_cells

    def search_sector_info_all_sheets(self, station_id, sector_id=None, technology=None):
        """
        Busca información del sector en TODAS las hojas, priorizando coincidencia de tecnología

        Args:
            station_id: ID de la estación
            sector_id: ID del sector (opcional)
            technology: Tecnología preferida (lte, umts, gsm) para priorizar resultados

        Returns:
            Diccionario con la información encontrada, priorizando tecnología coincidente
        """
        results_by_priority = {
            'matching_tech': [],  # Coincide tecnología
            'other_tech': []       # Otras tecnologías
        }

        # Buscar en todas las hojas
        for sheet_name, df_sheet in self.all_sheets.items():
            # Filtrar por station_id
            mask = df_sheet['station_id'] == station_id

            # Si tenemos sector_id, filtrar también por eso
            if sector_id and 'sector_id' in df_sheet.columns:
                mask = mask & (df_sheet['sector_id'] == sector_id)

            matches = df_sheet[mask]

            if len(matches) > 0:
                # Determinar si coincide la tecnología
                sheet_tech = sheet_name.lower()
                is_matching_tech = (technology and sheet_tech == technology.lower())

                for idx, row in matches.iterrows():
                    sector_data = {
                        'sheet_name': sheet_name,
                        'technology': sheet_name,
                        'station_id': row.get('station_id'),
                        'sector_id': row.get('sector_id'),
                        'name': row.get('name'),
                        'latitude': row.get('latitude'),
                        'longitude': row.get('longitude'),
                        'structure_height': row.get('structure_height'),
                        'structure_owner': row.get('structure_owner'),
                        'structure_type': row.get('structure_type'),
                        'row_index': idx
                    }

                    if is_matching_tech:
                        results_by_priority['matching_tech'].append(sector_data)
                    else:
                        results_by_priority['other_tech'].append(sector_data)

        # Retornar primero los que coinciden en tecnología, luego los otros
        all_results = results_by_priority['matching_tech'] + results_by_priority['other_tech']

        if all_results:
            self.logger.info(f"Encontrados {len(all_results)} sectores para station_id={station_id}, "
                           f"{len(results_by_priority['matching_tech'])} con tecnología coincidente")

        return all_results

    def complete_blank_fields(self, station_id, current_data, technology=None):
        """
        Completa campos en blanco buscando en todas las hojas

        Args:
            station_id: ID de la estación
            current_data: Diccionario con datos actuales (pueden tener valores en blanco)
            technology: Tecnología para priorizar búsqueda

        Returns:
            Diccionario con campos completados
        """
        # Buscar información en todas las hojas
        sector_info = self.search_sector_info_all_sheets(
            station_id,
            sector_id=current_data.get('sector_id'),
            technology=technology
        )

        if not sector_info:
<<<<<<< Updated upstream
            self.logger.warning(f"No se encontró información adicional para station_id={station_id}")
=======
            self.logger.debug(f"No se encontró información adicional para station_id={station_id}")
>>>>>>> Stashed changes
            return current_data

        completed_data = current_data.copy()
        fields_to_complete = ['name', 'latitude', 'longitude', 'structure_height',
                            'structure_owner', 'structure_type']

        # Completar campos en blanco
        for field in fields_to_complete:
<<<<<<< Updated upstream
            if pd.isna(completed_data.get(field)) or completed_data.get(field) == '' or completed_data.get(field) is None:
=======
            current_val = completed_data.get(field)
            is_empty = pd.isna(current_val) or current_val == '' or current_val is None
            
            if is_empty:
>>>>>>> Stashed changes
                # Buscar en los resultados (ya están priorizados)
                for info in sector_info:
                    if not pd.isna(info.get(field)) and info.get(field) != '':
                        completed_data[field] = info[field]
<<<<<<< Updated upstream
                        self.logger.info(f"Campo '{field}' completado desde hoja '{info['sheet_name']}': {info[field]}")
=======
                        self.logger.info(f"  📋 Campo '{field}' completado desde hoja '{info['sheet_name']}': {info[field]}")
>>>>>>> Stashed changes
                        break

        return completed_data
    
    def process_anomalous_station(self, station_data):
        """
        Procesa una estación anómala y determina valores correctos

        Args:
            station_data: Serie de pandas con datos de la estación anómala

        Returns:
            Diccionario con valores correctos determinados
        """
        station_id = station_data['station_id']
        self.logger.info(f"Procesando estación: {station_id}")
<<<<<<< Updated upstream

        # Detectar celdas extendidas en LTE
        extended_cells = self.detect_extended_cells(station_id)
        if extended_cells:
            self.logger.info(f"Detectadas {len(extended_cells)} celdas extendidas en LTE para {station_id}")
=======
        
        # Detectar sectores extendidos usando el nuevo detector
        extended_cells = []
        if self.extended_detector:
            # Obtener datos de la estación de todas las hojas
            station_df_parts = []
            for sheet_name, df_sheet in self.all_sheets.items():
                mask = df_sheet['station_id'] == station_id
                if mask.any():
                    station_df_parts.append(df_sheet[mask])
            
            if station_df_parts:
                station_df = pd.concat(station_df_parts, ignore_index=True)
                extended_cells = self.extended_detector.detect_extended_cells_in_station(station_df)
                
                if extended_cells:
                    self.logger.info(f"  🔄 {len(extended_cells)} sectores extendidos detectados")
                    
                    # Marcar en todas las hojas
                    for sheet_name, df_sheet in self.all_sheets.items():
                        self.all_sheets[sheet_name] = self.extended_detector.mark_extended_cells(
                            df_sheet,
                            extended_cells
                        )
                    
                    # Registrar para el reporte
                    for cell_id in extended_cells:
                        self.extended_cells_detected.append({
                            'station_id': station_id,
                            'cell_id': cell_id,
                            'action': 'marked_as_extended_cell'
                        })
        else:
            # Fallback al método legacy
            extended_cells_legacy = self.detect_extended_cells(station_id)
            if extended_cells_legacy:
                self.logger.info(f"  🔄 Detectadas {len(extended_cells_legacy)} celdas extendidas (método legacy)")
>>>>>>> Stashed changes

        # Parsear listas
        names = self.parse_list_values(station_data['name'])
        lats = self.parse_list_values(station_data['latitude'])
        lons = self.parse_list_values(station_data['longitude'])
        heights = self.parse_list_values(station_data['structure_height'])
        owners = self.parse_list_values(station_data['structure_owner'])
        types = self.parse_list_values(station_data['structure_type'])

        # Calcular scores de discrepancia
        discrepancy_scores = {
            'latitude': self.calculate_discrepancy_score(lats),
            'longitude': self.calculate_discrepancy_score(lons),
            'name': self.calculate_discrepancy_score(names),
            'structure_height': self.calculate_discrepancy_score(heights),
            'structure_owner': self.calculate_discrepancy_score(owners),
            'structure_type': self.calculate_discrepancy_score(types)
        }

        avg_discrepancy = np.mean(list(discrepancy_scores.values()))

        # Determinar valores correctos
        correct_values = {
            'station_id': station_id,
            'name': self.select_best_name(names, station_id),
            'latitude': self.select_best_latitude(lats) if not extended_cells else None,
            'longitude': self.select_best_longitude(lons) if not extended_cells else None,
            'structure_height': self.select_best_structure_height(heights),
            'structure_owner': self.select_best_structure_owner(owners),
            'structure_type': self.select_best_structure_type(types),
            'discrepancy_score': avg_discrepancy,
            'has_extended_cells': len(extended_cells) > 0
        }

        # Extraer tecnología si está disponible
        technology = station_data.get('technology', None)

<<<<<<< Updated upstream
        # NUEVO: Completar campos en blanco usando búsqueda multi-hoja
        self.logger.info(f"Completando campos en blanco para {station_id}")
        correct_values = self.complete_blank_fields(station_id, correct_values, technology)
=======
        # Completar campos en blanco usando búsqueda multi-hoja
        self.logger.info(f"Completando campos en blanco para {station_id}")
        correct_values = self.complete_blank_fields(station_id, correct_values, technology)
        
        # Rellenar parámetros faltantes desde template
        if self.template_manager:
            correct_values = self.template_manager.fill_missing_parameters(
                station_id,
                correct_values
            )
>>>>>>> Stashed changes

        # Marcar para revisión manual si discrepancia es alta
        threshold = self.config['processing']['require_manual_review_threshold']
        if avg_discrepancy > threshold and not extended_cells:
            self.logger.warning(f"Estación {station_id} requiere revisión manual (score: {avg_discrepancy:.2f})")
            self.manual_review_required.append({
                'station_id': station_id,
                'discrepancy_score': avg_discrepancy,
                'discrepancy_details': discrepancy_scores,
                'original_values': {
                    'names': names,
                    'latitudes': lats,
                    'longitudes': lons,
                    'heights': heights,
                    'owners': owners,
                    'types': types
                },
                'proposed_values': correct_values,
                'extended_cells': extended_cells
            })

        return correct_values
    
    def apply_corrections(self, station_id, correct_values):
        """
        Aplica correcciones al DataFrame de parámetros físicos y a todas las hojas

        Args:
            station_id: ID de la estación
            correct_values: Diccionario con valores correctos

        Returns:
            Lista de correcciones realizadas
        """
        corrections_made = []
        total_rows_affected = 0
<<<<<<< Updated upstream
=======
        
        # No corregir coordenadas si hay extended cells
        skip_coords = correct_values.get('has_extended_cells', False)
>>>>>>> Stashed changes

        # Aplicar correcciones en TODAS las hojas
        for sheet_name, df_sheet in self.all_sheets.items():
            mask = df_sheet['station_id'] == station_id
            affected_rows = df_sheet[mask].index

            if len(affected_rows) == 0:
                continue

            self.logger.info(f"Aplicando correcciones en hoja '{sheet_name}': {len(affected_rows)} filas")

            for param, new_value in correct_values.items():
<<<<<<< Updated upstream
                if param in ['station_id', 'discrepancy_score', 'sector_id'] or new_value is None:
=======
                if param in ['station_id', 'discrepancy_score', 'sector_id', 'has_extended_cells']:
                    continue
                
                # Saltar coordenadas si hay extended cells
                if skip_coords and param in ['latitude', 'longitude']:
                    self.logger.info(
                        f"  ⏭️  Saltando corrección de {param} "
                        f"(estación con extended cells)"
                    )
>>>>>>> Stashed changes
                    continue

                # Verificar que la columna existe en esta hoja
                if param not in df_sheet.columns:
                    continue

                # Obtener valores actuales únicos
                old_values = df_sheet.loc[mask, param].unique().tolist()

                # Aplicar corrección solo si hay cambio
                if len(old_values) > 1 or (len(old_values) > 0 and old_values[0] != new_value):
                    df_sheet.loc[mask, param] = new_value

                    correction_record = {
                        'station_id': station_id,
                        'sheet_name': sheet_name,
                        'parameter': param,
<<<<<<< Updated upstream
                        'old_values': old_values,
                        'new_value': new_value,
                        'rows_affected': len(affected_rows),
                        'timestamp': datetime.now().isoformat()
=======
                        'old_values': str(old_values),
                        'new_value': new_value,
                        'rows_affected': len(affected_rows),
                        'timestamp': datetime.now().isoformat(),
                        'source': 'template' if self.template_manager and param in ['structure_owner', 'structure_type', 'tx_type', 'name'] else 'algorithm'
>>>>>>> Stashed changes
                    }

                    corrections_made.append(correction_record)
                    self.logger.info(f"  ✓ [{sheet_name}] {param}: {old_values} → {new_value}")

            total_rows_affected += len(affected_rows)

        # Actualizar también el DataFrame consolidado
        mask_consolidated = self.df_physical['station_id'] == station_id
        if mask_consolidated.any():
            for param, new_value in correct_values.items():
<<<<<<< Updated upstream
                if param in ['station_id', 'discrepancy_score'] or new_value is None:
=======
                if param in ['station_id', 'discrepancy_score', 'has_extended_cells']:
                    continue
                if skip_coords and param in ['latitude', 'longitude']:
>>>>>>> Stashed changes
                    continue
                if param in self.df_physical.columns:
                    self.df_physical.loc[mask_consolidated, param] = new_value

        if total_rows_affected == 0:
            self.logger.warning(f"Estación {station_id} no encontrada en ninguna hoja")

        return corrections_made
    
    def process_anomalous_file(self, anomalous_file, sheet_name='anomalous_stations_data'):
        """
        Procesa el archivo completo de anomalías
        
        Args:
            anomalous_file: Ruta al archivo de anomalías
            sheet_name: Nombre de la hoja con datos anómalos
            
        Returns:
            Lista de todas las correcciones realizadas
        """
        self.logger.info(f"Cargando archivo de anomalías: {anomalous_file}")
        df_anomalous = pd.read_excel(anomalous_file, sheet_name=sheet_name)
        
        self.logger.info(f"Procesando {len(df_anomalous)} estaciones anómalas...")
        
        all_corrections = []
        
        for idx, row in df_anomalous.iterrows():
            station_id = row['station_id']
            
            try:
                # Determinar valores correctos
                correct_values = self.process_anomalous_station(row)
                
                # Aplicar correcciones
                corrections = self.apply_corrections(station_id, correct_values)
                all_corrections.extend(corrections)
                
            except Exception as e:
                self.logger.error(f"Error procesando {station_id}: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
                continue
        
        self.corrections_log = all_corrections
        self.logger.info(f"Proceso completado: {len(all_corrections)} correcciones realizadas")
        
        return all_corrections
    
    def save_corrected_data(self, output_file):
        """
        Guarda el archivo corregido con metadatos actualizados en TODAS las hojas

        Args:
            output_file: Ruta donde guardar el archivo corregido
        """
        # Actualizar campos de modificación en todas las hojas
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, df_sheet in self.all_sheets.items():
                # Actualizar metadatos si las columnas existen
                if 'db_modified_by_user' in df_sheet.columns:
                    df_sheet['db_modified_by_user'] = self.config['system_user']
                if 'db_modification_datetime' in df_sheet.columns:
                    df_sheet['db_modification_datetime'] = timestamp

                # Guardar hoja
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
                self.logger.info(f"✓ Hoja '{sheet_name}' guardada: {len(df_sheet)} filas")

        self.logger.info(f"✓ Archivo corregido guardado con {len(self.all_sheets)} hojas: {output_file}")
    
    def generate_correction_report(self, report_file):
        """
        Genera reporte detallado de correcciones en Excel
        
        Args:
            report_file: Ruta donde guardar el reporte
        """
        df_corrections = pd.DataFrame(self.corrections_log)
        
        # Crear resumen estadístico
        if len(df_corrections) > 0:
            summary = {
                'total_stations': df_corrections['station_id'].nunique(),
                'total_corrections': len(df_corrections),
                'total_rows_affected': df_corrections['rows_affected'].sum(),
                'corrections_from_template': (df_corrections.get('source', pd.Series(['algorithm'])) == 'template').sum(),
                'corrections_from_algorithm': (df_corrections.get('source', pd.Series(['algorithm'])) == 'algorithm').sum(),
                'extended_cells_detected': len(self.extended_cells_detected),
                'execution_time': datetime.now().isoformat()
            }
        else:
            summary = {
                'total_stations': 0,
                'total_corrections': 0,
                'total_rows_affected': 0,
                'corrections_from_template': 0,
                'corrections_from_algorithm': 0,
                'extended_cells_detected': len(self.extended_cells_detected),
                'execution_time': datetime.now().isoformat()
            }
        
        # Crear DataFrames adicionales
        df_manual_review = pd.DataFrame(self.manual_review_required) if self.manual_review_required else pd.DataFrame()
        df_extended_cells = pd.DataFrame(self.extended_cells_detected) if self.extended_cells_detected else pd.DataFrame()
        
        # Guardar en Excel con múltiples hojas
        with pd.ExcelWriter(report_file, engine='xlsxwriter') as writer:
            # Hoja 1: Resumen
            pd.DataFrame([summary]).to_excel(writer, sheet_name='summary', index=False)
            
            # Hoja 2: Correcciones detalladas
            if len(df_corrections) > 0:
                df_corrections.to_excel(writer, sheet_name='detailed_corrections', index=False)
            
            # Hoja 3: Requiere revisión manual
            if len(df_manual_review) > 0:
                df_manual_review.to_excel(writer, sheet_name='manual_review_required', index=False)
            
            # Hoja 4: Estadísticas por parámetro
            if len(df_corrections) > 0:
                param_stats = df_corrections.groupby('parameter').agg({
                    'station_id': 'nunique',
                    'rows_affected': 'sum'
                }).rename(columns={'station_id': 'stations_affected'})
                param_stats.to_excel(writer, sheet_name='parameter_statistics')
            
            # Hoja 5: Extended Cells
            if len(df_extended_cells) > 0:
                df_extended_cells.to_excel(writer, sheet_name='extended_cells', index=False)
        
        self.logger.info(f"✓ Reporte de correcciones guardado: {report_file}")
        
        # Imprimir resumen en consola
        print("\n" + "="*70)
        print("RESUMEN DE CORRECCIONES")
        print("="*70)
        print(f"Estaciones corregidas:        {summary['total_stations']}")
        print(f"Total de correcciones:        {summary['total_corrections']}")
        print(f"  - Desde template:           {summary['corrections_from_template']}")
        print(f"  - Desde algoritmo:          {summary['corrections_from_algorithm']}")
        print(f"Filas afectadas:              {summary['total_rows_affected']}")
        print(f"Extended Cells detectados:    {summary['extended_cells_detected']}")
        
        if self.manual_review_required:
            print(f"\n⚠️  ATENCIÓN: {len(self.manual_review_required)} estaciones requieren revisión manual")
            print("   Ver hoja 'manual_review_required' en el reporte")
        
        print("="*70)