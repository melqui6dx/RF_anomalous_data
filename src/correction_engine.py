import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
import re
import logging

class RFDataCorrectionEngine:
    """
    Motor de corrección automática de datos de sitios RF
    """
    
    def __init__(self, physical_params_file, config):
        """
        Inicializa el motor con el archivo de parámetros físicos
        
        Args:
            physical_params_file: Ruta al archivo Excel de parámetros físicos
            config: Diccionario con configuración del sistema
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        self.logger.info(f"Cargando archivo: {physical_params_file}")
        self.df_physical = pd.read_excel(physical_params_file)
        self.logger.info(f"Archivo cargado: {len(self.df_physical)} filas")
        
        self.corrections_log = []
        self.manual_review_required = []
        
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
    
    def select_best_name(self, name_list):
        """
        Selecciona el mejor nombre (más completo)
        
        Args:
            name_list: Lista de nombres
            
        Returns:
            Nombre seleccionado o None
        """
        if not name_list:
            return None
        
        valid_names = [str(n).strip() for n in name_list if n and str(n).strip()]
        
        if not valid_names:
            return None
        
        # Seleccionar el más largo (generalmente más completo)
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
            'name': self.select_best_name(names),
            'latitude': self.select_best_latitude(lats),
            'longitude': self.select_best_longitude(lons),
            'structure_height': self.select_best_structure_height(heights),
            'structure_owner': self.select_best_structure_owner(owners),
            'structure_type': self.select_best_structure_type(types),
            'discrepancy_score': avg_discrepancy
        }
        
        # Marcar para revisión manual si discrepancia es alta
        threshold = self.config['processing']['require_manual_review_threshold']
        if avg_discrepancy > threshold:
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
                'proposed_values': correct_values
            })
        
        return correct_values
    
    def apply_corrections(self, station_id, correct_values):
        """
        Aplica correcciones al DataFrame de parámetros físicos
        
        Args:
            station_id: ID de la estación
            correct_values: Diccionario con valores correctos
            
        Returns:
            Lista de correcciones realizadas
        """
        mask = self.df_physical['station_id'] == station_id
        affected_rows = self.df_physical[mask].index
        
        if len(affected_rows) == 0:
            self.logger.warning(f"Estación {station_id} no encontrada en archivo físico")
            return []
        
        corrections_made = []
        
        for param, new_value in correct_values.items():
            if param in ['station_id', 'discrepancy_score'] or new_value is None:
                continue
            
            # Obtener valores actuales únicos
            old_values = self.df_physical.loc[mask, param].unique().tolist()
            
            # Aplicar corrección solo si hay cambio
            if len(old_values) > 1 or old_values[0] != new_value:
                self.df_physical.loc[mask, param] = new_value
                
                correction_record = {
                    'station_id': station_id,
                    'parameter': param,
                    'old_values': old_values,
                    'new_value': new_value,
                    'rows_affected': len(affected_rows),
                    'timestamp': datetime.now().isoformat()
                }
                
                corrections_made.append(correction_record)
                self.logger.info(f"  ✓ {param}: {old_values} → {new_value}")
        
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
                continue
        
        self.corrections_log = all_corrections
        self.logger.info(f"Proceso completado: {len(all_corrections)} correcciones realizadas")
        
        return all_corrections
    
    def save_corrected_data(self, output_file):
        """
        Guarda el archivo corregido con metadatos actualizados
        
        Args:
            output_file: Ruta donde guardar el archivo corregido
        """
        # Actualizar campos de modificación
        self.df_physical['db_modified_by_user'] = self.config['system_user']
        self.df_physical['db_modification_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        self.df_physical.to_excel(output_file, index=False, engine='openpyxl')
        self.logger.info(f"✓ Archivo corregido guardado: {output_file}")
    
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
                'parameters_corrected': df_corrections.groupby('parameter').size().to_dict(),
                'execution_time': datetime.now().isoformat()
            }
        else:
            summary = {
                'total_stations': 0,
                'total_corrections': 0,
                'total_rows_affected': 0,
                'parameters_corrected': {},
                'execution_time': datetime.now().isoformat()
            }
        
        # Crear DataFrame de revisión manual
        df_manual_review = pd.DataFrame(self.manual_review_required) if self.manual_review_required else pd.DataFrame()
        
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
        
        self.logger.info(f"✓ Reporte de correcciones guardado: {report_file}")
        
        # Imprimir resumen en consola
        print("\n" + "="*60)
        print("RESUMEN DE CORRECCIONES")
        print("="*60)
        print(f"Estaciones corregidas: {summary['total_stations']}")
        print(f"Total de correcciones: {summary['total_corrections']}")
        print(f"Filas afectadas: {summary['total_rows_affected']}")
        
        if self.manual_review_required:
            print(f"\n⚠️  ATENCIÓN: {len(self.manual_review_required)} estaciones requieren revisión manual")
            print("   Ver hoja 'manual_review_required' en el reporte")
        
        print("="*60)