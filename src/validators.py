import pandas as pd
import numpy as np
import logging

class DataValidator:
    """
    Validador de calidad de datos RF
    """
    
    def __init__(self, config):
        """
        Inicializa el validador
        
        Args:
            config: Diccionario de configuración
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_consistency(self, df, station_id_col='station_id'):
        """
        Valida consistencia de parámetros por estación
        
        Args:
            df: DataFrame a validar
            station_id_col: Nombre de la columna de ID de estación
            
        Returns:
            DataFrame con resultados de validación
        """
        self.logger.info("Validando consistencia de datos...")
        
        params_to_check = ['latitude', 'longitude', 'name', 
                          'structure_height', 'structure_owner', 'structure_type']
        
        validation_results = []
        
        for station_id in df[station_id_col].unique():
            mask = df[station_id_col] == station_id
            station_data = df[mask]
            
            result = {'station_id': station_id}
            
            for param in params_to_check:
                if param in df.columns:
                    unique_values = station_data[param].nunique()
                    result[f'{param}_unique_count'] = unique_values
                    result[f'{param}_consistent'] = (unique_values == 1)
                else:
                    result[f'{param}_unique_count'] = None
                    result[f'{param}_consistent'] = None
            
            # Verificar si todos los parámetros son consistentes
            consistency_checks = [result[f'{p}_consistent'] for p in params_to_check if f'{p}_consistent' in result]
            result['all_consistent'] = all(consistency_checks) if consistency_checks else False
            result['total_sectors'] = len(station_data)
            
            validation_results.append(result)
        
        df_validation = pd.DataFrame(validation_results)
        
        # Estadísticas
        total_stations = len(df_validation)
        consistent_stations = df_validation['all_consistent'].sum()
        consistency_rate = (consistent_stations / total_stations * 100) if total_stations > 0 else 0
        
        self.logger.info(f"Estaciones 100% consistentes: {consistent_stations}/{total_stations} ({consistency_rate:.1f}%)")
        
        return df_validation
    
    def validate_geographic_ranges(self, df):
        """
        Valida que coordenadas estén en rangos geográficos válidos
        
        Args:
            df: DataFrame con coordenadas
            
        Returns:
            DataFrame con resultados de validación
        """
        self.logger.info("Validando rangos geográficos...")
        
        lat_min = self.config['geographic_validation']['latitude_min']
        lat_max = self.config['geographic_validation']['latitude_max']
        lon_min = self.config['geographic_validation']['longitude_min']
        lon_max = self.config['geographic_validation']['longitude_max']
        
        results = []
        
        for idx, row in df.iterrows():
            lat = row.get('latitude')
            lon = row.get('longitude')
            station_id = row.get('station_id', idx)
            
            lat_valid = lat_min <= lat <= lat_max if pd.notna(lat) else False
            lon_valid = lon_min <= lon <= lon_max if pd.notna(lon) else False
            
            results.append({
                'station_id': station_id,
                'latitude': lat,
                'longitude': lon,
                'latitude_valid': lat_valid,
                'longitude_valid': lon_valid,
                'coordinates_valid': lat_valid and lon_valid
            })
        
        df_geo_validation = pd.DataFrame(results)
        
        valid_count = df_geo_validation['coordinates_valid'].sum()
        total_count = len(df_geo_validation)
        validity_rate = (valid_count / total_count * 100) if total_count > 0 else 0
        
        self.logger.info(f"Coordenadas válidas: {valid_count}/{total_count} ({validity_rate:.1f}%)")
        
        invalid_coords = df_geo_validation[~df_geo_validation['coordinates_valid']]
        if len(invalid_coords) > 0:
            self.logger.warning(f"⚠️  {len(invalid_coords)} estaciones con coordenadas inválidas")
        
        return df_geo_validation
    
    def validate_structure_parameters(self, df):
        """
        Valida parámetros de estructura (altura, tipo, propietario)
        
        Args:
            df: DataFrame a validar
            
        Returns:
            DataFrame con resultados de validación
        """
        self.logger.info("Validando parámetros de estructura...")
        
        height_min = self.config['structure_validation']['height_min']
        height_max = self.config['structure_validation']['height_max']
        
        results = []
        
        for idx, row in df.iterrows():
            height = row.get('structure_height')
            s_type = row.get('structure_type')
            owner = row.get('structure_owner')
            station_id = row.get('station_id', idx)
            
            height_valid = height_min <= height <= height_max if pd.notna(height) else False
            type_valid = pd.notna(s_type) and str(s_type).strip() != '' and str(s_type) != '-'
            owner_valid = pd.notna(owner) and str(owner).strip() != ''
            
            results.append({
                'station_id': station_id,
                'structure_height': height,
                'structure_type': s_type,
                'structure_owner': owner,
                'height_valid': height_valid,
                'type_valid': type_valid,
                'owner_valid': owner_valid,
                'all_structure_params_valid': height_valid and type_valid and owner_valid
            })
        
        df_structure_validation = pd.DataFrame(results)
        
        valid_count = df_structure_validation['all_structure_params_valid'].sum()
        total_count = len(df_structure_validation)
        validity_rate = (valid_count / total_count * 100) if total_count > 0 else 0
        
        self.logger.info(f"Parámetros de estructura válidos: {valid_count}/{total_count} ({validity_rate:.1f}%)")
        
        return df_structure_validation
    
    def generate_validation_report(self, df_original, df_corrected, report_file):
        """
        Genera reporte comparativo de validación antes/después
        
        Args:
            df_original: DataFrame original
            df_corrected: DataFrame corregido
            report_file: Ruta para guardar el reporte
        """
        self.logger.info("Generando reporte de validación...")
        
        # Validaciones en archivo original
        original_consistency = self.validate_consistency(df_original)
        original_geo = self.validate_geographic_ranges(df_original)
        original_structure = self.validate_structure_parameters(df_original)
        
        # Validaciones en archivo corregido
        corrected_consistency = self.validate_consistency(df_corrected)
        corrected_geo = self.validate_geographic_ranges(df_corrected)
        corrected_structure = self.validate_structure_parameters(df_corrected)
        
        # Crear resumen comparativo
        comparison = {
            'metric': [
                'Estaciones consistentes',
                'Coordenadas válidas',
                'Parámetros de estructura válidos'
            ],
            'original_count': [
                original_consistency['all_consistent'].sum(),
                original_geo['coordinates_valid'].sum(),
                original_structure['all_structure_params_valid'].sum()
            ],
            'corrected_count': [
                corrected_consistency['all_consistent'].sum(),
                corrected_geo['coordinates_valid'].sum(),
                corrected_structure['all_structure_params_valid'].sum()
            ],
            'improvement': [
                corrected_consistency['all_consistent'].sum() - original_consistency['all_consistent'].sum(),
                corrected_geo['coordinates_valid'].sum() - original_geo['coordinates_valid'].sum(),
                corrected_structure['all_structure_params_valid'].sum() - original_structure['all_structure_params_valid'].sum()
            ]
        }
        
        df_comparison = pd.DataFrame(comparison)
        
        # Guardar reporte
        with pd.ExcelWriter(report_file, engine='xlsxwriter') as writer:
            df_comparison.to_excel(writer, sheet_name='comparison_summary', index=False)
            original_consistency.to_excel(writer, sheet_name='original_consistency', index=False)
            corrected_consistency.to_excel(writer, sheet_name='corrected_consistency', index=False)
            original_geo.to_excel(writer, sheet_name='original_geographic', index=False)
            corrected_geo.to_excel(writer, sheet_name='corrected_geographic', index=False)
            original_structure.to_excel(writer, sheet_name='original_structure', index=False)
            corrected_structure.to_excel(writer, sheet_name='corrected_structure', index=False)
        
        self.logger.info(f"✓ Reporte de validación guardado: {report_file}")
        
        # Mostrar resumen
        print("\n" + "="*60)
        print("RESUMEN DE VALIDACIÓN")
        print("="*60)
        print(df_comparison.to_string(index=False))
        print("="*60)