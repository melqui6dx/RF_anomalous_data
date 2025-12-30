import pandas as pd
import numpy as np
import re
import logging
from typing import List, Tuple, Optional

class ExtendedCellDetector:
    """
    Detector de sectores extendidos (Extended Cells)
    Adaptado para trabajar con la arquitectura multi-hoja existente
    """
    
    def __init__(self, config):
        """
        Inicializa el detector
        
        Args:
            config: Diccionario de configuración
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.distance_threshold = config['extended_cell_distance_threshold']
    
    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
        """
        Calcula distancia aproximada entre dos coordenadas (en grados)
        
        Para distancias cortas, usamos aproximación euclidiana.
        Para precisión mayor, usar fórmula de Haversine.
        
        Args:
            lat1, lon1: Primera coordenada
            lat2, lon2: Segunda coordenada
            
        Returns:
            Distancia en grados decimales o None si hay valores inválidos
        """
        if any(pd.isna([lat1, lon1, lat2, lon2])):
            return None
        
        try:
            # Aproximación euclidiana simple
            distance = np.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)
            return distance
        except Exception as e:
            self.logger.warning(f"Error calculando distancia: {e}")
            return None
    
    def follows_extended_nomenclature(self, station_id: str, cell_id: str) -> bool:
        """
        Verifica si el cell_id sigue la nomenclatura de sector extendido
        
        Nomenclatura: [station_id] + R + [número]
        Ejemplos: ABB -> ABBR1, ABBR2
                  12D -> 12DR1, 12DR2
        
        Args:
            station_id: ID de la estación
            cell_id: ID completo del sector
            
        Returns:
            True si sigue la nomenclatura, False en caso contrario
        """
        if not station_id or not cell_id:
            return False
        
        # Patrón: station_id + 'R' + dígitos
        # Usar re.escape para caracteres especiales en station_id
        pattern = f"^{re.escape(str(station_id))}R\\d+$"
        
        try:
            match = re.match(pattern, str(cell_id), re.IGNORECASE)
            return match is not None
        except Exception as e:
            self.logger.warning(f"Error verificando nomenclatura para {cell_id}: {e}")
            return False
    
    def detect_extended_cells_in_station(self, station_data: pd.DataFrame) -> List[str]:
        """
        Detecta sectores extendidos en una estación
        
        Criterios:
        1. Seguir nomenclatura [station_id]R[n]
        2. Tener coordenadas significativamente diferentes
        3. Pertenecer al mismo sitio
        
        Args:
            station_data: DataFrame con todos los sectores de una estación
            
        Returns:
            Lista de cell_id que son sectores extendidos
        """
        extended_cells = []
        
        if len(station_data) == 0:
            return extended_cells
        
        station_id = station_data['station_id'].iloc[0]
        
        # Verificar que tenemos las columnas necesarias
        required_cols = ['latitude', 'longitude']
        if not all(col in station_data.columns for col in required_cols):
            self.logger.debug(f"Columnas faltantes para detectar extended cells en {station_id}")
            return extended_cells
        
        # Determinar la columna de identificación de celda
        cell_id_col = None
        for possible_col in ['station_cell_id', 'cell_id', 'sector_id']:
            if possible_col in station_data.columns:
                cell_id_col = possible_col
                break
        
        if not cell_id_col:
            self.logger.debug(f"No se encontró columna de cell_id para {station_id}")
            return extended_cells
        
        # Filtrar filas con coordenadas válidas
        valid_coords = station_data.dropna(subset=['latitude', 'longitude']).copy()
        
        if len(valid_coords) <= 1:
            # Solo hay una ubicación o ninguna, no hay sectores extendidos
            return extended_cells
        
        # Agrupar por coordenadas únicas
        coord_groups = valid_coords.groupby(['latitude', 'longitude'])
        
        if len(coord_groups) <= 1:
            # Solo hay una ubicación, no hay sectores extendidos
            return extended_cells
        
        # Obtener ubicación principal (la que más sectores tiene)
        group_sizes = coord_groups.size()
        main_location = group_sizes.idxmax()
        main_lat, main_lon = main_location
        
        self.logger.debug(f"Ubicación principal de {station_id}: ({main_lat}, {main_lon})")
        
        # Revisar cada sector
        for idx, row in valid_coords.iterrows():
            cell_id = row.get(cell_id_col, '')
            cell_lat = row.get('latitude')
            cell_lon = row.get('longitude')
            
            # Verificar nomenclatura
            if not self.follows_extended_nomenclature(station_id, cell_id):
                continue
            
            # Calcular distancia
            distance = self.calculate_distance(main_lat, main_lon, cell_lat, cell_lon)
            
            if distance is None:
                continue
            
            # Si está lejos de la ubicación principal
            if distance > self.distance_threshold:
                extended_cells.append(str(cell_id))
                distance_km = distance * 111  # Aproximación: 1° ≈ 111km
                self.logger.info(
                    f"  🔄 Sector extendido detectado: {cell_id} "
                    f"(distancia: {distance:.4f}° ≈ {distance_km:.1f}km de ubicación principal)"
                )
        
        return extended_cells
    
    def mark_extended_cells(self, df: pd.DataFrame, extended_cells_list: List[str]) -> pd.DataFrame:
        """
        Marca sectores como Extended Cell en el DataFrame
        
        Args:
            df: DataFrame de parámetros físicos
            extended_cells_list: Lista de cell_id que son extended cells
            
        Returns:
            DataFrame modificado con cell_type actualizado
        """
        if not extended_cells_list or len(df) == 0:
            return df
        
        # Determinar columna de cell_id
        cell_id_col = None
        for possible_col in ['station_cell_id', 'cell_id', 'sector_id']:
            if possible_col in df.columns:
                cell_id_col = possible_col
                break
        
        if not cell_id_col:
            self.logger.debug("No se encontró columna de cell_id para marcar extended cells")
            return df
        
        # Verificar si existe columna cell_type
        if 'cell_type' not in df.columns:
            self.logger.debug("Columna 'cell_type' no existe, no se pueden marcar extended cells")
            return df
        
        # Marcar celdas extendidas
        mask = df[cell_id_col].isin(extended_cells_list)
        cells_marked = mask.sum()
        
        if cells_marked > 0:
            df.loc[mask, 'cell_type'] = 'Extended Cell'
            self.logger.info(f"  ✓ {cells_marked} sectores marcados como Extended Cell")
        
        return df