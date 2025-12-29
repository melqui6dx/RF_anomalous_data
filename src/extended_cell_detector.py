import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Tuple, Optional

class ExtendedCellDetector:
    """
    Detector de sectores extendidos (Extended Cells)

    Los sectores extendidos son sectores que pertenecen al mismo sitio (station_id)
    pero están ubicados físicamente en otra ubicación, típicamente a 500m-1km de distancia.

    Criterios de detección:
    1. Nomenclatura: station_cell_id sigue el patrón [station_id]R[número]
    2. Distancia: Coordenadas están a > threshold (~1.1 km) de la ubicación principal
    3. Mismo sitio: Tiene el mismo station_id
    """

    def __init__(self, config: Dict):
        """
        Inicializa el detector de extended cells

        Args:
            config: Diccionario de configuración del sistema
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.distance_threshold = config.get('extended_cell_distance_threshold', 0.01)

        self.logger.info(
            f"ExtendedCellDetector inicializado con threshold: {self.distance_threshold}° "
            f"(~{self.distance_threshold * 111:.1f} km)"
        )

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
        """
        Calcula distancia aproximada entre dos coordenadas (en grados)

        Para distancias cortas (< 100km), usamos aproximación euclidiana simple.
        Para mayor precisión en distancias largas, se podría usar la fórmula de Haversine.

        Args:
            lat1, lon1: Primera coordenada (latitud, longitud)
            lat2, lon2: Segunda coordenada (latitud, longitud)

        Returns:
            Distancia en grados decimales, o None si hay valores inválidos
        """
        if any(pd.isna([lat1, lon1, lat2, lon2])):
            return None

        try:
            # Aproximación euclidiana simple
            # distance² = (Δlat)² + (Δlon)²
            distance = np.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)
            return distance
        except Exception as e:
            self.logger.warning(f"Error calculando distancia: {e}")
            return None

    def follows_extended_nomenclature(self, station_id: str, cell_id: str) -> bool:
        """
        Verifica si el cell_id sigue la nomenclatura de sector extendido

        Nomenclatura esperada: [station_id] + 'R' + [dígitos]

        Ejemplos válidos:
        - Station ABB: ABBR1, ABBR2, ABBR10
        - Station 12D: 12DR1, 12DR2

        Ejemplos inválidos:
        - Station ABB: ABBA1, ABBA2 (usa 'A' en lugar de 'R')
        - Station 12D: 12DA, 12DB (no tiene 'R')

        Args:
            station_id: ID de la estación
            cell_id: ID completo del sector

        Returns:
            True si sigue la nomenclatura de extended cell, False en caso contrario
        """
        if not station_id or not cell_id:
            return False

        # Patrón regex: station_id exacto + 'R' + uno o más dígitos
        # Ejemplo: ^ABB + R + \d+$ → ABBR1, ABBR2, etc.
        pattern = f"^{re.escape(station_id)}R\\d+$"

        match = re.match(pattern, str(cell_id), re.IGNORECASE)

        return match is not None

    def detect_extended_cells_in_station(self, station_data: pd.DataFrame) -> List[str]:
        """
        Detecta sectores extendidos en una estación

        Proceso:
        1. Agrupar sectores por coordenadas únicas
        2. Identificar ubicación principal (la que tiene más sectores)
        3. Para cada sector:
           a. Verificar si sigue nomenclatura [station_id]R[n]
           b. Calcular distancia respecto a ubicación principal
           c. Si distancia > threshold, es extended cell
        4. Retornar lista de station_cell_id que son extended cells

        Args:
            station_data: DataFrame con todos los sectores de una estación

        Returns:
            Lista de station_cell_id que son sectores extendidos
        """
        extended_cells = []

        if len(station_data) == 0:
            return extended_cells

        station_id = station_data['station_id'].iloc[0]

        # Verificar que existan las columnas necesarias
        required_cols = ['station_cell_id', 'latitude', 'longitude']
        missing_cols = [col for col in required_cols if col not in station_data.columns]

        if missing_cols:
            self.logger.warning(
                f"Columnas faltantes para detectar extended cells en {station_id}: {missing_cols}"
            )
            return extended_cells

        # Agrupar por coordenadas únicas
        coord_groups = station_data.groupby(['latitude', 'longitude'], dropna=True)

        if len(coord_groups) <= 1:
            # Solo hay una ubicación, no puede haber sectores extendidos
            self.logger.debug(f"Estación {station_id} tiene solo una ubicación, no hay extended cells")
            return extended_cells

        # Identificar ubicación principal (la que tiene más sectores)
        group_sizes = coord_groups.size()
        main_location = group_sizes.idxmax()  # Tupla (lat, lon) con más sectores
        main_lat, main_lon = main_location

        self.logger.debug(
            f"Ubicación principal de {station_id}: ({main_lat}, {main_lon}) "
            f"con {group_sizes[main_location]} sectores"
        )

        # Revisar cada sector
        for idx, row in station_data.iterrows():
            cell_id = row.get('station_cell_id', '')
            cell_lat = row.get('latitude')
            cell_lon = row.get('longitude')

            # 1. Verificar nomenclatura [station_id]R[n]
            if not self.follows_extended_nomenclature(station_id, cell_id):
                continue

            # 2. Calcular distancia respecto a ubicación principal
            distance = self.calculate_distance(main_lat, main_lon, cell_lat, cell_lon)

            if distance is None:
                self.logger.warning(f"No se pudo calcular distancia para {cell_id}")
                continue

            # 3. Si distancia > threshold, es extended cell
            if distance > self.distance_threshold:
                extended_cells.append(cell_id)
                distance_km = distance * 111  # Conversión aproximada a km
                self.logger.info(
                    f"  🔍 Sector extendido detectado: {cell_id} "
                    f"(distancia: {distance:.4f}° ≈ {distance_km:.1f} km de ubicación principal)"
                )
            else:
                self.logger.debug(
                    f"Sector {cell_id} sigue nomenclatura R[n] pero está cerca "
                    f"({distance:.4f}° < {self.distance_threshold}°), no es extended cell"
                )

        if extended_cells:
            self.logger.info(
                f"  ✓ Total de {len(extended_cells)} extended cells detectados en {station_id}"
            )

        return extended_cells

    def mark_extended_cells(self, df: pd.DataFrame, extended_cells_list: List[str]) -> pd.DataFrame:
        """
        Marca sectores como Extended Cell en el DataFrame

        Cambia el valor de cell_type de "Macro Cell" a "Extended Cell"
        para todos los station_cell_id en la lista.

        Args:
            df: DataFrame de parámetros físicos
            extended_cells_list: Lista de station_cell_id que son extended cells

        Returns:
            DataFrame modificado con cell_type actualizado
        """
        if not extended_cells_list:
            self.logger.debug("No hay extended cells para marcar")
            return df

        if 'cell_type' not in df.columns:
            self.logger.warning("Columna 'cell_type' no encontrada en DataFrame")
            return df

        if 'station_cell_id' not in df.columns:
            self.logger.warning("Columna 'station_cell_id' no encontrada en DataFrame")
            return df

        # Crear máscara para los extended cells
        mask = df['station_cell_id'].isin(extended_cells_list)
        cells_marked = mask.sum()

        if cells_marked > 0:
            # Cambiar cell_type a "Extended Cell"
            df.loc[mask, 'cell_type'] = 'Extended Cell'
            self.logger.info(f"  ✓ {cells_marked} sectores marcados como Extended Cell")

            # Log detallado de los cambios
            for cell_id in extended_cells_list:
                cell_mask = df['station_cell_id'] == cell_id
                if cell_mask.any():
                    old_type = df.loc[cell_mask, 'cell_type'].iloc[0] if 'cell_type' in df.columns else 'Unknown'
                    self.logger.debug(f"    {cell_id}: Macro Cell → Extended Cell")
        else:
            self.logger.warning(
                f"Ninguno de los {len(extended_cells_list)} extended cells fue encontrado en el DataFrame"
            )

        return df
