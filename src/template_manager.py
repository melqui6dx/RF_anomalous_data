import pandas as pd
import numpy as np
import logging
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Any

class TemplateManager:
    """
    Gestor del template de referencia para datos RF

    Este gestor maneja el archivo template que contiene los valores correctos
    de referencia para las estaciones, y proporciona métodos para búsqueda
    con fuzzy matching y completado de parámetros faltantes.
    """

    def __init__(self, template_file: str, config: Dict):
        """
        Inicializa el gestor de template

        Args:
            template_file: Ruta al archivo Excel del template
            config: Diccionario de configuración del sistema
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        try:
            self.logger.info(f"Cargando template de referencia: {template_file}")
            self.df_template = pd.read_excel(template_file)
            self.logger.info(f"Template cargado: {len(self.df_template)} filas")

            # Crear índice por station_id para búsqueda rápida
            if 'station_id' in self.df_template.columns:
                self.template_by_station = self.df_template.groupby('station_id')
                self.logger.info(f"Índice creado: {len(self.template_by_station)} estaciones únicas")
            else:
                self.logger.error("Columna 'station_id' no encontrada en template")
                self.template_by_station = None

        except FileNotFoundError:
            self.logger.error(f"Archivo template no encontrado: {template_file}")
            self.df_template = pd.DataFrame()
            self.template_by_station = None
        except Exception as e:
            self.logger.error(f"Error cargando template: {e}")
            self.df_template = pd.DataFrame()
            self.template_by_station = None

    def get_station_data(self, station_id: str) -> Optional[pd.DataFrame]:
        """
        Obtiene datos del template para una estación específica

        Args:
            station_id: ID de la estación

        Returns:
            DataFrame con datos de la estación o None si no existe
        """
        if self.template_by_station is None:
            return None

        try:
            return self.template_by_station.get_group(station_id)
        except KeyError:
            self.logger.debug(f"Estación {station_id} no encontrada en template")
            return None

    def calculate_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calcula similitud entre dos nombres usando SequenceMatcher

        Normaliza los nombres a minúsculas y elimina espacios extra antes
        de calcular la similitud.

        Args:
            name1: Primer nombre a comparar
            name2: Segundo nombre a comparar

        Returns:
            Valor entre 0.0 (totalmente diferentes) y 1.0 (idénticos)
        """
        if not name1 or not name2:
            return 0.0

        # Normalizar: minúsculas, sin espacios extra
        n1 = str(name1).lower().strip()
        n2 = str(name2).lower().strip()

        # Usar SequenceMatcher para calcular similitud
        return SequenceMatcher(None, n1, n2).ratio()

    def get_reference_name(self, station_id: str, candidate_names: List[str]) -> Optional[str]:
        """
        Obtiene el nombre de referencia del template o el más similar

        Proceso:
        1. Buscar nombre en template
        2. Si está en candidatos exactamente, retornarlo
        3. Si no, buscar el más similar usando fuzzy matching
        4. Si similitud >= threshold, retornar el candidato más similar
        5. Si no hay match, retornar nombre del template

        Args:
            station_id: ID de la estación
            candidate_names: Lista de nombres candidatos de los datos anómalos

        Returns:
            Nombre seleccionado o None si no hay datos
        """
        station_data = self.get_station_data(station_id)

        if station_data is None or len(station_data) == 0:
            self.logger.debug(f"Sin datos en template para {station_id}, usando algoritmo por defecto")
            return None

        # Obtener nombre(s) del template
        if 'name' not in station_data.columns:
            self.logger.warning(f"Columna 'name' no encontrada en template para {station_id}")
            return None

        template_names = station_data['name'].dropna().unique().tolist()

        if not template_names:
            return None

        template_name = template_names[0]  # Usar el primero

        if not candidate_names:
            return template_name

        # Si el nombre del template está en los candidatos exactamente, usarlo
        if template_name in candidate_names:
            self.logger.info(f"  ✓ Nombre del template encontrado exacto: {template_name}")
            return template_name

        # Si está activado fuzzy matching, buscar el más similar
        if self.config.get('name_similarity', {}).get('use_fuzzy_matching', True):
            threshold = self.config.get('name_similarity', {}).get('similarity_threshold', 0.7)

            best_match = None
            best_similarity = 0.0

            for candidate in candidate_names:
                similarity = self.calculate_name_similarity(template_name, candidate)

                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = candidate

            if best_similarity >= threshold:
                self.logger.info(
                    f"  ✓ Nombre similar encontrado: {best_match} "
                    f"(similitud: {best_similarity:.2%} con template: {template_name})"
                )
                return best_match
            else:
                self.logger.warning(
                    f"  ⚠️  Ningún nombre candidato es similar al template "
                    f"(mejor: {best_match} con {best_similarity:.2%})"
                )

        # Si no hay match, retornar nombre del template
        self.logger.info(f"  → Usando nombre del template: {template_name}")
        return template_name

    def get_reference_value(self, station_id: str, parameter: str) -> Optional[Any]:
        """
        Obtiene valor de referencia del template para un parámetro

        Si hay múltiples valores para el parámetro en el template,
        retorna el más frecuente.

        Args:
            station_id: ID de la estación
            parameter: Nombre del parámetro (ej: 'structure_owner', 'tx_type')

        Returns:
            Valor de referencia o None si no existe
        """
        station_data = self.get_station_data(station_id)

        if station_data is None or parameter not in station_data.columns:
            return None

        # Obtener valores únicos no nulos
        values = station_data[parameter].dropna().unique().tolist()

        if not values:
            return None

        # Si hay múltiples valores, retornar el más frecuente
        if len(values) > 1:
            value_counts = station_data[parameter].value_counts()
            most_common = value_counts.index[0]
            self.logger.debug(
                f"Múltiples valores para {parameter} en {station_id}, "
                f"seleccionando más frecuente: {most_common}"
            )
            return most_common

        return values[0]

    def fill_missing_parameters(self, station_id: str, current_values: Dict) -> Dict:
        """
        Rellena parámetros faltantes desde el template

        Parámetros que se pueden rellenar:
        - structure_owner: Propietario de la estructura
        - structure_type: Tipo de estructura ((S) Selfsupported, (M) Monopole, etc.)
        - tx_type: Tipo de transmisión (MW, FO, etc.)

        Solo rellena si el valor actual es None, vacío o no existe.

        Args:
            station_id: ID de la estación
            current_values: Dict con valores actuales (puede tener None o vacíos)

        Returns:
            Dict con valores actualizados (los faltantes rellenados desde template)
        """
        fillable_params = ['structure_owner', 'structure_type', 'tx_type']
        updated_values = current_values.copy()

        station_data = self.get_station_data(station_id)

        if station_data is None:
            self.logger.debug(f"No hay datos en template para rellenar parámetros de {station_id}")
            return updated_values

        for param in fillable_params:
            # Verificar si el valor actual está vacío o es None
            current_val = current_values.get(param)

            if current_val is None or current_val == '' or (isinstance(current_val, str) and current_val.strip() == ''):
                template_value = self.get_reference_value(station_id, param)

                if template_value:
                    updated_values[param] = template_value
                    self.logger.info(
                        f"  ✓ Parámetro {param} rellenado desde template: {template_value}"
                    )
                else:
                    self.logger.debug(f"No hay valor en template para {param} de {station_id}")

        return updated_values
