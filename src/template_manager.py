import pandas as pd
import numpy as np
import logging
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Any

class TemplateManager:
    """
    Gestor del template de referencia para datos RF
    Adaptado para trabajar con la arquitectura multi-hoja existente
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
            # Cargar template (puede tener una o múltiples hojas)
            self.template_sheets = pd.read_excel(template_file, sheet_name=None)
            self.logger.info(f"Template cargado con {len(self.template_sheets)} hoja(s)")
            
            # Consolidar todas las hojas en un solo DataFrame para búsqueda
            self.df_template = pd.concat(self.template_sheets.values(), ignore_index=True)
            self.logger.info(f"Template consolidado: {len(self.df_template)} filas")
            
            # Crear índice por station_id para búsqueda rápida
            if 'station_id' in self.df_template.columns:
                self.template_by_station = self.df_template.groupby('station_id')
            else:
                self.logger.warning("Columna 'station_id' no encontrada en template")
                self.template_by_station = None
            
        except FileNotFoundError:
            self.logger.error(f"Template no encontrado: {template_file}")
            self.df_template = pd.DataFrame()
            self.template_by_station = None
            self.template_sheets = {}
        except Exception as e:
            self.logger.error(f"Error cargando template: {e}")
            self.df_template = pd.DataFrame()
            self.template_by_station = None
            self.template_sheets = {}
    
    def get_station_data(self, station_id: str) -> Optional[pd.DataFrame]:
        """
        Obtiene datos del template para una estación específica
        
        Args:
            station_id: ID de la estación
            
        Returns:
            DataFrame con datos de la estación o None
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
        
        Args:
            name1, name2: Nombres a comparar
            
        Returns:
            Valor entre 0.0 (totalmente diferentes) y 1.0 (idénticos)
        """
        if not name1 or not name2:
            return 0.0
        
        # Normalizar: minúsculas, sin espacios extra
        n1 = str(name1).lower().strip()
        n2 = str(name2).lower().strip()
        
        # Remover puntos y caracteres especiales para mejor comparación
        n1 = n1.replace('.', '').replace(',', '').replace('-', ' ')
        n2 = n2.replace('.', '').replace(',', '').replace('-', ' ')
        
        # Normalizar espacios múltiples
        n1 = ' '.join(n1.split())
        n2 = ' '.join(n2.split())
        
        return SequenceMatcher(None, n1, n2).ratio()
    
    def get_reference_name(self, station_id: str, candidate_names: List[str]) -> Optional[str]:
        """
        Obtiene el nombre de referencia del template o el más similar
        
        Args:
            station_id: ID de la estación
            candidate_names: Lista de nombres candidatos
            
        Returns:
            Nombre seleccionado o None
        """
        station_data = self.get_station_data(station_id)
        
        if station_data is None or len(station_data) == 0:
            self.logger.debug(f"Sin datos en template para {station_id}, usando algoritmo por defecto")
            return None
        
        # Obtener nombre(s) del template
        if 'name' not in station_data.columns:
            self.logger.debug(f"Columna 'name' no encontrada en template para {station_id}")
            return None
        
        template_names = station_data['name'].dropna().unique().tolist()
        
        if not template_names:
            return None
        
        template_name = template_names[0]  # Usar el primero
        
        if not candidate_names:
            return template_name
        
        # Si el nombre del template está en los candidatos, usarlo
        if template_name in candidate_names:
            self.logger.info(f"  ✓ Nombre del template encontrado: {template_name}")
            return template_name
        
        # Si está activado fuzzy matching, buscar el más similar
        if self.config['name_similarity']['use_fuzzy_matching']:
            threshold = self.config['name_similarity']['similarity_threshold']
            
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
        self.logger.info(f"  📘 Usando nombre del template: {template_name}")
        return template_name
    
    def get_reference_value(self, station_id: str, parameter: str) -> Optional[Any]:
        """
        Obtiene valor de referencia del template para un parámetro
        
        Args:
            station_id: ID de la estación
            parameter: Nombre del parámetro
            
        Returns:
            Valor de referencia o None
        """
        station_data = self.get_station_data(station_id)
        
        if station_data is None or parameter not in station_data.columns:
            return None
        
        # Obtener valores únicos no nulos
        values = station_data[parameter].dropna().unique().tolist()
        
        if not values:
            return None
        
        # Filtrar valores vacíos o inválidos
        values = [v for v in values if str(v).strip() != '' and str(v) != '-']
        
        if not values:
            return None
        
        # Si hay múltiples valores, retornar el más frecuente
        if len(values) > 1:
            value_counts = station_data[parameter].value_counts()
            return value_counts.index[0]
        
        return values[0]
    
    def fill_missing_parameters(self, station_id: str, current_values: Dict) -> Dict:
        """
        Rellena parámetros faltantes desde el template
        
        Parámetros que se pueden rellenar:
        - structure_owner
        - structure_type
        - tx_type
        
        Args:
            station_id: ID de la estación
            current_values: Dict con valores actuales (puede tener None)
            
        Returns:
            Dict con valores actualizados
        """
        fillable_params = ['structure_owner', 'structure_type', 'tx_type']
        updated_values = current_values.copy()
        
        station_data = self.get_station_data(station_id)
        
        if station_data is None:
            return updated_values
        
        for param in fillable_params:
            # Si el valor actual es None o vacío
            current_val = current_values.get(param)
            
            is_empty = (
                pd.isna(current_val) or 
                current_val is None or 
                str(current_val).strip() == '' or
                str(current_val) == '-'
            )
            
            if is_empty:
                template_value = self.get_reference_value(station_id, param)
                
                if template_value:
                    updated_values[param] = template_value
                    self.logger.info(
                        f"  ✓ Parámetro '{param}' rellenado desde template: {template_value}"
                    )
        
        return updated_values
    
    def is_available(self) -> bool:
        """
        Verifica si el template está disponible y cargado
        
        Returns:
            True si el template está disponible
        """
        return self.template_by_station is not None and len(self.df_template) > 0