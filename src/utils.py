import os
import yaml
from datetime import datetime
import shutil
import logging

def load_config(config_path='config/settings.yaml'):
    """Carga la configuración desde archivo YAML"""
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

def setup_logging(log_dir='logs/'):
    """Configura el sistema de logging"""
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'correction_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def create_backup(file_path, backup_dir='data/output/backups/'):
    """Crea backup de un archivo"""
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.basename(file_path)
    backup_path = os.path.join(backup_dir, f'{timestamp}_{filename}')
    
    shutil.copy2(file_path, backup_path)
    return backup_path

def get_timestamp():
    """Retorna timestamp formateado"""
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def ensure_directories_exist(config):
    """Asegura que todos los directorios necesarios existan"""
    dirs = [
        os.path.dirname(config['input_files']['physical_parameters']),
        os.path.dirname(config['input_files']['anomalous_data']),
        config['output_files']['corrected_data_dir'],
        config['output_files']['reports_dir'],
        config['output_files']['backups_dir'],
        config['output_files']['logs_dir']
    ]
    
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)