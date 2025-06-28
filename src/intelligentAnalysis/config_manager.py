import json
import yaml
from typing import Dict, Any


class ConfigManager:
    """Manages configuration loading for analyzers"""
    
    @staticmethod
    def load_config(config_file: str) -> Dict[str, Any]:
        """Load themes configuration from JSON file"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration {config_file} non trouvé")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON: {e}")
            raise
    
    @staticmethod
    def load_intents_config(intents_file: str) -> Dict[str, Any]:
        """Load intents configuration from JSON file"""
        try:
            with open(intents_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration des intentions {intents_file} non trouvé")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON des intentions: {e}")
            raise
    
    @staticmethod
    def load_prompts_config(prompts_file: str) -> Dict[str, Any]:
        """Load prompts configuration from YAML file"""
        try:
            with open(prompts_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration des prompts {prompts_file} non trouvé")
            raise
        except yaml.YAMLError as e:
            print(f"Erreur de parsing YAML: {e}")
            raise