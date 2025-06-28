
import json
from abc import ABC, abstractmethod
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from typing import Dict, Any, List
from config_manager import ConfigManager

class BaseAnalyzer(ABC):
    """Base class for all content analyzers"""
    
    def __init__(self, google_api_key: str, brand_name: str, 
                 intents_file_posts: str = "../config/themes/posts_intents.json",
                 intents_file_comments: str = "../config/themes/comments_intents.json", 
                 prompts_file: str = "../config/prompts/prompts_o.yaml"):
        """
        Initialize the base analyzer
        
        Args:
            google_api_key: Google Gemini API key
            brand_name: Brand name
            intents_file_posts: Path to posts intents JSON config
            intents_file_comments: Path to comments intents JSON config
            prompts_file: Path to YAML prompts file
        """
        self.config_file = f"../config/themes/{brand_name}_themes.json"
        self.intents_file_posts = intents_file_posts
        self.intents_file_comments = intents_file_comments
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key
        self.brand_name = brand_name
        
        # Load configurations
        self.data = ConfigManager.load_config(self.config_file)
        self.intents_data_comments = ConfigManager.load_intents_config(intents_file_comments)
        self.intents_data_posts = ConfigManager.load_intents_config(intents_file_posts)
        self.prompts_config = ConfigManager.load_prompts_config(prompts_file)
        
        # Setup API key
        self._setup_api_key()
        
        # Initialize LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash", 
            temperature=0,
            google_api_key=self.google_api_key
        )
        
        # Setup analyzers
        self.setup_analyzers()
    
    def _setup_api_key(self):
        """Configure Google API key"""
        if not self.google_api_key:
            raise ValueError("La clé API Google est requise. Veuillez fournir une clé API valide.")
    
    @abstractmethod
    def setup_analyzers(self):
        """Setup specific analyzers for this module"""
        pass
    
    def _create_classifier_from_yaml(self, classifier_name: str) -> Dict[str, Any]:
        """Create classifier from YAML configuration"""
        classifier_config = self.prompts_config.get('classifiers', {}).get(classifier_name, {})
        
        if not classifier_config:
            raise ValueError(f"Configuration {classifier_name} non trouvée dans le fichier prompts YAML")
        
        # Configure response schema from YAML
        output_schema = classifier_config.get('output_schema', [])
        classifier_schema = []
        for schema_item in output_schema:
            classifier_schema.append(
                ResponseSchema(
                    name=schema_item['name'], 
                    description=schema_item['description']
                )
            )
        
        classifier_parser = StructuredOutputParser.from_response_schemas(classifier_schema)
        
        # Get prompt template from YAML
        prompt_template = classifier_config.get('prompt_template', '')
        
        classifier_prompt = ChatPromptTemplate.from_template(
            template=prompt_template,
            template_format="jinja2"
        )
        
        classifier_prompt = classifier_prompt.partial(
            format_instructions=classifier_parser.get_format_instructions()
        )
        
        return {
            "prompt": classifier_prompt,
            "parser": classifier_parser,
            "chain": classifier_prompt | self.llm | classifier_parser
        }
    
    def _display_final_prompt(self, classifier_name: str, input_data: Dict[str, Any]):
        """Display final formatted prompt with input data"""
        print(f"\n{'='*20} PROMPT FINAL - {classifier_name.upper()} {'='*20}")
        
        try:
            classifier = getattr(self, f"{classifier_name}_classifier", None)
            if not classifier:
                print(f"Classifieur {classifier_name} non trouvé")
                return
            
            formatted_prompt = classifier["prompt"].format(**input_data)
            
            if hasattr(formatted_prompt, 'messages'):
                for i, message in enumerate(formatted_prompt.messages):
                    print(f"Message {i+1} ({message._name_}):")
                    print(f"Contenu: {message.content}")
                    print("-" * 50)
            else:
                print(f"Prompt formaté:\n{formatted_prompt}")
            
        except Exception as e:
            print(f"Erreur lors de l'affichage du prompt pour {classifier_name}: {e}")
            print(f"Données d'entrée: {json.dumps(input_data, ensure_ascii=False, indent=2)}")
        
        print(f"{'='*(42 + len(classifier_name))}\n")
    
    def get_intents_for_theme(self, theme_id: str, content_type: str) -> List[str]:
        """Get list of intents associated with a theme for specific content type"""
        if content_type == "post":
            try:
                for theme_data in self.intents_data_posts.get("themes", []):
                    if theme_id in theme_data:
                        return theme_data[theme_id].get("intents", [])
                return []
            except Exception as e:
                print(f"Erreur lors de la récupération des intentions pour le thème {theme_id}: {e}")
                return []
        elif content_type == "comment":
            try:
                for theme_data in self.intents_data_comments.get("themes", []):
                    if theme_id in theme_data:
                        return theme_data[theme_id].get("intents", [])
                return []
            except Exception as e:
                print(f"Erreur lors de la récupération des intentions pour le thème {theme_id}: {e}")
                return []