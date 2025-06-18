import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_core.runnables import RunnablePassthrough
from typing import Dict, Any, List, Optional
import asyncio
import yaml

class SearchPostAnalyzer:
    def __init__(self, google_api_key: str, 
                 config_file: str = "config/themes/general_themes.json", 
                 intents_file: str = "config/themes/posts_intents.json", 
                 prompts_file: str = "config/prompts/search_prompts.yaml"):
        """
        Initialise l'analyseur de posts de veille extérieure
        
        Args:
            google_api_key: Clé API Google Gemini
            config_file: Chemin vers le fichier de configuration JSON des thèmes généraux
            intents_file: Chemin vers le fichier de configuration JSON des intentions
            prompts_file: Chemin vers le fichier YAML des prompts
        """
        self.config_file = config_file
        self.intents_file = intents_file
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key

        # Chargement des configurations
        self.data = self._load_config()
        self.intents_data = self._load_intents_config()
        self.prompts = self._load_prompts()
        
        # Configuration de la clé API Google
        self._setup_api_key()
        
        # Initialisation du modèle LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash", 
            temperature=0.1,
            google_api_key=self.google_api_key
        )
        
        # Configuration des analyseurs
        self.setup_analyzers()

    def _setup_api_key(self):
        """Configure la clé API Google"""
        if not self.google_api_key:
            raise ValueError("La clé API Google est requise. Veuillez fournir une clé API valide.")
        
        # Définir la variable d'environnement pour assurer la compatibilité
        os.environ["GOOGLE_API_KEY"] = self.google_api_key

    def _get_formatted_prompt(self, classifier_name: str, inputs: Dict[str, Any]) -> str:
        """
        Génère et retourne le prompt final formaté pour un classifieur donné.
        Utile pour le débogage.

        Args:
            classifier_name (str): Le nom de l'attribut du classifieur.
            inputs (Dict[str, Any]): Le dictionnaire des variables à insérer dans le prompt.

        Returns:
            str: Le prompt final sous forme de chaîne de caractères.
        """
        try:
            classifier = getattr(self, classifier_name)
            prompt_template = classifier["prompt"]
            formatted_prompt = prompt_template.format_prompt(**inputs)
            return formatted_prompt.to_string()
        except (AttributeError, KeyError) as e:
            return f"Erreur lors de la génération du prompt pour '{classifier_name}': {e}"

    def _load_config(self) -> Dict[str, Any]:
        """Charge la configuration des thèmes généraux depuis le fichier JSON"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration {self.config_file} non trouvé")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON: {e}")
            raise

    def _load_intents_config(self) -> Dict[str, Any]:
        """Charge la configuration des intentions depuis le fichier JSON"""
        try:
            with open(self.intents_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration des intentions {self.intents_file} non trouvé")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON des intentions: {e}")
            raise
    
    def _load_prompts(self) -> Dict[str, str]:
        """Charge les templates de prompts depuis le fichier YAML"""
        try:
            with open(self.prompts_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Erreur lors du chargement du fichier de prompts {self.prompts_file}: {e}")
            raise

    def setup_analyzers(self):
        """Configuration des analyseurs pour la veille extérieure"""
        # 1. Classificateur de thème principal
        self.theme_classifier = self._create_theme_classifier()
        
        # 2. Classificateur des intentions (toujours veille_exterieure)
        self.intent_classifier = self._create_intent_classifier()

    def _create_theme_classifier(self):
        """Crée le classificateur de thème principal pour la veille extérieure"""
        theme_schema = [
            ResponseSchema(name="theme_id", 
                          description="ID du thème identifié parmi les thèmes disponibles"),
            ResponseSchema(name="theme_name", 
                          description="Nom du thème identifié"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        theme_parser = StructuredOutputParser.from_response_schemas(theme_schema)
        
        # Préparation des thèmes disponibles pour le prompt
        available_themes = []
        for theme in self.data["themes"]:
            available_themes.append({
                "id": theme["id"],
                "label": theme["label"],
                "keywords": theme.get("keywords", []),
                "priority": theme.get("priority", 1)
            })
        
        prompt_template_string = self.prompts['theme']
        theme_prompt = ChatPromptTemplate.from_template(prompt_template_string)
        
        theme_prompt = theme_prompt.partial(
            available_themes=json.dumps(available_themes, ensure_ascii=False, indent=2),
            format_instructions=theme_parser.get_format_instructions()
        )
        
        return {
            "prompt": theme_prompt,
            "parser": theme_parser,
            "chain": theme_prompt | self.llm | theme_parser
        }

    def _create_intent_classifier(self):
        """Crée le classificateur d'intentions pour la veille extérieure"""
        intent_schema = [
            ResponseSchema(name="intent", 
                          description="L'intention identifiée du post"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        intent_parser = StructuredOutputParser.from_response_schemas(intent_schema)
        prompt_template_string = self.prompts['intent']
        intent_prompt = ChatPromptTemplate.from_template(prompt_template_string)

        intent_prompt = intent_prompt.partial(
            format_instructions=intent_parser.get_format_instructions()
        )
        
        return {
            "prompt": intent_prompt,
            "parser": intent_parser,
            "chain": intent_prompt | self.llm | intent_parser
        }

    def _get_intents_for_veille_exterieure(self) -> List[str]:
        """Récupère les intentions disponibles pour la veille extérieure"""
        try:
            # Chercher dans les thèmes pour veille_exterieure
            for theme_data in self.intents_data.get("themes", []):
                if "veille_exterieure" in theme_data:
                    return theme_data["veille_exterieure"].get("intents", [])
            return []
        except Exception as e:
            print(f"Erreur lors de la récupération des intentions pour veille_exterieure: {e}")
            return []

    async def analyze_post(self, post_text: str) -> Dict[str, Any]:
        """
        Analyse un post de veille extérieure et retourne sa classification complète
        """
        # 1. Classification du thème principal
        theme_result = await self.theme_classifier["chain"].ainvoke({"post_text": post_text})
        
        # Structure de base du résultat
        result = {
            "theme": {
                "id": theme_result["theme_id"],
                "name": theme_result["theme_name"]
            },
            "confidence": theme_result["confidence"]
        }
        
        # 2. Classification des intentions (toujours veille_exterieure)
        result = await self._classify_intent(post_text, result)
        
        return result

    async def _classify_intent(self, post_text: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Classifie l'intention du post pour la veille extérieure"""
        try:
            # Récupérer les intentions disponibles pour veille_exterieure
            available_intents = self._get_intents_for_veille_exterieure()
            
            if not available_intents:
                print("Aucune intention trouvée pour veille_exterieure")
                result["intent"] = {
                    "name": "unknown",
                    "confidence": 0.0
                }
                return result
            
            # Classification de l'intention
            intent_result = await self.intent_classifier["chain"].ainvoke({
                "post_text": post_text,
                "available_intents": ", ".join(available_intents)
            })
            
            # Ajouter l'intention au résultat
            result["intent"] = {
                "name": intent_result["intent"],
                "confidence": intent_result["confidence"]
            }
            
            # Mettre à jour la confiance globale
            result["confidence"] = min(result["confidence"], intent_result["confidence"])
            
        except Exception as e:
            print(f"Erreur lors de la classification d'intention: {e}")
            result["intent"] = {
                "name": "unknown",
                "confidence": 0.0
            }
        
        return result

    def analyze_post_sync(self, post_text: str) -> Dict[str, Any]:
        """Version synchrone de l'analyse de post"""
        return asyncio.run(self.analyze_post(post_text))


# Exemple d'utilisation et test
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyDroS___71S2NH_Qz08fuZBkJeX0s21dCY"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return

    try:
        # Initialiser l'analyseur avec la clé API
        analyzer = SearchPostAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            config_file="config/themes/general_themes.json",
            intents_file="config/themes/posts_intents.json",
            prompts_file="config/prompts/search_prompts.yaml"
        )
        
        # Tests avec différents types de posts de veille extérieure
        test_posts = [
            "تطبيق كيحول عرض نجمة 6 الى نجمة 3 على [إنوي و اتيصلات المغرب و أورنج...]",
            "طريقة كفاش تسلف من إنوي✅🤩"

        ]

        print("=== Tests de Classification Veille Extérieure ===\n")
        
        for i, post in enumerate(test_posts, 1):
            print(f"Test {i}: {post.strip()}")
            print("-" * 50)
            
            try:
                result = await analyzer.analyze_post(post)
                print(f"Résultat: {json.dumps(result, ensure_ascii=False, indent=2)}")
            except Exception as e:
                print(f"Erreur lors de l'analyse: {e}")
            
            print("\n" + "="*70 + "\n")
            
    except Exception as e:
        print(f"Erreur d'initialisation: {e}")

if __name__ == "__main__":
    # Pour tester la classe
    asyncio.run(main())