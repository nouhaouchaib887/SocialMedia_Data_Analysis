import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from typing import Dict, Any, List, Optional
import asyncio
import yaml
from jinja2 import Template

class SearchPostAnalyzer:
    def __init__(self, google_api_key: str, 
                 themes_file: str = "config/themes/general_themes.json",
                 post_intents_file: str = "config/themes/posts_intents.json", 
                 comment_intents_file: str = "config/themes/comments_intents.json",
                 prompts_file: str = "config/prompts/search_prompts.yaml"):
        """
        Initialise l'analyseur de contenu de veille extérieure (posts et commentaires).
        
        Args:
            google_api_key: Clé API Google Gemini.
            themes_file: Chemin vers le fichier JSON des thèmes généraux.
            post_intents_file: Chemin vers le fichier JSON des intentions pour les posts.
            comment_intents_file: Chemin vers le fichier JSON des intentions pour les commentaires.
            prompts_file: Chemin vers le fichier YAML des prompts structurés.
        """
        self.google_api_key = google_api_key
        self.themes_file = themes_file
        self.post_intents_file = post_intents_file
        self.comment_intents_file = comment_intents_file
        self.prompts_file = prompts_file

        # Chargement des configurations
        self.themes_data = self._load_json_config(self.themes_file)
        self.post_intents_data = self._load_json_config(self.post_intents_file)
        self.comment_intents_data = self._load_json_config(self.comment_intents_file)
        self.prompts_config = self._load_prompts_config()
        
        self._setup_api_key()
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash", 
            temperature=0.1,
            google_api_key=self.google_api_key
        )
        
        self.setup_analyzers()

    def _setup_api_key(self):
        if not self.google_api_key:
            raise ValueError("La clé API Google est requise.")
        os.environ["GOOGLE_API_KEY"] = self.google_api_key

    def _load_json_config(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration {file_path} non trouvé.")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON dans {file_path}: {e}")
            raise

    def _load_prompts_config(self) -> Dict[str, Any]:
        try:
            with open(self.prompts_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Erreur lors du chargement du fichier de prompts {self.prompts_file}: {e}")
            raise

    def setup_analyzers(self):
        """Configuration des analyseurs à partir de la configuration YAML."""
        # Note: les noms 'theme', 'intent', 'sentiment' doivent correspondre
        # aux clés dans votre fichier search_prompts.yaml
        self.theme_classifier = self._create_classifier_from_yaml('theme')
        self.intent_classifier = self._create_classifier_from_yaml('intent')
        self.sentiment_classifier = self._create_classifier_from_yaml('sentiment')

    def _create_classifier_from_yaml(self, classifier_name: str) -> Dict[str, Any]:
        """Crée un classifieur (chaîne LangChain) à partir de sa configuration YAML."""
        config = self.prompts_config.get('classifiers', {}).get(classifier_name, {})
        if not config:
            raise ValueError(f"Configuration pour '{classifier_name}' non trouvée dans {self.prompts_file}")

        response_schemas = [ResponseSchema(**s) for s in config.get('output_schema', [])]
        parser = StructuredOutputParser.from_response_schemas(response_schemas)

        prompt_template_str = config.get('prompt_template', '')
        # IMPORTANT: Activation de Jinja2 pour interpréter les `{% if %}`
        prompt = ChatPromptTemplate.from_template(
            template=prompt_template_str,
            template_format="jinja2" 
        )
        prompt = prompt.partial(format_instructions=parser.get_format_instructions())

        return {
            "prompt": prompt,
            "parser": parser,
            "chain": prompt | self.llm | parser
        }

    def _get_available_intents(self, content_type: str) -> List[str]:
        """Récupère les intentions disponibles en fonction du type de contenu."""
        intents_data = self.post_intents_data if content_type == 'post' else self.comment_intents_data
        theme_key = "veille_exterieure"
        
        try:
            for theme_info in intents_data.get("themes", []):
                if theme_key in theme_info:
                    return theme_info[theme_key].get("intents", [])
            return []
        except Exception as e:
            print(f"Erreur lors de la récupération des intentions pour {content_type}: {e}")
            return []

    async def analyze_content(self, 
                              content_type: str, 
                              text: str, 
                              post_text: Optional[str] = None, 
                              post_analysis: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyse un contenu (post ou commentaire) et retourne sa classification complète.
        """
        if content_type not in ['post', 'comment']:
            raise ValueError("content_type doit être 'post' ou 'comment'")
        
        # Prépare les variables pour les prompts
        post_analysis_str = json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "{}"
        
        # 1. Classification du thème principal
        available_themes_list = self.themes_data.get("themes", [])
        theme_result = await self.theme_classifier["chain"].ainvoke({
            "content_type": content_type,
            "text": text,
            "post_text": post_text,
            "post_analysis": post_analysis_str,
            "available_themes": json.dumps(available_themes_list, ensure_ascii=False, indent=2)
        })
        
        # 2. Analyse du sentiment (peut se faire en parallèle)
        sentiment_result = await self.sentiment_classifier["chain"].ainvoke({
            "content_type": content_type,
            "text": text,
            "post_text": post_text,
            "post_analysis": post_analysis_str,
        })
        
        # 3. Classification de l'intention
        available_intents = self._get_available_intents(content_type)
        parent_post_info = post_analysis.get('theme', {}) if post_analysis else {}

        intent_result = await self.intent_classifier["chain"].ainvoke({
            "content_type": content_type,
            "text": text,
            "parent_post_info": parent_post_info, # Pour la variable {{parent_post_info.theme_name}}
            "available_intents": ", ".join(available_intents)
        })

        # 4. Assemblage du résultat final
        result = {
            "theme": {"id": theme_result.get("theme_id"), "name": theme_result.get("theme_name")},
            "sentiment": {
                "value": sentiment_result.get("sentiment"),
                "polarity_score": float(sentiment_result.get("polarity_score", 0.0)),
                "confidence": float(sentiment_result.get("confidence", 0.0))
            },
            "intent": {
                "name": intent_result.get("intent"),
                "confidence": float(intent_result.get("confidence", 0.0))
            },
            # La confiance globale est le minimum des confiances individuelles
            "confidence": min(
                float(theme_result.get("confidence", 0.0)),
                float(sentiment_result.get("confidence", 0.0)),
                float(intent_result.get("confidence", 0.0))
            )
        }
        
        return result

    def analyze_content_sync(self, *args, **kwargs) -> Dict[str, Any]:
        """Version synchrone de l'analyse de contenu."""
        return asyncio.run(self.analyze_content(*args, **kwargs))

# --- EXEMPLE D'UTILISATION ET TEST ---
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyDroS___71S2NH_Qz08fuZBkJeX0s21dCY"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return

    try:
        analyzer = SearchPostAnalyzer(google_api_key=GOOGLE_API_KEY)
        
        print("=== Scénario: Analyse d'un Post et de ses Commentaires ===\n")
        
        post_content = "طريقة كفاش تسلف من إنوي✅🤩"
        comments = [
            "شكرا بزاف، هادشي اللي كنت كنقلب عليه",
            "واش فابور ولا بالفلوس؟",
            "مخداماش ليا هاد الطريقة، كتكذبو علينا"
        ]
        
        print(f"📝 ANALYSE DU POST :\n'{post_content}'")
        print("-" * 50)
        
        post_analysis_result = await analyzer.analyze_content("post", post_content)
        print(f"Résultat Post: {json.dumps(post_analysis_result, ensure_ascii=False, indent=2)}")
        
        print("\n" + "="*70 + "\n")
        
        for i, comment in enumerate(comments, 1):
            print(f"💬 ANALYSE DU COMMENTAIRE {i} :\n'{comment}'")
            print("-" * 50)
            
            comment_analysis_result = await analyzer.analyze_content(
                content_type="comment",
                text=comment,
                post_text=post_content,
                post_analysis=post_analysis_result
            )
            print(f"Résultat Commentaire {i}: {json.dumps(comment_analysis_result, ensure_ascii=False, indent=2)}")
            print("\n" + "="*50 + "\n")
            
    except Exception as e:
        print(f"Une erreur est survenue: {e}")

if __name__ == "__main__":
    asyncio.run(main())