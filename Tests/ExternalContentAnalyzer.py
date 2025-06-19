import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_core.runnables import RunnablePassthrough
from typing import Dict, Any, List, Optional
import asyncio
import yaml
from jinja2 import Template

class ExternalContentAnalyzer:
    def __init__(self, google_api_key: str, 
                 topics_file: str = "../config/themes/general_themes.json",
                 intents_file: str = "../config/themes/comments_intents.json", 
                 prompts_file: str = "../config/prompts/search_prompts.yaml"):
        """
        Initialise l'analyseur de contenu externe (veille extérieure).
        
        Args:
            google_api_key: Clé API Google Gemini.
            topics_file: Chemin vers le fichier JSON des thèmes généraux.
            intents_file: Chemin vers le fichier JSON des intentions des commentaires.
            prompts_file: Chemin vers le fichier YAML des prompts structurés.
        """
        self.topics_file = topics_file
        self.intents_file = intents_file
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key

        # Chargement des configurations
        self.topics_data = self._load_json_config(self.topics_file)
        self.intents_data = self._load_json_config(self.intents_file)
        self.prompts_config = self._load_prompts_config()
        
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
        """Configure la clé API Google."""
        if not self.google_api_key:
            raise ValueError("La clé API Google est requise. Veuillez fournir une clé API valide.")
        os.environ["GOOGLE_API_KEY"] = self.google_api_key

    def _load_json_config(self, file_path: str) -> Dict[str, Any]:
        """Charge une configuration depuis un fichier JSON."""
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
        """Charge la configuration des prompts depuis le fichier YAML."""
        try:
            with open(self.prompts_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration des prompts {self.prompts_file} non trouvé.")
            raise
        except yaml.YAMLError as e:
            print(f"Erreur de parsing YAML: {e}")
            raise

    def _render_prompt_template(self, template_str: str, **kwargs) -> str:
        """Utilise Jinja2 pour rendre le template avec les variables."""
        template = Template(template_str)
        return template.render(**kwargs)

    def setup_analyzers(self):
        """Configuration des analyseurs (topic, intent, sentiment) à partir du fichier YAML."""
        self.topic_classifier = self._create_classifier_from_yaml('topic')
        self.intent_classifier = self._create_classifier_from_yaml('intent')
        self.sentiment_classifier = self._create_classifier_from_yaml('sentiment')

    def _create_classifier_from_yaml(self, classifier_name: str):
        """Crée un classifieur générique à partir de sa configuration dans le fichier YAML."""
        classifier_config = self.prompts_config.get('classifiers', {}).get(classifier_name)
        if not classifier_config:
            raise ValueError(f"Configuration pour '{classifier_name}' non trouvée dans {self.prompts_file}")

        # Création du parser de sortie à partir du schéma YAML
        output_schema_config = classifier_config.get('output_schema', [])
        response_schemas = [ResponseSchema(**item) for item in output_schema_config]
        parser = StructuredOutputParser.from_response_schemas(response_schemas)

        # Création du template de prompt
        prompt_template_str = classifier_config.get('prompt_template', '')
        
        # Placeholders pour le rendu Jinja2 initial
        # Les vraies valeurs seront injectées lors de l'appel `ainvoke`
        placeholders = {
            "content_type": "{content_type}",
            "text": "{text}",
            "post_text": "{post_text}",
            "post_analysis": "{post_analysis}",
            "parent_post_info": "{parent_post_info}", # Spécifique pour l'intention
            "available_topics": "{available_topics}", # Spécifique au topic
            "available_intents": "{available_intents}", # Spécifique à l'intention
            "format_instructions": "{format_instructions}"
        }
        
        rendered_prompt_str = self._render_prompt_template(prompt_template_str, **placeholders)
        
        prompt = ChatPromptTemplate.from_template(rendered_prompt_str)
        prompt = prompt.partial(format_instructions=parser.get_format_instructions())

        return {
            "prompt": prompt,
            "parser": parser,
            "chain": prompt | self.llm | parser
        }

    def _get_intents_for_veille_exterieure(self, content_type: str) -> List[str]:
        """Récupère les intentions disponibles pour la veille extérieure."""
        try:
            intents_config = self.intents_data.get("comment_intents", [{}])[0].get("veille_exterieure", {})
            if content_type == "comment":
                # Agréger toutes les intentions des sous-catégories
                all_intents = []
                for category in intents_config.values():
                    if isinstance(category, list):
                        all_intents.extend(category)
                return all_intents
            else: # post
                # Pour les posts, les intentions sont définies directement dans le prompt YAML.
                # On peut retourner une liste indicative ou vide. Ici on retourne les intentions de commentaires pour l'exemple.
                # L'idéal serait de les définir aussi dans le JSON.
                return ["Information", "Feedback_Request", "Critique"] # Exemple, comme dans votre YAML
        except Exception as e:
            print(f"Erreur lors de la récupération des intentions pour la veille extérieure: {e}")
            return []

    async def analyze_content(self, content_type: str, text: str, post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Analyse un contenu externe (post ou commentaire) et retourne sa classification complète.
        
        Args:
            content_type: 'post' ou 'comment'.
            text: Le contenu textuel à analyser.
            post_text: Le texte du post parent (si `content_type` est 'comment').
            post_analysis: L'analyse déjà effectuée du post parent (si `content_type` est 'comment').
            
        Returns:
            Un dictionnaire contenant l'analyse complète. La clé 'sentiment' n'est présente que pour les commentaires.
        """
        result = {}

        # 1. Classification du Topic (toujours exécutée)
        available_topics = self.topics_data.get("topics", [])
        topic_result = await self.topic_classifier["chain"].ainvoke({
            "content_type": content_type,
            "text": text,
            "post_text": post_text,
            "post_analysis": json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "N/A",
            "available_topics": json.dumps(available_topics, ensure_ascii=False, indent=2)
        })
        result["topic"] = {
            "id": topic_result.get("topic_id"),
            "name": topic_result.get("topic_name")
        }
        result["confidence"] = topic_result.get("confidence", 0.0)

        # 2. Classification de l'Intention (toujours exécutée)
        available_intents = self._get_intents_for_veille_exterieure(content_type)
        parent_post_info = post_analysis.get("topic", {}) if post_analysis else {}
        
        intent_result = await self.intent_classifier["chain"].ainvoke({
            "content_type": content_type,
            "text": text,
            "parent_post_info": parent_post_info,
            "available_intents": ", ".join(available_intents)
        })
        result["intent"] = {
            "name": intent_result.get("intent"),
            "confidence": intent_result.get("confidence", 0.0)
        }
        
        # 3. Analyse du Sentiment (Conditionnelle, uniquement pour les commentaires)
        if content_type == "comment":
            sentiment_result = await self.sentiment_classifier["chain"].ainvoke({
                "content_type": content_type,
                "text": text,
                "post_text": post_text,
                "post_analysis": json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "N/A",
            })
            result["sentiment"] = {
                "value": sentiment_result.get("sentiment"),
                "confidence": sentiment_result.get("confidence", 0.0),
                "polarity_score": sentiment_result.get("polarity_score", 0.0)
            }
        
        # L'ancien bloc `else` qui ajoutait `sentiment: None` a été supprimé.
        # La clé 'sentiment' ne sera donc ajoutée au dictionnaire que si le contenu est un commentaire.

        return result

    def analyze_content_sync(self, content_type: str, text: str, post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """Version synchrone de l'analyse de contenu."""
        return asyncio.run(self.analyze_content(content_type, text, post_text, post_analysis))

# --- Exemple d'utilisation et test ---
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyDroS___71S2NH_Qz08fuZBkJeX0s21dCY"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return

    try:
        # Initialiser l'analyseur
        analyzer = ExternalContentAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            topics_file="config/themes/general_themes.json",
            intents_file="config/themes/comments_intents.json",
            prompts_file="config/prompts/search_prompts.yaml"
        )
        
        # === Exemple d'un Post et ses Commentaires ===
        post_content = "طريقة كفاش تسلف من إنوي✅🤩"
        comments = [
            "خويا عفاك أنا عندي أورنج، واش كاينة شي طريقة؟",
            "نصابة، جربتها و ما خدماتش ليا و داو ليا كاع الصولد",
            "شكرا بزاف على المعلومة، مفيدة جدا"
        ]
        
        print("=== Analyse d'un Post Externe et ses Commentaires ===\n")
        
        # 1. Analyser le post principal
        print("📝 ANALYSE DU POST PRINCIPAL:")
        print(f"Contenu: {post_content}")
        print("-" * 50)
        
        post_analysis = None
        try:
            post_analysis = await analyzer.analyze_content(content_type="post", text=post_content)
            print(f"Résultat Post: {json.dumps(post_analysis, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"Erreur lors de l'analyse du post: {e}")
        
        print("\n" + "="*70 + "\n")
        
        # 2. Analyser chaque commentaire en utilisant le contexte du post
        if post_analysis:
            for i, comment in enumerate(comments, 1):
                print(f"💬 ANALYSE DU COMMENTAIRE {i}:")
                print(f"Contenu: {comment}")
                print("-" * 50)
                
                try:
                    comment_analysis = await analyzer.analyze_content(
                        content_type="comment",
                        text=comment,
                        post_text=post_content,
                        post_analysis=post_analysis  # <-- Passage du contexte
                    )
                    print(f"Résultat Commentaire {i}: {json.dumps(comment_analysis, ensure_ascii=False, indent=2)}")
                except Exception as e:
                    print(f"Erreur lors de l'analyse du commentaire {i}: {e}")
                
                print("\n" + "="*50 + "\n")
            
    except Exception as e:
        print(f"Erreur d'initialisation ou d'exécution: {e}")

if __name__ == "__main__":
    asyncio.run(main())