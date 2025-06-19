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
                 topics_file="config/themes/general_themes.json",
                 intents_post_file="config/themes/posts_intents.json",
                 intents_comments_file="config/themes/comments_intents.json",
                 prompts_file="config/prompts/search_prompts.yaml"):
        """
        Initialise l'analyseur de contenu externe (veille extérieure).
        
        Args:
            google_api_key: Clé API Google Gemini.
            topics_file: Chemin vers le fichier JSON des thèmes généraux.
            intents_post_file: Chemin vers le fichier JSON des intentions des posts.
            intents_comments_file: Chemin vers le fichier JSON des intentions des commentaires.
            prompts_file: Chemin vers le fichier YAML des prompts structurés.
        """
        self.topics_file = topics_file
        self.intents_posts_file = intents_post_file
        self.intents_comments_file = intents_comments_file
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key

        # Chargement des configurations
        self.topics_data = self._load_json_config(self.topics_file)
        self.intents_posts_data = self._load_json_config(self.intents_posts_file)
        self.intents_comments_data = self._load_json_config(self.intents_comments_file)
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
    
    def _get_intents_for_topic(self, content_type: str, theme_id: str = "veille_exterieure" , topic_id : str =None) -> List[str]:
        """Récupère les intentions disponibles pour un thème donné"""
        try:
            if content_type == "post":
                for theme_data in self.intents_posts_data.get("themes", []):
                    if theme_id in theme_data:
                        return theme_data[theme_id].get("intents", [])
                return []
            else:
                intents_config = self.intents_comments_data.get("themes", [{}])[0].get("veille_exterieure", {})
                # Agréger toutes les intentions des sous-catégories
                all_intents = []
                for category in intents_config.values():
                    if isinstance(category, list):
                        all_intents.extend(category)
                return all_intents
            
        except Exception as e:
            print(f"Erreur lors de la récupération des intentions pour le topic {theme_id}: {e}")
            return []
        
        
    async def _classify_intent(self, content_type,text: str, result: Dict[str, Any] ,post_text: str = "", post_analysis: str = "",theme_id:str ="veille_exterieure", topic_id: str =None) -> Dict[str, Any]:
        """Classifie l'intention du post basée sur le thème identifié"""
        try:
            # Récupérer les intentions disponibles pour ce thème
            available_intents = self._get_intents_for_topic("post","veille_exterieure")
            
            if not available_intents:
                print(f"Aucune intention trouvée pour le topic: {theme_id}")
                return result
            
            # Classification de l'intention
            intent_result = await self.intent_classifier["chain"].ainvoke({
                "content_type": content_type,
                "post_text": post_text,
                "post_analysi": post_analysis,
                "text": text,
                "topic_name": result["topic"]["name"],
                "available_intents": ", ".join(available_intents)
            })
            intent_confidence = intent_result.get("confidence", 0.0)

            # Ajouter l'intention au résultat
            result["intent"] = {
                "name": intent_result["intent"],
            }

            # Mettre à jour la confiance globale
            result["confidence"] = min(result.get("confidence", 1.0), intent_confidence)
            
            
        except Exception as e:
            print(f"Erreur lors de la classification d'intention: {e}")
            result["intent"] = {
                "name": "unknown",
            }
        
        return result
    
    async def _analyze_relevance(self, content_type,text: str, result: Dict[str, Any], topic_id: str ,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:


        return result
    
    async def _analyze_sentiment(self, content_type: str, text: str, post_text: str, post_analysis: Optional[Dict], result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyse le sentiment d'un contenu (généralement un commentaire)."""
        try:
            sentiment_result = await self.sentiment_classifier["chain"].ainvoke({
                "content_type": content_type,
                "text": text,
                "post_text": post_text,
                "post_analysis": post_analysis
            })
            sentiment_confidence = sentiment_result.get("confidence", 0.0)
            result["sentiment"] = {
                "value": sentiment_result.get("sentiment"),
                "polarity_score": sentiment_result.get("polarity_score", 0.0)
            }

            # La confiance globale devient le minimum de la confiance actuelle et de celle du sentiment.
            result["confidence"] = min(result.get("confidence", 1.0), sentiment_confidence)

        except Exception as e:
            print(f"Erreur lors de l'analyse du sentiment: {e}")
            result["sentiment"] = {
                "value": "unknown",
                "polarity_score": 0.0
            }
        return result
    
    async def _analyze_topic(self, content_type: str, text: str, post_text: str, post_analysis: Optional[Dict]) -> Dict[str, Any]:
        """Analyse le thème (topic) d'un contenu et retourne le dictionnaire de résultat initial."""
        try:
            post_analysis_str = json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "N/A"
            available_topics = self.topics_data.get("topics", [])

            topic_result = await self.topic_classifier["chain"].ainvoke({
                "content_type": content_type,
                "text": text,
                "post_text": post_text,
                "post_analysis": post_analysis_str,
                "available_topics": json.dumps(available_topics, ensure_ascii=False, indent=2)
            })
            

            return {
                "topic": {
                    "id": topic_result.get("topic_id"),
                    "name": topic_result.get("topic_name")
                },
                "confidence": topic_result.get("confidence", 0.0)
            }
        except Exception as e:
            print(f"Erreur lors de la classification du topic: {e}")
            return {
                "topic": {"id": "error", "name": "error"},
            }


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

        # 1. Analyse du Topic : C'est la première étape, elle crée le dictionnaire de résultat.
        result = await self._analyze_topic(
            content_type=content_type,
            text=text,
            post_text=post_text,
            post_analysis=post_analysis
        )

        # 2. Classification de l'Intention : Uniquement si un thème pertinent est trouvé.
        topic_id = result.get("topic", {}).get("id")
        if topic_id and topic_id != "none" and topic_id != "error":
            result = await self._classify_intent(
                content_type=content_type,
                text=text,
                post_text=post_text,
                post_analysis=post_analysis,
                result=result
            )
        
        # 3. Analyse du Sentiment (Conditionnelle, maintenant dans sa propre fonction)
        if content_type == "comment":
            result = await self._analyze_sentiment(
                content_type=content_type,
                text=text,
                post_text=post_text,
                post_analysis=post_analysis,
                result=result
            )

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
            intents_post_file="config/themes/posts_intents.json",
            intents_comments_file="config/themes/comments_intents.json",
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