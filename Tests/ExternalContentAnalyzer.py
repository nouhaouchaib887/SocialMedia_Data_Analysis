import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
# from langchain_core.runnables import RunnablePassthrough # Cet import n'est plus utilisé
from typing import Dict, Any, List, Optional
import asyncio
import yaml
# from jinja2 import Template # ### CORRECTION: Cet import est supprimé car LangChain gère le templating

class ExternalContentAnalyzer:
    def __init__(self, google_api_key: str,
                 topics_file="config/themes/general_themes.json",
                 intents_post_file="config/themes/posts_intents.json",
                 intents_comments_file="config/themes/comments_intents.json",
                 prompts_file="config/prompts/search_prompts.yaml"):
        """
        Initialise l'analyseur de contenu externe (veille extérieure).
        """
        self.topics_file = topics_file
        self.intents_posts_file = intents_post_file
        self.intents_comments_file = intents_comments_file
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key

        self.topics_data = self._load_json_config(self.topics_file)
        self.intents_posts_data = self._load_json_config(self.intents_posts_file)
        self.intents_comments_data = self._load_json_config(self.intents_comments_file)
        self.prompts_config = self._load_prompts_config()
        
        self._setup_api_key()
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash", 
            temperature=0.1,
            google_api_key=self.google_api_key
        )
        
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

    # ### CORRECTION: Cette méthode est supprimée car elle est la cause du problème.
    # def _render_prompt_template(self, template_str: str, **kwargs) -> str:
    #     """Utilise Jinja2 pour rendre le template avec les variables."""
    #     template = Template(template_str)
    #     return template.render(**kwargs)

    def setup_analyzers(self):
        """Configuration des analyseurs (topic, intent, sentiment) à partir du fichier YAML."""
        self.topic_classifier = self._create_classifier_from_yaml('topic')
        self.intent_classifier = self._create_classifier_from_yaml('intent')
        self.sentiment_classifier = self._create_classifier_from_yaml('sentiment')
        self.relevance_analyzer = self._create_classifier_from_yaml('relevance_analyzer')

    # ### CORRECTION MAJEURE: La méthode de création de classifieur est modifiée ici.
    def _create_classifier_from_yaml(self, classifier_name: str):
        """Crée un classifieur générique à partir de sa configuration dans le fichier YAML."""
        classifier_config = self.prompts_config.get('classifiers', {}).get(classifier_name)
        if not classifier_config:
            raise ValueError(f"Configuration pour '{classifier_name}' non trouvée dans {self.prompts_file}")

        output_schema_config = classifier_config.get('output_schema', [])
        response_schemas = [ResponseSchema(**item) for item in output_schema_config]
        parser = StructuredOutputParser.from_response_schemas(response_schemas)

        prompt_template_str = classifier_config.get('prompt_template', '')
        
        # On ne pré-rend plus le template. On le passe directement à LangChain
        # en lui spécifiant d'utiliser le moteur Jinja2.
        prompt = ChatPromptTemplate.from_template(
            template=prompt_template_str,
            template_format="jinja2"  # C'est la ligne la plus importante de la correction.
        )
        
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
                all_intents = []
                for category in intents_config.values():
                    if isinstance(category, list):
                        all_intents.extend(category)
                return all_intents
            
        except Exception as e:
            print(f"Erreur lors de la récupération des intentions pour le topic {theme_id}: {e}")
            return []
        
    # ### CORRECTION: La structure des données passées à ainvoke est modifiée pour correspondre au template.
    async def _classify_intent(self, content_type,text: str, result: Dict[str, Any] ,post_text: str = "", post_analysis: str = "",theme_id:str ="veille_exterieure", topic_id: str =None) -> Dict[str, Any]:
        """Classifie l'intention du post basée sur le thème identifié"""
        try:
            available_intents = self._get_intents_for_topic(content_type, "veille_exterieure")
            
            if not available_intents:
                print(f"Aucune intention trouvée pour le topic: {theme_id}")
                return result
            
            # Le template YAML attend {{ parent_post_info.topic_name }}. 
            # Il faut donc créer cette structure imbriquée.
            parent_info = {"topic_name": result.get("topic", {}).get("name", "N/A")}

            intent_result = await self.intent_classifier["chain"].ainvoke({
                "content_type": content_type,
                "post_text": post_text,
                "post_analysis": post_analysis,
                "text": text,
                "parent_post_info": parent_info, # Correction ici
                "available_intents": ", ".join(available_intents)
            })
            intent_confidence = intent_result.get("confidence", 0.0)

            result["intent"] = { "name": intent_result["intent"] }
            result["confidence"] = min(result.get("confidence", 1.0), intent_confidence)
            
        except Exception as e:
            print(f"Erreur lors de la classification d'intention: {e}")
            result["intent"] = { "name": "unknown" }
        
        return result
    
    # ### CORRECTION: Suppression de la méthode vide 'c' pour nettoyer le code.
    # async def c(self, ...):
    #     return result
    
    async def _analyze_sentiment(self, content_type: str, text: str, post_text: str, post_analysis: Optional[Dict], result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyse le sentiment d'un contenu (généralement un commentaire)."""
        try:
            # Conversion de l'analyse du post en string pour le prompt
            post_analysis_str = json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "N/A"
            
            sentiment_result = await self.sentiment_classifier["chain"].ainvoke({
                "content_type": content_type,
                "text": text,
                "post_text": post_text,
                "post_analysis": post_analysis_str
            })
            sentiment_confidence = sentiment_result.get("confidence", 0.0)
            result["sentiment"] = {
                "value": sentiment_result.get("sentiment"),
                "emotion": sentiment_result["emotion"],
                "polarity_score": sentiment_result.get("polarity_score", 0.0)
            }

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
        
    async def _analyze_relevance(self,text: str, result: Dict[str, Any], post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Analyse la pertinence du contenu."""
        try:
            post_analysis_str = json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "N/A"

            relevance_result = await self.relevance_analyzer["chain"].ainvoke({
                "post_text": post_text,
                "post_analysis":  post_analysis_str,
                "text":text 
            })
        
            result["relevance"] = {
                "relevance_post": relevance_result.get("relevance_post", "unknown"),
                "general_relevance": relevance_result.get("general_relevance", "unknown")
            }
            relevance_confidence = relevance_result.get("confidence", 0.0)
            result["confidence"] = min(result.get("confidence", 1.0), relevance_confidence)
            
        except Exception as e:
            print(f"Erreur lors de l'analyse de pertinence: {e}")
            result["relevance"] = {
                "relevance_post": "unknown",
                "general_relevance": "unknown",
                "confidence": 0.0
            }
        
        return result

    async def analyze_content(self, content_type: str, text: str, post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Analyse un contenu externe (post ou commentaire) et retourne sa classification complète.
        """
        result = {}

        result = await self._analyze_topic(
            content_type=content_type,
            text=text,
            post_text=post_text,
            post_analysis=post_analysis
        )

        topic_id = result.get("topic", {}).get("id")
        if topic_id and topic_id != "none" and topic_id != "error":
            result = await self._classify_intent(
                content_type=content_type,
                text=text,
                post_text=post_text,
                post_analysis=post_analysis,
                result=result
            )
        
        if content_type == "comment":
            result = await self._analyze_sentiment(
                content_type=content_type,
                text=text,
                post_text=post_text,
                post_analysis=post_analysis,
                result=result
            )
            result = await self._analyze_relevance(
                text=text, 
                result=result,
                post_text=post_text, 
                post_analysis=post_analysis
            )

        return result

    def analyze_content_sync(self, content_type: str, text: str, post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """Version synchrone de l'analyse de contenu."""
        return asyncio.run(self.analyze_content(content_type, text, post_text, post_analysis))

# Le reste du fichier main() est déjà correct et n'a pas besoin de modification.
async def main():
    GOOGLE_API_KEY = "AIzaSyDroS___71S2NH_Qz08fuZBkJeX0s21dCY" # Remplacez par votre clé
    
    if "VOTRE_CLE_API" in GOOGLE_API_KEY:
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return

    try:
        analyzer = ExternalContentAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            topics_file="config/themes/general_themes.json",
            intents_post_file="config/themes/posts_intents.json",
            intents_comments_file="config/themes/comments_intents.json",
            prompts_file="config/prompts/search_prompts.yaml"
        )
        
        post_content = "طريقة كفاش تسلف من إنوي✅🤩"
        comments = [
            "خويا عفاك أنا عندي أورنج، واش كاينة شي طريقة؟",
            "نصابة، جربتها و ما خدماتش ليا و داو ليا كاع الصولد",
            "شكرا بزاف على المعلومة، مفيدة جدا"
        ]
        
        print("=== Analyse d'un Post Externe et ses Commentaires ===\n")
        
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
                        post_analysis=post_analysis
                    )
                    print(f"Résultat Commentaire {i}: {json.dumps(comment_analysis, ensure_ascii=False, indent=2)}")
                except Exception as e:
                    print(f"Erreur lors de l'analyse du commentaire {i}: {e}")
                
                print("\n" + "="*50 + "\n")
            
    except Exception as e:
        print(f"Erreur d'initialisation ou d'exécution: {e}")

if __name__ == "__main__":
    asyncio.run(main())