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
            model="gemini-2.0-flash-lite", 
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

    def setup_analyzers(self):
        """Configuration des analyseurs (topic, intent, sentiment) à partir du fichier YAML."""
        self.topic_classifier = self._create_classifier_from_yaml('topic')
        self.intent_classifier = self._create_classifier_from_yaml('intent')
        self.sentiment_classifier = self._create_classifier_from_yaml('sentiment')
        self.relevance_analyzer = self._create_classifier_from_yaml('relevance_analyzer')

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
        
    async def _analyze_relevance(self, content_type: str, text: str, result: Dict[str, Any], post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """Analyse la pertinence du contenu selon son type."""
        try:
            # Convertit l'analyse du post en une chaîne de caractères.
            # Fournit "N/A" si l'analyse n'existe pas (cas d'un post).
            post_analysis_str = json.dumps(post_analysis, ensure_ascii=False, indent=2) if post_analysis else "N/A"

            # Prépare TOUS les paramètres que le template pourrait demander.
            # Même si post_text et post_analysis ne sont pas utilisés pour un "post",
            # LangChain exige qu'ils soient présents car ils existent dans le template.
            invoke_params = {
                "content_type": content_type,
                "text": text,
                "post_text": post_text or "N/A",  # Fournit une valeur par défaut si post_text est vide
                "post_analysis": post_analysis_str
            }

            # L'appel à ainvoke aura maintenant toutes les variables nécessaires.
            relevance_result = await self.relevance_analyzer["chain"].ainvoke(invoke_params)
            
            # La suite de la logique reste la même
            if content_type == "post":
                # Le parser ne devrait retourner que 'general_relevance' pour un post
                result["relevance"] = {
                    "general_relevance": relevance_result.get("general_relevance", False) # Mettre une valeur par défaut booléenne est plus sûr
                }
            else:  # content_type == "comment"
                result["relevance"] = {
                    "relevance_post": relevance_result.get("relevance_post", False),
                    "general_relevance": relevance_result.get("general_relevance", False)
                }
            
            relevance_confidence = relevance_result.get("confidence", 0.0)
            result["confidence"] = min(result.get("confidence", 1.0), relevance_confidence)
            
        except Exception as e:
            print(f"Erreur lors de l'analyse de pertinence: {e}")
            if content_type == "post":
                result["relevance"] = { "general_relevance": "unknown" }
            else:
                result["relevance"] = { "relevance_post": "unknown", "general_relevance": "unknown" }
        
        return result

    async def analyze_content(self, content_type: str, text: str, post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Analyse un contenu externe. Pour un "post", la pertinence est vérifiée en premier.
        """
        # --- LOGIQUE POUR LES POSTS (OPTIMISÉE) ---
        if content_type == "post":
            # 1. On analyse la pertinence EN PREMIER
            result = {}
            relevance_analysis = await self.relevance_analyzer["chain"].ainvoke({
                "content_type": "post",
                "text": text,
                "post_text": "N/A",
                "post_analysis": "N/A"
            })
            result["relevance"] = {
                "general_relevance": relevance_analysis.get("general_relevance", False)
            }
            result["confidence"] = relevance_analysis.get("confidence", 0.0)

            # 2. Si le post n'est pas pertinent, on s'arrête là.
            if not result.get("relevance", {}).get("general_relevance"):
                print("  [ANALYSE] Post jugé non pertinent. Analyse arrêtée.")
                result['topic'] = {'id': 'not_relevant', 'name': 'Non pertinent'}
                return result # Arrêt précoce pour économiser les appels API

            # 3. Si le post EST pertinent, on continue l'analyse (thème, intention, etc.)
            print("  [ANALYSE] Post pertinent. Poursuite de l'analyse (thème, intention)...")
            
            # Analyse du thème
            topic_result = await self._analyze_topic("post", text, "", None) # _analyze_topic retourne déjà le bon format
            result.update(topic_result) # On fusionne le résultat du thème

            # Analyse de l'intention si le thème est valide
            topic_id = result.get("topic", {}).get("id")
            if topic_id and topic_id not in ["none", "error"]:
                 result = await self._classify_intent(
                    content_type="post",
                    text=text,
                    result=result
                )
            
            return result

        # --- LOGIQUE POUR LES COMMENTAIRES (RESTE INCHANGÉE) ---
        elif content_type == "comment":
            # Pour un commentaire, l'ordre original est plus logique
             # 1. Analyse du Thème du commentaire, en utilisant le post comme contexte.
            result = await self._analyze_topic(
                content_type="comment",
                text=text,
                post_text=post_text,
                post_analysis=post_analysis
            )
            
            # 2. Analyse de la Pertinence du commentaire (par rapport au post et en général).
            result = await self._analyze_relevance(
                content_type="comment",
                text=text,
                result=result,
                post_text=post_text,
                post_analysis=post_analysis
            )
    
            # 3. Analyse du Sentiment du commentaire.
            result = await self._analyze_sentiment(
                content_type="comment",
                text=text,
                post_text=post_text,
                post_analysis=post_analysis,
                result=result
            )
    
            # 4. Analyse de l'Intention du commentaire.
            topic_id = result.get("topic", {}).get("id")
            if topic_id and topic_id not in ["none", "error"]:
                result = await self._classify_intent(
                    content_type="comment",
                    text=text,
                    result=result,
                    post_text=post_text,
                    post_analysis=post_analysis
                )
            else:
                result["intent"] = {'name': 'N/A'}
                
            print("  [ANALYSE] Analyse du commentaire terminée.")
            
            return result
        
        else:
            raise ValueError(f"Type de contenu inconnu: {content_type}")

    def analyze_content_sync(self, content_type: str, text: str, post_text: str = "", post_analysis: Optional[Dict] = None) -> Dict[str, Any]:
        """Version synchrone de l'analyse de contenu."""
        return asyncio.run(self.analyze_content(content_type, text, post_text, post_analysis))

async def main():
    # Remplacez par votre clé API valide
    GOOGLE_API_KEY = "AIzaSyCSF8QXA4bIxQvdCUlFWZqzGOxYRM6fJKM" 
    
    if "VOTRE_CLE_API" in GOOGLE_API_KEY:
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return


    # Définition des ID des pages officielles à ignorer
    OFFICIAL_PAGE_IDS = {
        "100064944541507", 
        "100050531557552", 
        "100064662374992", 
        "100069357321773", 
        "100070238800377c" 
    }

    # Chemin vers le fichier de données
    DATA_FILE_PATH = "/home/doha/Desktop/SocialMedia_Data_Analysis/data/external/facebook_data_backup/inwi_إنوي_facebook_data_20250605_195731.json"

    try:
        analyzer = ExternalContentAnalyzer(google_api_key=GOOGLE_API_KEY)
        
        print(f"📖 Chargement des données depuis : {DATA_FILE_PATH}\n")
        with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
            social_media_data = json.load(f)

        posts_to_process = social_media_data.get("data", [])
        total_posts = len(posts_to_process)
        print(f"✅ Fichier chargé. Nombre total de posts trouvés : {total_posts}\n")

        if not posts_to_process:
            print("❌ Erreur: Le fichier ne contient pas de clé 'data' ou elle est vide.")
            return

        posts_analyzed_count, posts_skipped_count, posts_error_count = 0, 0, 0

        for i, post_data in enumerate(posts_to_process):
            print("\n" + "="*80)
            print(f"--- [POST {i+1}/{total_posts}] --- Vérification du post ID: {post_data.get('post_id', 'N/A')}")
            
            page_id = post_data.get("page_id")
            if page_id and str(page_id) in OFFICIAL_PAGE_IDS:
                print(f"  [ACTION] Post ignoré : provient d'une page officielle (Page ID: {page_id}).")
                posts_skipped_count += 1
                continue

            try:
                post_content = post_data.get("message") or post_data.get("caption")
                if not post_content:
                    print(f"  [ACTION] Post ignoré : aucun contenu textuel ('message' ou 'caption') trouvé.")
                    posts_skipped_count += 1
                    continue

                print(f"  [ACTION] Analyse du post...")

                print("\n" + "-"*60)
                print(f"  CONTENU DU POST À ANALYSER:")
                print(f"  {post_content}")
                print("-" * 60)

                # Analyse du post principal
                post_analysis = await analyzer.analyze_content(content_type="post", text=post_content)
                
                print("\n  ---------- RÉSULTAT DE L'ANALYSE DU POST ----------")
                print(json.dumps(post_analysis, ensure_ascii=False, indent=2))
                print("  ----------------------------------------------------")

                # On n'analyse les commentaires que si le post parent était pertinent
                is_post_relevant = post_analysis.get("relevance", {}).get("general_relevance")
                if not is_post_relevant:
                    print("\n  [INFO] Le post parent étant non pertinent, les commentaires ne seront pas analysés.")
                    posts_analyzed_count += 1
                    continue


                # Analyse des commentaires
                comments = post_data.get("comments", [])
                if comments:
                    print(f"\n  [INFO] Analyse de {len(comments)} commentaire(s) associé(s)...")
                    for j, comment_data in enumerate(comments):
                        comment_content = comment_data.get("message")
                        if not comment_content:
                            continue

                        print("\n" + "    " + "-"*50)
                        print(f"    CONTENU DU COMMENTAIRE {j+1}/{len(comments)} À ANALYSER:")
                        print(f"    '{comment_content}'")
                        print("    " + "-"*50)
                        
                        comment_analysis = await analyzer.analyze_content(
                            content_type="comment",
                            text=comment_content,
                            post_text=post_content,
                            post_analysis=post_analysis
                        )
                        
                        print(f"\n    --- Résultat du Commentaire {j+1}/{len(comments)} ---")
                        print(json.dumps(comment_analysis, ensure_ascii=False, indent=4)) 
                        
                posts_analyzed_count += 1

            except Exception as e:
                print(f"  [ERREUR] Une erreur est survenue lors du traitement du post {i+1}. Il sera ignoré.")
                print(f"  Détail de l'erreur: {e}")
                posts_error_count += 1
                continue
        
        print("\n" + "="*80)
        print("🎉 Traitement terminé. Résumé :")
        print(f"   - Posts analysés avec succès : {posts_analyzed_count}")
        print(f"   - Posts ignorés (page officielle ou sans texte) : {posts_skipped_count}")
        print(f"   - Posts en erreur : {posts_error_count}")
        print(f"   - Total vérifié : {posts_analyzed_count + posts_skipped_count + posts_error_count} / {total_posts}")
        print("="*80)

    except Exception as e:
        print(f"❌ Une erreur critique est survenue: {e}")

if __name__ == "__main__":
    asyncio.run(main())