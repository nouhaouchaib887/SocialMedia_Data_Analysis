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


class ContentAnalyzer:
    def __init__(self, google_api_key: str, brand_name: str, intents_file_posts: str = "../config/themes/posts_intents.json",intents_file_comments: str = "../config/themes/comments_intents.json", prompts_file: str = "../config/prompts/prompts_o.yaml"):
        """
        Initialise l'analyseur de posts Orange
        
        Args:
            google_api_key: Clé API Google Gemini
            brand_name: Nom de la marque
            intents_file: Chemin vers le fichier de configuration JSON des intentions
            prompts_file: Chemin vers le fichier YAML des prompts
        """
        self.config_file = f"../config/themes/{brand_name}_themes.json"
        self.intents_file_posts = intents_file_posts
        self.intents_file_comments = intents_file_comments
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key
        self.brand_name = brand_name
        
        # Chargement des configurations
        self.data = self._load_config()
        self.intents_data_comments = self._load_intents_config("comment")
        self.intents_data_posts= self._load_intents_config("post")

        self.prompts_config = self._load_prompts_config()
        
        # Configuration de la clé API Google
        self._setup_api_key()
        
        # Initialisation du modèle LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash", 
            temperature=0,
            google_api_key=self.google_api_key
        )
        
        # Configuration des analyseurs
        self.setup_analyzers()
    
    def _setup_api_key(self):
        """Configure la clé API Google"""
        if not self.google_api_key:
            raise ValueError("La clé API Google est requise. Veuillez fournir une clé API valide.")
        
        
    
    def _load_config(self) -> Dict[str, Any]:
        """Charge la configuration des thèmes depuis le fichier JSON"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration {self.config_file} non trouvé")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON: {e}")
            raise
    
    def _load_intents_config(self,content_type) -> Dict[str, Any]:
        """"Charge la configuration des intentions depuis le fichier JSON, en fonction du type de contenu.

            Args:
                    content_type (str): Le type de contenu pour lequel charger les intentions. 
                    Il peut s'agir, par exemple, de "comment" (pour les commentaires)
                    ou "post" (pour les publications).

            Returns:
                    Dict[str, Any]: Un dictionnaire représentant la configuration des intentions
                    (intents) correspondant au type de contenu fourni.
        """
        if content_type == "post":
            try:
                with open(self.intents_file_posts, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except FileNotFoundError:
                print(f"Fichier de configuration des intentions {self.intents_file_posts} non trouvé")
                raise
            except json.JSONDecodeError as e:
                print(f"Erreur de parsing JSON des intentions: {e}")
                raise
        else : 
            try:
                with open(self.intents_file_comments, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except FileNotFoundError:
                print(f"Fichier de configuration des intentions {self.intents_file_comments} non trouvé")
                raise
            except json.JSONDecodeError as e:
                print(f"Erreur de parsing JSON des intentions: {e}")
                raise
    
    def _load_prompts_config(self) -> Dict[str, Any]:
        """Charge la configuration des prompts depuis le fichier YAML

             Returns:
                Dict[str, Any]: Un dictionnaire représentant la configuration chargée.
        
        """
        try:
            with open(self.prompts_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration des prompts {self.prompts_file} non trouvé")
            raise
        except yaml.YAMLError as e:
            print(f"Erreur de parsing YAML: {e}")
            raise
    
    
    

    def setup_analyzers(self):
        """Configuration des analyseurs pour chaque niveau de classification"""
        # 1. Classificateur de thème principal avec prompt YAML
        self.theme_classifier = self._create_classifier_from_yaml("thematic")
        
        # 2. Classificateurs spécifiques par étapes (gardés comme avant pour les autres)
        self.offre_category_classifier = self._create_classifier_from_yaml("offer_category")
        self.offre_subcategory_classifier = self._create_classifier_from_yaml("offer_subcategory")
        self.offre_product_classifier = self._create_classifier_from_yaml("offer")
        
        self.initiative_classifier = self._create_classifier_from_yaml("initiative")
        self.initiative_event_classifier = self._create_classifier_from_yaml("initiative_event")
        
        self.communication_topic_classifier = self._create_classifier_from_yaml("communication_topic")
        self.communication_subtopic_classifier = self._create_classifier_from_yaml("communication_subtopic")
        
        # 3. Classificateur des intentions
        self.intent_classifier = self._create_classifier_from_yaml("intent")

        # 4. Analyseur de pertinence des commentaires
        self.relevance_analyzer = self._create_classifier_from_yaml("relevance_analyzer")

        # 5. Analyseur de sentiment
        self.sentiment_classifier = self._create_classifier_from_yaml('sentiment')

    
    def _create_classifier_from_yaml(self,classifier_name ):
        """Crée le classificateur de thème principal à partir du fichier YAML

        Args:
        classifier_name (str): Le nom du classificateur à charger.

        """
        # Récupération de la configuration du classificateur thématique
        classifier_config = self.prompts_config.get('classifiers', {}).get(classifier_name, {})
        
        if not classifier_config:
            raise ValueError(f"Configuration {classifier_name} non trouvée dans le fichier prompts YAML")
        
        # Configuration du schéma de réponse depuis YAML
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
        
        # Récupération du template de prompt depuis YAML
        prompt_template = classifier_config.get('prompt_template', '')
        
        if classifier_name == "offer_category":
            # Extraction des catégories d'offres depuis les données
            offre_theme = next((t for t in self.data["themes"] if t["id"] == "offre"), None)
            categories = []
            if offre_theme and "category_offre" in offre_theme:
                for category in offre_theme["category_offre"]:
                    categories.append({
                "id": category["id"],
                "name": category["name"],
                "keywords": category.get("keywords", [])
            })
                
        classifier_prompt = ChatPromptTemplate.from_template(
        template=prompt_template,
        template_format="jinja2"  # C'est cette ligne qui résout tout.
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
        """Affiche le prompt final formaté avec les données d'entrée."""
        print(f"\n{'='*20} PROMPT FINAL - {classifier_name.upper()} {'='*20}")
        
        try:
            classifier = getattr(self, f"{classifier_name}_classifier", None)
            if not classifier:
                print(f"Classifieur {classifier_name} non trouvé")
                return
            
            # Formater le prompt avec les données d'entrée
            formatted_prompt = classifier["prompt"].format(**input_data)
            
            # Si c'est un ChatPromptTemplate, on récupère les messages
            if hasattr(formatted_prompt, 'messages'):
                for i, message in enumerate(formatted_prompt.messages):
                    print(f"Message {i+1} ({message._class.name_}):")
                    print(f"Contenu: {message.content}")
                    print("-" * 50)
            else:
                print(f"Prompt formaté:\n{formatted_prompt}")
            
        except Exception as e:
            print(f"Erreur lors de l'affichage du prompt pour {classifier_name}: {e}")
            # Affichage de fallback
            print(f"Données d'entrée: {json.dumps(input_data, ensure_ascii=False, indent=2)}")
        
        print(f"{'='*(42 + len(classifier_name))}\n")
    
    
    def _get_intents_for_theme(self, theme_id: str,content_type) -> List[str]:
        """Récupère la liste des intentions associées à un thème donné pour un type de contenu spécifique.

            Args:
                theme_id (str): L'identifiant unique du thème pour lequel on souhaite récupérer les intentions.
                content_type (str): Le type de contenu concerné (par exemple "comment" pour les commentaires,
                            ou "post" pour les publications).

            Returns:
                List[str]: Une liste de chaînes de caractères représentant les noms ou identifiants
                des intentions associées à ce thème et ce type de contenu.
    """
        if content_type =="post":
            try:
                # Chercher dans post_themes
                for theme_data in self.intents_data_posts.get("themes", []):
                    if theme_id in theme_data:
                        return theme_data[theme_id].get("intents", [])
                return []
            except Exception as e:
                print(f"Erreur lors de la récupération des intentions pour le thème {theme_id}: {e}")
                return []
        elif content_type =="comment":
            try:
                # Chercher dans post_themes
                for theme_data in self.intents_data_comments.get("themes", []):
                    if theme_id in theme_data:
                        return theme_data[theme_id].get("intents", [])
                return []
            except Exception as e:
                print(f"Erreur lors de la récupération des intentions pour le thème {theme_id}: {e}")
                return []
    
    
    async def _classify_theme(self, result: Dict[str, Any],content_type,text: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Classifie l'intention du post basée sur le thème identifié

        Args:
            result (Dict[str, Any]): Résultat intermédiaire à enrichir.
            content_type: Type de contenu analysé (commentaire, post, etc.).
            text (str): Contenu principal à analyser.
            post_text (str, optional): Texte complet du post associé (si disponible).
            post_analysis (str, optional): Résultat de l'analyse précédente du post associé au commentaire.

        Returns:
            Dict[str, Any]: Résultat enrichi avec les informations de classification thématique.
    """
        
       
        try:
            # Récupérer les intentions disponibles pour ce thème
            theme_result = await self.theme_classifier["chain"].ainvoke({
             "brand_name": self.brand_name,
                "content_type": content_type,
                "post_text": post_text,
                "post_analysis": json.dumps(post_analysis, indent=2, ensure_ascii=False),
                "text":text

                                                         })
            print(theme_result)
            print(result)
            
            # Structure de base du résultat
            result["theme"] = {
                "id": theme_result["theme_id"],
                "name": theme_result["theme_name"]
            }
            result["confidence"] = theme_result["confidence"]
    
            

            
        except Exception as e:
            print(f"Erreur lors de la classification du thème: {e}")
            result["theme"] = {
                "name": "unknown",
                "id": "unknown"
            }
            result["confidence"] = 0.0
        
        return result


    async def _classify_intent(self, content_type,text: str, result: Dict[str, Any], theme_id: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """    Récupère la liste des intentions associées à un thème donné pour un type de contenu spécifique.

                Args:
                    theme_id (str): L'identifiant unique du thème pour lequel on souhaite récupérer les intentions.
                    content_type (str): Le type de contenu concerné (par exemple "comment" pour les commentaires,
                    ou "post" pour les publications).

                Returns:
                    List[str]: Une liste de chaînes de caractères représentant les noms ou identifiants
                    des intentions associées à ce thème et ce type de contenu.
  
        """
        try:
            # Récupérer les intentions disponibles pour ce thème
            available_intents = self._get_intents_for_theme(theme_id,content_type)
            
            if not available_intents:
                print(f"Aucune intention trouvée pour le thème: {theme_id}")
                return result
            #input data
            input_data = {
                "brand_name": self.brand_name,
                "content_type": content_type,
                "post_text": post_text,
                "post_analysi":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
                "text": text,
                "theme_name": result["theme"]["name"],
                "available_intents": ", ".join(available_intents)
            }
            
            # Classification de l'intention
            intent_result = await self.intent_classifier["chain"].ainvoke(input_data)
            
            # Ajouter l'intention au résultat
            result["intent"] = {
                "name": intent_result["intent"],
                
            }
            result["confidence"] =  intent_result["confidence"]
            # Mettre à jour la confiance globale
            result["confidence"] = min(result["confidence"], intent_result["confidence"])
            
        except Exception as e:
            print(f"Erreur lors de la classification d'intention: {e}")
            result["intent"] = {
                "name": "unknown",
                "confidence": 0.0
            }
        
        return result
    
    async def _classify_offre_hierarchical(self, content_type,text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """ 
                Effectue une classification hiérarchique du contenu relatif à une offre :
                Catégorie -> Sous-catégorie -> Offre.

                Args:
                    content_type (str): Le type de contenu à analyser, tel que "comment" ou "post".
                    Ce paramètre peut influencer les modèles ou schémas appliqués.
        
                    text (str): Le texte principal à classer, souvent le contenu d’un commentaire
                    ou d’une publication.

                    result (Dict[str, Any]): Un dictionnaire des résultats
                                 intermédiaires. Il est enrichi
                                 au fur et à mesure par la fonction.

                    post_text (str, optional): Le texte de la publication associée dans le cas d'analyse d'un commentaire,
                                   utile pour fournir du contexte à la classification.

                    post_analysis (str, optional): Analyse ou métadonnée liée à la publication,
                                       pouvant aider à affiner la classification.

                Returns:
                    Dict[str, Any]: Le dictionnaire `result` mis à jour avec les niveaux de classification
                        identifiés : catégorie principale, sous-catégorie, et offre spécifique.
    """
        
        # Étape 1: Classification de la catégorie
        # Extraction des catégories d'offres depuis les données
        offre_theme = next((t for t in self.data["themes"] if t["id"] == "offre"), None)
        categories = []
        if offre_theme and "category_offre" in offre_theme:
            for category in offre_theme["category_offre"]:
                categories.append({
                "id": category["id"],
                "name": category["name"],
                "keywords": category.get("keywords", [])
            })
        categories=json.dumps(categories, ensure_ascii=False, indent=2)
        category_result = await self.offre_category_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "categories":  categories,
            "post_text": post_text,
            "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
            "text": text})
        
        result["category_offre"] = {
            "id": category_result["category_id"],
            "name": category_result["category_name"]
        }
        result["confidence"] = min(result["confidence"], category_result["confidence"])
        
        # Étape 2: Classification de la sous-catégorie (seulement si catégorie trouvée)
        if category_result["category_id"] != "none":
            # Récupérer les sous-catégories pour cette catégorie
            offre_theme = next((t for t in self.data["themes"] if t["id"] == "offre"), None)
            if offre_theme:
                category_data = next((c for c in offre_theme["category_offre"] if c["id"] == category_result["category_id"]), None)
                subcategories = []
                if category_data and "subcategories" in category_data:
                    for sub_category in category_data["subcategories"]:
                        subcategories.append({
                        "id": sub_category["id"],
                        "name": sub_category["name"],
                        "keywords": sub_category.get("keywords", [])
                    })
                    print(subcategories)
                    subcategory_result = await self.offre_subcategory_classifier["chain"].ainvoke({
                        "brand_name": self.brand_name,
                        "content_type": content_type,
                        "post_text": post_text,
                        "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
                        "text": text,
                        "category_name": category_result["category_name"],
                        "subcategories": json.dumps(subcategories, ensure_ascii=False, indent=2)
                    })
                    
                    if subcategory_result["subcategory_id"] != "none":
                        result["category_offre"]["subcategory_offre"] = {
                            "id": subcategory_result["subcategory_id"],
                            "name": subcategory_result["subcategory_name"]
                        }
                        result["confidence"] = min(result["confidence"], subcategory_result["confidence"])
                        
                        # Étape 3: Classification du produit (seulement si sous-catégorie trouvée)
                        subcategory_data = next((s for s in category_data["subcategories"] if s["id"] == subcategory_result["subcategory_id"]), None)
                        if subcategory_data and "products" in subcategory_data:
                            products = subcategory_data["products"]
                            print(products)
                            product_result = await self.offre_product_classifier["chain"].ainvoke({
                                "brand_name": self.brand_name,
                                "content_type": content_type,
                                "post_text": post_text,
                                "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
                                "text": text,
                                "subcategory_name": subcategory_result["subcategory_name"],
                                "products": json.dumps(products, ensure_ascii=False, indent=2)
                            })
                            
                            if product_result["product_id"] != "general":
                                result["category_offre"]["subcategory_offre"]["offre"] = product_result["product_name"]
                                result["confidence"] = min(result["confidence"], product_result["confidence"])
        
        return result
    
    async def _classify_initiative_hierarchical(self,content_type, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """ 
                Effectue une classification hiérarchique du contenu relatif aux initiatives :
                Initiative principale -> Événement ou action spécifique.

            Args:
                content_type (str): Le type de contenu à analyser, comme "comment" ou "post".

                text (str): Le texte principal à analyser (par exemple, un commentaire).

                result (Dict[str, Any]): Un dictionnaire contenant les résultats intermédiaires.
                                 La fonction enrichit ce dictionnaire avec les niveaux de classification.

                post_text (str, optional): Le texte de la publication associée, utilisé pour donner
                                   du contexte à l'analyse des commentaires.

                post_analysis (str, optional): Analyse sémantique ou métadonnées supplémentaires
                                       issues du post, pouvant influencer la classification.

            Returns:
                Dict[str, Any]: Le dictionnaire `result` enrichi avec les champs correspondants
                        à la classification hiérarchique.
    """
        
        # Étape 1: Classification de l'initiative
        # Récupération de la liste des initiatives depuis les données
        initiative_theme = next((t for t in self.data["themes"] if t["id"] == "initiative"), None)
        initiatives = []
        if initiative_theme and "initiatives" in initiative_theme:
            for initiative in initiative_theme["initiatives"]:
                initiatives.append({
                "id": initiative["id"],
                "name": initiative["name"],
                "keywords": initiative.get("keywords", [])
            })
        initiatives=json.dumps(initiatives, ensure_ascii=False, indent=2)
        initiative_result = await self.initiative_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "post_text": post_text,
            "initiatives": initiatives,
            "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
            "text": text})
        
        result["initiative"] = {
            "id": initiative_result["initiative_id"],
            "name": initiative_result["initiative_name"]
        }
        result["confidence"] = min(result["confidence"], initiative_result["confidence"])
        
        # Étape 2: Classification de l'événement (seulement si initiative trouvée)
        if initiative_result["initiative_id"] != "none":
            # Récupérer les événements pour cette initiative
            initiative_theme = next((t for t in self.data["themes"] if t["id"] == "initiative"), None)
            if initiative_theme:
                initiative_data = next((i for i in initiative_theme["initiatives"] if i["id"] == initiative_result["initiative_id"]), None)
                if initiative_data and "events" in initiative_data:
                    events = initiative_data["events"]
                    
                    event_result = await self.initiative_event_classifier["chain"].ainvoke({
                        "brand_name": self.brand_name,
                        "content_type": content_type,
                        "post_text": post_text,
                        "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
                        "text": text,
                        "initiative_name": initiative_result["initiative_name"],
                        "events": json.dumps(events, ensure_ascii=False, indent=2)
                    })
                    
                    if event_result["event_id"] != "general":
                        result["initiative"]["evenement"] = event_result["event_name"]
                        result["confidence"] = min(result["confidence"], event_result["confidence"])
        
        return result
    
    async def _classify_communication_hierarchical(self,content_type, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """
            Effectue une classification hiérarchique du contenu relatif à la communication :
            Sujet principal (Topic) -> Sous-sujet.

            Args:
                content_type (str): Le type de contenu analysé, tel que "comment" ou "post".
                            Ce paramètre peut influencer les modèles, règles ou configurations utilisées.
        
                text (str): Le texte principal à analyser (ex. : commentaire ou description de publication).

                result (Dict[str, Any]): Un dictionnaire contenant les résultats intermédiaires.
                                 La fonction enrichit ce dictionnaire avec les niveaux de classification

                post_text (str, optional): Le texte de la publication d'origine, pouvant fournir un contexte
                                   supplémentaire pour une meilleure classification  des commentaires.

                post_analysis (str, optional): Résultats d’analyse ou métadonnées du post (ex. : tonalité, thème détecté),
                                       pouvant aider à raffiner la classification.

            Returns:
                Dict[str, Any]: Le dictionnaire `result` mis à jour avec les deux niveaux de classification.
                       
    """
        
        # Étape 1: Classification du topic
        comm_theme = next((t for t in self.data["themes"] if t["id"] == "communication_interaction"), None)
        topics = []
        if comm_theme and "Communication & Engagement Client" in comm_theme:
            for topic in comm_theme["Communication & Engagement Client"]:
                topics.append({
                "id": topic["id"],
                "name": topic["name"],
                "keywords": topic.get("keywords", [])
            })
        topics =json.dumps(topics, ensure_ascii=False, indent=2)
                
        topic_result = await self.communication_topic_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "post_text": post_text,
            "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
            "topics": topics,
            "text": text})
        
        result["communication_interaction_topic"] = {
            "id": topic_result["topic_id"],
            "name": topic_result["topic_name"]
        }
        result["confidence"] = min(result["confidence"], topic_result["confidence"])
        
        # Étape 2: Classification du sous-sujet (seulement si topic trouvé)
        if topic_result["topic_id"] != "none":
            # Récupérer les sous-sujets pour ce topic
            comm_theme = next((t for t in self.data["themes"] if t["id"] == "communication_interaction"), None)
            if comm_theme:
                topic_data = next((t for t in comm_theme["Communication & Engagement Client"] if t["id"] == topic_result["topic_id"]), None)
                if topic_data and "subtopics" in topic_data:
                    subtopics = topic_data["subtopics"]
                    
                    subtopic_result = await self.communication_subtopic_classifier["chain"].ainvoke({
                        "brand_name": self.brand_name,
                        "content_type": content_type,
                        "post_text": post_text,
                        "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
                        "text": text,
                        "topic_name": topic_result["topic_name"],
                        "subtopics": json.dumps(subtopics, ensure_ascii=False, indent=2)
                    })
                    
                    if subtopic_result["subtopic_id"] != "general":
                        result["communication_interaction_topic"]["subtopic"] = subtopic_result["subtopic_name"]
                        result["confidence"] = min(result["confidence"], subtopic_result["confidence"])
        
        return result
    
    async def _analyze_relevance(self,text: str, post_text: str, post_analysis: str="") -> Dict[str, Any]:
        """Analyse la pertinence du contenu et classifie l’intention principale du texte en fonction du thème identifié.

            Args:
                text (str): Le texte principal à analyser, souvent un commentaire ou un message utilisateur.

                post_text (str): Le texte de la publication associée, utilisé pour effectuer l’analyse.

                post_analysis (str, optional): Métadonnées ou résultats d’analyse préexistants liés au post,

            Returns:
                Dict[str, Any]: Un dictionnaire contenant les résultats de l’analyse"""
        try:
            # Récupérer les intentions disponibles pour ce thème
            relevance_result = await self.relevance_analyzer["chain"].ainvoke({
            "brand_name": self.brand_name,
            "post_text": post_text,
            "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
            "text":text 
             })
            
            # Ajouter l'intention au résultat
            result = {
                "relevance" :{
                "relevance_post": relevance_result["relevance_post"],
                "general_relevance": relevance_result["general_relevance"]
                }
                
            }
           
            # Mettre à jour la confiance globale
            result["confidence"] =  relevance_result["confidence"]
        
          
    
            
            
        except Exception as e:
            print(f"Erreur lors de l'analyse de pertinence: {e}")
            result["relevance"] = {
                "relevance_post": "unknown",
                "general_relevance": "unknown",
                "confidence": 0.0
            }
        
        return result
    async def _analyze_sentiment(self, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """ 
        Analyse le sentiment exprimé dans un commentaire et enrichit les résultats d’analyse.

        Args:
            text (str): Le texte principal à analyser, généralement un commentaire d'utilisateur.

            result (Dict[str, Any]): Dictionnaire des résultats intermédiaires.
                                 Cette méthode y ajoute les résultats liés au sentiment.

            post_text (str, optional): Le texte de la publication associée, utilisé comme contexte supplémentaire
                                   pour affiner la détection du sentiment (par défaut vide).

            post_analysis (str, optional): Métadonnées ou analyses précédentes sur le post, pouvant influencer
                                       ou préciser l’interprétation du sentiment.

        Returns:
            Dict[str, Any]: Le dictionnaire `result` mis à jour"""
        try:
            
            # Classification de l'intention
            sentiment_result = await self.sentiment_classifier["chain"].ainvoke({
                "brand_name": self.brand_name,
                "post_text": post_text,
                "post_analysis":  json.dumps(post_analysis, indent=2, ensure_ascii=False),
                "text": text

            })
            
            # Ajouter le sentiment au résultat
            result["sentiment"] = {
                "sentiment": sentiment_result["sentiment"],
                "emotion": sentiment_result["emotion"],
                "polarity_score": sentiment_result["polarity_score"]
                
            }
            # Mettre à jour la confiance globale
            result["confidence"] = min(result["confidence"], sentiment_result["confidence"])
            
        except Exception as e:
            print(f"Erreur lors de l'analyse de sentiment: {e}")
            result["sentiment"] = {
                "name": "unknown",
                "confidence": 0.0
            }
        
        return result
    
    
    async def analyze_content(self,content_type,  text: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """
         Analyse un contenu (commentaire ou post) et retourne sa classification complète
        selon le schéma hiérarchique défini par Orange.

        L’analyse comprend plusieurs étapes successives :
            - Détection du thème principal
            - Classification hiérarchique spécifique au thème (ex. : offres, initiatives, communication…)
            - Détection de l’intention
            - Analyse du sentiment
            - Vérification de la pertinence (relevance)

        Args:
                content_type (str): Le type de contenu à analyser, comme "comment" ou "post".
                Ce paramètre influence le traitement (intents, modèles, configuration…).

                text (str): Le texte principal à analyser (ex. : commentaire d’un client ou description d’un post).

                post_text (str, optional): Le texte du post original, utilisé pour apporter un
                                   contexte supplémentaire à l’analyse des commentaires.

                post_analysis (str, optional): Résultats ou métadonnées issus d’une analyse précédente du post
                                       (ex. : thème identifié, tonalité, type de campagne…).

        Returns:
                Dict[str, Any]: Un dictionnaire contenant la classification complète du contenu
        """
        if content_type =="comment":
            print("")
            result = await self._analyze_relevance(text,post_text, post_analysis)
            print("")
        else:
            result = { "theme":{
                "id":"",
                "name":""
            },
            "confidence": ""} 
                              
        if (content_type == "comment" and  result.get("relevance", {}).get("general_relevance") == "true" ) or content_type !="comment":
            
            # 1. Classification du thème principal avec le prompt YAML
            result = await self._classify_theme(result,content_type, text,post_text, post_analysis)
            theme_id = result["theme"]["id"]
            # 2. Classification spécifique selon le thème - HIÉRARCHIQUE
            if theme_id  == "offre":
                result = await self._classify_offre_hierarchical(content_type, text, result,post_text, post_analysis)
            
            elif theme_id  == "initiative":
                result = await self._classify_initiative_hierarchical(content_type,text, result,post_text, post_analysis)
            
            elif theme_id  == "communication_interaction":
                result = await self._classify_communication_hierarchical(content_type,text, result,post_text, post_analysis)
        
            # 3. Classification des intentions basée sur le thème
            if theme_id  != "none":
                result = await self._classify_intent(content_type,text, result, theme_id ,post_text, post_analysis)
            if content_type == "comment":

                # 3. Classification des intentions basée sur le thème
                if theme_id  != "none":
                    result = await self._analyze_sentiment(text, result, post_text, post_analysis)
        
        return result
    
    def analyze_content_sync(self, content_type, text: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Version synchrone de l'analyse de post"""
        return asyncio.run(self.analyze_content(content_type,text,post_text, post_analysis))
    # Exemple d'utilisation et test
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyBkwR3k6Hu_WybdYjGzm1sDq4evhSal0nk"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return
    
    try:
        # Initialiser l'analyseur avec la clé API
        analyzer = ContentAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            brand_name="orangemaroc"
        )
        
        # Exemple de post avec commentaires
        post_content = '''
🚨 تفرجو على راحتكم مع عرض *5 😎

استافد من 2,5Go على Youtube بالإضافة ل TikTok مع *5, صالحة سيمانة و غييير ب 10 دراهم 🧡'''
        
        # Commentaires associés au post
        comments = [
            "دخلتها على اساس كونكسيو الواتساب صدقات ليا فتيكتوك",
            "amazon chnou fiha"
        ]
        
        print("=== Analyse d'un Post avec ses Commentaires ===\n")
        
        # 1. Analyser le post principal
        print("📝 ANALYSE DU POST PRINCIPAL:")
        print(f"Contenu: {post_content}")
        print("-" * 50)
        
        try:
            post_analysis = await analyzer.analyze_content("post", post_content)
            print(f"Résultat Post: {json.dumps(post_analysis, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"Erreur lors de l'analyse du post: {e}")
            post_analysis = None
        
        print("\n" + "="*70 + "\n")
        
        # 2. Analyser chaque commentaire
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
        
        # Tests supplémentaires avec d'autres posts
        print("=== Tests Supplémentaires ===\n")
        additional_tests = [
            '''النجمة 6 غادي تغادرنا ولكن ماتخافوش راها مامطولاش فالتسافيرة ديالها 😎
تسناوها نهار 23 يونيو 🔥''',
            "كيفاش دايز عندكوم العيد ؟"
        ]
        
        for i, test_post in enumerate(additional_tests, 1):
            print(f"Test {i}: {test_post}")
            print("-" * 50)
            
            try:
                result = await analyzer.analyze_content("post", test_post)
                print(f"Résultat: {json.dumps(result, ensure_ascii=False, indent=2)}")
            except Exception as e:
                print(f"Erreur lors de l'analyse: {e}")
            
            print("\n" + "="*70 + "\n")
            
    except Exception as e:
        print(f"Erreur d'initialisation: {e}")

if __name__ == "__main__":
    # Pour tester la classe
    asyncio.run(main())