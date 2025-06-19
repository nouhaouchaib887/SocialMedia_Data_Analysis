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
    def __init__(self, google_api_key: str, brand_name: str, intents_file: str = "../config/themes/posts_intents.json", prompts_file: str = "../config/prompts/prompts_o.yaml"):
        """
        Initialise l'analyseur de posts Orange
        
        Args:
            google_api_key: Clé API Google Gemini
            brand_name: Nom de la marque
            intents_file: Chemin vers le fichier de configuration JSON des intentions
            prompts_file: Chemin vers le fichier YAML des prompts
        """
        self.config_file = f"../config/themes/{brand_name}_themes.json"
        self.intents_file = intents_file
        self.prompts_file = prompts_file
        self.google_api_key = google_api_key
        self.brand_name = brand_name
        
        # Chargement des configurations
        self.data = self._load_config()
        self.intents_data = self._load_intents_config()
        self.prompts_config = self._load_prompts_config()
        
        # Configuration de la clé API Google
        self._setup_api_key()
        
        # Initialisation du modèle LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash", 
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
    
    def _load_prompts_config(self) -> Dict[str, Any]:
        """Charge la configuration des prompts depuis le fichier YAML"""
        try:
            with open(self.prompts_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration des prompts {self.prompts_file} non trouvé")
            raise
        except yaml.YAMLError as e:
            print(f"Erreur de parsing YAML: {e}")
            raise
    
    def _render_prompt_template(self, template_str: str, **kwargs) -> str:
        """Utilise Jinja2 pour rendre le template avec les variables"""
        template = Template(template_str)
        return template.render(**kwargs)
    
    def setup_analyzers(self):
        """Configuration des analyseurs pour chaque niveau de classification"""
        # 1. Classificateur de thème principal avec prompt YAML
        self.theme_classifier = self._create_theme_classifier_from_yaml()
        
        # 2. Classificateurs spécifiques par étapes (gardés comme avant pour les autres)
        self.offre_category_classifier = self._create_offre_category_classifier()
        self.offre_subcategory_classifier = self._create_offre_subcategory_classifier_from_yaml()
        self.offre_product_classifier = self._create_offre_product_classifier()
        
        self.initiative_classifier = self._create_initiative_classifier_from_yaml()
        self.initiative_event_classifier = self._create_initiative_event_classifier_from_yaml()
        
        self.communication_topic_classifier = self._create_communication_topic_classifier_from_yaml()
        self.communication_subtopic_classifier = self._create_communication_subtopic_classifier_from_yaml()
        
        # 3. Classificateur des intentions
        self.intent_classifier = self._create_intent_classifier_from_yaml()
    
    def _create_theme_classifier_from_yaml(self):
        """Crée le classificateur de thème principal à partir du fichier YAML"""
        # Récupération de la configuration du classificateur thématique
        thematic_config = self.prompts_config.get('classifiers', {}).get('thematic', {})
        
        if not thematic_config:
            raise ValueError("Configuration 'thematic' non trouvée dans le fichier prompts YAML")
        
        # Configuration du schéma de réponse depuis YAML
        output_schema = thematic_config.get('output_schema', [])
        theme_schema = []
        for schema_item in output_schema:
            theme_schema.append(
                ResponseSchema(
                    name=schema_item['name'], 
                    description=schema_item['description']
                )
            )
        
        theme_parser = StructuredOutputParser.from_response_schemas(theme_schema)
        
        # Récupération du template de prompt depuis YAML
        prompt_template = thematic_config.get('prompt_template', '')
        
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
            prompt_template,
            brand_name="{brand_name}",
            content_type="{content_type}",  # Par défaut pour les posts
            post_text="{post_text}",  # Vide par défaut
            post_analysis="{post_analysis}",  # Vide par défaut
            text="{text}",  # Placeholder pour le texte à analyser
            format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
        )
        
        theme_prompt = ChatPromptTemplate.from_template(rendered_prompt)
        
        theme_prompt = theme_prompt.partial(
            format_instructions=theme_parser.get_format_instructions()

        )
        
        return {
            "prompt": theme_prompt,
            "parser": theme_parser,
            "chain": theme_prompt | self.llm | theme_parser
        }
    
    def _create_intent_classifier_from_yaml(self):
        """Crée le classificateur d'intentions à partir du fichier YAML"""
        # Récupération de la configuration du classificateur d'intentions
        intent_config = self.prompts_config.get('classifiers', {}).get('intent', {})
    
        if not intent_config:
            raise ValueError("Configuration 'intent' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = intent_config.get('output_schema', [])
        intent_schema = []
        for schema_item in output_schema:
            intent_schema.append(
            ResponseSchema(
                name=schema_item['name'], 
                description=schema_item['description']
            )
        )
    
        intent_parser = StructuredOutputParser.from_response_schemas(intent_schema)
    
        # Récupération du template de prompt depuis YAML
        prompt_template = intent_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        post_text="{post_text}",  # Vide par défaut
        post_analysis="{post_analysis}",  # Vide par défaut
        theme_name="{theme_name}",
        available_intents="{available_intents}",
        text="{text}",
        format_instructions="{format_instructions}"
    )
    
        intent_prompt = ChatPromptTemplate.from_template(rendered_prompt)
    
        intent_prompt = intent_prompt.partial(
        format_instructions=intent_parser.get_format_instructions()
    )
    
        return {
            "prompt": intent_prompt,
            "parser": intent_parser,
            "chain": intent_prompt | self.llm | intent_parser
     }
    
    def _get_intents_for_theme(self, theme_id: str) -> List[str]:
        """Récupère les intentions disponibles pour un thème donné"""
        try:
            # Chercher dans post_themes
            for theme_data in self.intents_data.get("themes", []):
                if theme_id in theme_data:
                    return theme_data[theme_id].get("intents", [])
            return []
        except Exception as e:
            print(f"Erreur lors de la récupération des intentions pour le thème {theme_id}: {e}")
            return []
    
    def _create_offre_category_classifier(self):
        """Crée le classificateur pour les catégories d'offres à partir du fichier YAML"""
        # Récupération de la configuration du classificateur de catégories d'offres
        offer_category_config = self.prompts_config.get('classifiers', {}).get('offer_category', {})
        if not offer_category_config:
            raise ValueError("Configuration 'offer_category' non trouvée dans le fichier prompts YAML")
    
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
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = offer_category_config.get('output_schema', [])
        category_schema = []
        for schema_item in output_schema:
            category_schema.append(
            ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        category_parser = StructuredOutputParser.from_response_schemas(category_schema)
    
        # Récupération du template de prompt depuis YAML
        prompt_template = offer_category_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
            prompt_template,
            brand_name="{brand_name}",
            content_type="{content_type}",
            post_text="{post_text}",  # Vide par défaut
            post_analysis="{post_analysis}",  # Vide par défaut
            text="{text}",  # Placeholder pour le texte à analyser
            categories="{categories}",  # Placeholder pour les catégories
            format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
            )
    
        category_prompt = ChatPromptTemplate.from_template(rendered_prompt)
    
        # Application des valeurs partielles
        category_prompt = category_prompt.partial(
        brand_name=self.brand_name,
        categories=json.dumps(categories, ensure_ascii=False, indent=2),
        format_instructions=category_parser.get_format_instructions()
    )
    
        return {
        "prompt": category_prompt,
        "parser": category_parser,
        "chain": category_prompt | self.llm | category_parser
    }
    
    def _create_offre_subcategory_classifier_from_yaml(self):
        """Crée le classificateur pour les sous-catégories d'offres à partir du fichier YAML"""
        # Récupération de la configuration du classificateur de sous-catégories d'offres
        subcategory_config = self.prompts_config.get('classifiers', {}).get('offer_subcategory', {})
        if not subcategory_config:
            raise ValueError("Configuration 'offer_subcategory' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = subcategory_config.get('output_schema', [])
        subcategory_schema = []
        for schema_item in output_schema:
            subcategory_schema.append(
            ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        subcategory_parser = StructuredOutputParser.from_response_schemas(subcategory_schema)
    
        # Récupération du template de prompt depuis YAML
        prompt_template = subcategory_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        category_name="{category_name}",
        subcategories="{subcategories}",
        post_text="{post_text}",  # Vide par défaut
        post_analysis="{post_analysis}",  # Vide par défaut
        text="{text}",  # Placeholder pour le texte à analyser
        format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
    )
    
        subcategory_prompt = ChatPromptTemplate.from_template(rendered_prompt)
    
        subcategory_prompt = subcategory_prompt.partial(
        format_instructions=subcategory_parser.get_format_instructions()
    )
    
        return {
        "prompt": subcategory_prompt,
        "parser": subcategory_parser,
        "chain": subcategory_prompt | self.llm | subcategory_parser
    }
    
    def _create_offre_product_classifier(self):
        """Crée le classificateur pour les produits/offres spécifiques à partir du fichier YAML"""
        # Récupération de la configuration du classificateur d'offres
        offer_config = self.prompts_config.get('classifiers', {}).get('offer', {})
        if not offer_config:
            raise ValueError("Configuration 'offer' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = offer_config.get('output_schema', [])
        product_schema = []
        for schema_item in output_schema:
            product_schema.append(
                ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        product_parser = StructuredOutputParser.from_response_schemas(product_schema)
    
        # Récupération du template de prompt depuis YAML
        prompt_template = offer_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        subcategory_name="{subcategory_name}",
        products="{products}",
        post_text="{post_text}",      # Pour les commentaires
        post_analysis="{post_analysis}",  # Pour les commentaires
        text="{text}",                # Placeholder pour le texte à analyser
        format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
    )
    
        product_prompt = ChatPromptTemplate.from_template(rendered_prompt)
    
        product_prompt = product_prompt.partial(
        brand_name=self.brand_name,
        format_instructions=product_parser.get_format_instructions()
    )
    
        return {
        "prompt": product_prompt,
        "parser": product_parser,
        "chain": product_prompt | self.llm | product_parser
    }
    
    def _create_initiative_classifier_from_yaml(self):
        """Crée le classificateur d'initiative à partir du fichier YAML"""
        # Récupération de la configuration du classificateur d'initiative
        initiative_config = self.prompts_config.get('classifiers', {}).get('initiative', {})
        if not initiative_config:
            raise ValueError("Configuration 'initiative' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = initiative_config.get('output_schema', [])
        initiative_schema = []
        for schema_item in output_schema:
            initiative_schema.append(
            ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        initiative_parser = StructuredOutputParser.from_response_schemas(initiative_schema)
    
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
    
        # Récupération du template de prompt depuis YAML
        prompt_template = initiative_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        post_text="{post_text}",  # Vide par défaut
        post_analysis="{post_analysis}",  # Vide par défaut
        text="{text}",  # Placeholder pour le texte à analyser
        initiatives="{initiatives}",
        format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
    )
    
        initiative_prompt = ChatPromptTemplate.from_template(rendered_prompt)
        initiative_prompt = initiative_prompt.partial(
        initiatives=json.dumps(initiatives, ensure_ascii=False, indent=2),
        format_instructions=initiative_parser.get_format_instructions()
    )
    
        return {
        "prompt": initiative_prompt,
        "parser": initiative_parser,
        "chain": initiative_prompt | self.llm | initiative_parser
    }
    
    def _create_initiative_event_classifier_from_yaml(self):
        """Crée le classificateur d'événement d'initiative à partir du fichier YAML"""
        # Récupération de la configuration du classificateur d'événement d'initiative
        initiative_event_config = self.prompts_config.get('classifiers', {}).get('initiative_event', {})
        if not initiative_event_config:
            raise ValueError("Configuration 'initiative_event' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = initiative_event_config.get('output_schema', [])
        event_schema = []
        for schema_item in output_schema:
            event_schema.append(
                ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        event_parser = StructuredOutputParser.from_response_schemas(event_schema)
    
        # Récupération du template de prompt depuis YAML
        prompt_template = initiative_event_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        initiative_name="{initiative_name}",  # Placeholder pour le nom de l'initiative
        post_text="{post_text}",  # Vide par défaut
        post_analysis="{post_analysis}",  # Vide par défaut
        text="{text}",  # Placeholder pour le texte à analyser
        events="{events}",  # Placeholder pour la liste des événements
        format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
    )
    
        event_prompt = ChatPromptTemplate.from_template(rendered_prompt)
        event_prompt = event_prompt.partial(
        format_instructions=event_parser.get_format_instructions()
    )
    
        return {
        "prompt": event_prompt,
        "parser": event_parser,
        "chain": event_prompt | self.llm | event_parser
    }
    
    def _create_communication_topic_classifier_from_yaml(self):

        """Crée le classificateur de topic de communication à partir du fichier YAML"""
        comm_theme = next((t for t in self.data["themes"] if t["id"] == "communication_interaction"), None)
        topics = []
        if comm_theme and "Communication & Engagement Client" in comm_theme:
            for topic in comm_theme["Communication & Engagement Client"]:
                topics.append({
                    "id": topic["id"],
                    "name": topic["name"],
                    "keywords": topic.get("keywords", [])
                })
        # Récupération de la configuration du classificateur de topic de communication
        communication_topic_config = self.prompts_config.get('classifiers', {}).get('communication_topic', {})
        if not communication_topic_config:
            raise ValueError("Configuration 'communication_topic' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = communication_topic_config.get('output_schema', [])
        topic_schema = []
        for schema_item in output_schema:
            topic_schema.append(
            ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        topic_parser = StructuredOutputParser.from_response_schemas(topic_schema)
    
        # Récupération de la liste des topics de communication depuis les données
        comm_theme = next((t for t in self.data["themes"] if t["id"] == "communication_interaction"), None)
        topics = []
        if comm_theme and "Communication & Engagement Client" in comm_theme:
            for topic in comm_theme["Communication & Engagement Client"]:
                topics.append({
                "id": topic["id"],
                "name": topic["name"],
                "keywords": topic.get("keywords", [])
            })
    
        # Récupération du template de prompt depuis YAML
        prompt_template = communication_topic_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        post_text="{post_text}",  # Vide par défaut
        post_analysis="{post_analysis}",  # Vide par défaut
        text="{text}",  # Placeholder pour le texte à analyser
        topics="{topics}",
        format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
    )
    
        communication_topic_prompt = ChatPromptTemplate.from_template(rendered_prompt)
        communication_topic_prompt = communication_topic_prompt.partial(
             topics=json.dumps(topics, ensure_ascii=False, indent=2),
            format_instructions=topic_parser.get_format_instructions()
    )
    
        return {
        "prompt": communication_topic_prompt,
        "parser": topic_parser,
        "chain": communication_topic_prompt | self.llm | topic_parser
    }
    
    def _create_communication_subtopic_classifier_from_yaml(self):
        """Crée le classificateur de sous-sujet de communication à partir du fichier YAML"""
        # Récupération de la configuration du classificateur de sous-sujet de communication
        communication_subtopic_config = self.prompts_config.get('classifiers', {}).get('communication_subtopic', {})
        if not communication_subtopic_config:
            raise ValueError("Configuration 'communication_subtopic' non trouvée dans le fichier prompts YAML")
    
        # Configuration du schéma de réponse depuis YAML
        output_schema = communication_subtopic_config.get('output_schema', [])
        subtopic_schema = []
        for schema_item in output_schema:
            subtopic_schema.append(
                ResponseSchema(
                name=schema_item['name'],
                description=schema_item['description']
            )
        )
    
        subtopic_parser = StructuredOutputParser.from_response_schemas(subtopic_schema)
    
        # Récupération du template de prompt depuis YAML
        prompt_template = communication_subtopic_config.get('prompt_template', '')
    
        # Rendu du template avec les variables spécifiques
        rendered_prompt = self._render_prompt_template(
        prompt_template,
        brand_name="{brand_name}",
        content_type="{content_type}",
        topic_name="{topic_name}",  # Placeholder pour le nom du topic
        post_text="{post_text}",  # Vide par défaut
        post_analysis="{post_analysis}",  # Vide par défaut
        text="{text}",  # Placeholder pour le texte à analyser
        subtopics="{subtopics}",  # Placeholder pour la liste des sous-sujets
        format_instructions="{format_instructions}"  # Placeholder pour les instructions de format
    )
    
        subtopic_prompt = ChatPromptTemplate.from_template(rendered_prompt)
        subtopic_prompt = subtopic_prompt.partial(
        format_instructions=subtopic_parser.get_format_instructions()
    )
    
        return {
        "prompt": subtopic_prompt,
        "parser": subtopic_parser,
        "chain": subtopic_prompt | self.llm | subtopic_parser
    }

    async def analyze_content(self,content_type,  text: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """
        Analyse un post et retourne sa classification complète selon le schéma Orange
        Classification hiérarchique étape par étape avec intentions
        """
        # 1. Classification du thème principal avec le prompt YAML
        theme_result = await self.theme_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "text":text

                                                                     })
        
        # Structure de base du résultat
        result = {
            "theme": {
                "id": theme_result["theme_id"],
                "name": theme_result["theme_name"]
            },
            "confidence": theme_result["confidence"]
        }
        
        # 2. Classification spécifique selon le thème - HIÉRARCHIQUE
        if theme_result["theme_id"] == "offre":
            result = await self._classify_offre_hierarchical(content_type, text, result,post_text, post_analysis)
            
        elif theme_result["theme_id"] == "initiative":
            result = await self._classify_initiative_hierarchical(content_type,text, result,post_text, post_analysis)
            
        elif theme_result["theme_id"] == "communication_interaction":
            result = await self._classify_communication_hierarchical(content_type,text, result,post_text, post_analysis)
        
        # 3. Classification des intentions basée sur le thème
        if theme_result["theme_id"] != "none":
            result = await self._classify_intent(content_type,text, result, theme_result["theme_id"],post_text, post_analysis)
        
        return result
    
    async def _classify_intent(self, content_type,text: str, result: Dict[str, Any], theme_id: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Classifie l'intention du post basée sur le thème identifié"""
        try:
            # Récupérer les intentions disponibles pour ce thème
            available_intents = self._get_intents_for_theme(theme_id)
            
            if not available_intents:
                print(f"Aucune intention trouvée pour le thème: {theme_id}")
                return result
            
            # Classification de l'intention
            intent_result = await self.intent_classifier["chain"].ainvoke({
                "brand_name": self.brand_name,
                "content_type": content_type,
                "post_text": post_text,
                "post_analysi": post_analysis,
                "text": text,
                "theme_name": result["theme"]["name"],
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
    
    async def _classify_offre_hierarchical(self, content_type,text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Classification hiérarchique pour les offres: Catégorie -> Sous-catégorie -> Produit"""
        
        # Étape 1: Classification de la catégorie
        category_result = await self.offre_category_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "post_text": post_text,
            "post_analysis": post_analysis,
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
                        "post_analysis": post_analysis,
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
                                "post_analysis": post_analysis,
                                "text": text,
                                "subcategory_name": subcategory_result["subcategory_name"],
                                "products": json.dumps(products, ensure_ascii=False, indent=2)
                            })
                            
                            if product_result["product_id"] != "general":
                                result["category_offre"]["subcategory_offre"]["offre"] = product_result["product_name"]
                                result["confidence"] = min(result["confidence"], product_result["confidence"])
        
        return result
    
    async def _classify_initiative_hierarchical(self,content_type, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Classification hiérarchique pour les initiatives: Initiative -> Événement"""
        
        # Étape 1: Classification de l'initiative
        initiative_result = await self.initiative_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "post_text": post_text,
            "post_analysis": post_analysis,
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
                        "post_analysis": post_analysis,
                        "text": text,
                        "initiative_name": initiative_result["initiative_name"],
                        "events": json.dumps(events, ensure_ascii=False, indent=2)
                    })
                    
                    if event_result["event_id"] != "general":
                        result["initiative"]["evenement"] = event_result["event_name"]
                        result["confidence"] = min(result["confidence"], event_result["confidence"])
        
        return result
    
    async def _classify_communication_hierarchical(self,content_type, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Classification hiérarchique pour la communication: Topic -> Sous-sujet"""
        
        # Étape 1: Classification du topic
        topic_result = await self.communication_topic_classifier["chain"].ainvoke({
            "brand_name": self.brand_name,
            "content_type": content_type,
            "post_text": post_text,
            "post_analysis": post_analysis,
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
                        "post_analysis": post_analysis,
                        "text": text,
                        "topic_name": topic_result["topic_name"],
                        "subtopics": json.dumps(subtopics, ensure_ascii=False, indent=2)
                    })
                    
                    if subtopic_result["subtopic_id"] != "general":
                        result["communication_interaction_topic"]["subtopic"] = subtopic_result["subtopic_name"]
                        result["confidence"] = min(result["confidence"], subtopic_result["confidence"])
        
        return result
    
    def analyze_content_sync(self, content_type, text: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Version synchrone de l'analyse de post"""
        return asyncio.run(self.analyze_content(content_type,text,post_text, post_analysis))
    # Exemple d'utilisation et test
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyBiaGLmlDgYBw-yUrpT1XfX49hOB3xNI4s"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return
    
    try:
        # Initialiser l'analyseur avec la clé API
        analyzer = ContentAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            brand_name="orange"
        )
        
        # Exemple de post avec commentaires
        post_content = '''
🚨 آخر فرصة باش تبين الموهبة ديالك فكورة🚨
الحلم ديالك جا حتى لعندك مع Orange Koora Talents ⚽🧡
شارك على الرابط فال Bio
#OrangeKayna'''
        
        # Commentaires associés au post
        comments = [
            "واخا هاد المسابقة زوينة بزاف! شكرا Orange 🧡",
            "كيفاش نقدر نشارك؟ الرابط ما خدامش عندي"
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