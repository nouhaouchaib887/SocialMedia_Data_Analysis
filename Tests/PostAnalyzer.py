import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_core.runnables import RunnablePassthrough
from typing import Dict, Any, List, Optional
import asyncio

class OrangePostAnalyzer:
    def __init__(self, google_api_key: str, config_file: str = "../config/themes/orange_themes.json"):
        """
        Initialise l'analyseur de posts Orange
        
        Args:
            google_api_key: Clé API Google Gemini
            config_file: Chemin vers le fichier de configuration JSON
        """
        self.config_file = config_file
        self.google_api_key = google_api_key
        self.data = self._load_config()
        
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
    
    def _load_config(self) -> Dict[str, Any]:
        """Charge la configuration depuis le fichier JSON"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Fichier de configuration {self.config_file} non trouvé")
            raise
        except json.JSONDecodeError as e:
            print(f"Erreur de parsing JSON: {e}")
            raise
    
    def setup_analyzers(self):
        """Configuration des analyseurs pour chaque niveau de classification"""
        # 1. Classificateur de thème principal
        self.theme_classifier = self._create_theme_classifier()
        
        # 2. Classificateurs spécifiques par étapes
        self.offre_category_classifier = self._create_offre_category_classifier()
        self.offre_subcategory_classifier = self._create_offre_subcategory_classifier()
        self.offre_product_classifier = self._create_offre_product_classifier()
        
        self.initiative_classifier = self._create_initiative_classifier()
        self.initiative_event_classifier = self._create_initiative_event_classifier()
        
        self.communication_topic_classifier = self._create_communication_topic_classifier()
        self.communication_subtopic_classifier = self._create_communication_subtopic_classifier()
    
    def _create_theme_classifier(self):
        """Crée le classificateur de thème principal"""
        # Extraction des mots-clés pour chaque thème
        theme_keywords = {}
        for theme in self.data.get("themes", []):
            theme_keywords[theme["id"]] = theme.get("keywords", [])
        
        theme_schema = [
            ResponseSchema(name="theme_id", 
                          description="ID du thème identifié: 'offre', 'initiative', 'communication_interaction'"),
            ResponseSchema(name="theme_name", 
                          description="Nom du thème identifié"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        theme_parser = StructuredOutputParser.from_response_schemas(theme_schema)
        
        theme_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en analyse de contenu pour Orange Maroc. Analyse ce post et classifie-le dans l'un des thèmes suivants:
        
        - offre: contenu concernant les produits, forfaits, offres commerciales , les cadeaux et les promotions, les services d'Orange
        - initiative: contenu concernant les initiatives de marque, programmes sociaux, événements Orange
        - communication_interaction: contenu concernant la communication client, annonces, interactions
        
        IMPORTANT:
        - Les posts peuvent être en arabe ou en français
        - Analyse le contexte et l'intention du message
        - Retourne uniquement l'un de ces 4 thèmes
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        theme_prompt = theme_prompt.partial(
            format_instructions=theme_parser.get_format_instructions()
        )
        
        return {
            "prompt": theme_prompt,
            "parser": theme_parser,
            "chain": theme_prompt | self.llm | theme_parser
        }
    
    def _create_offre_category_classifier(self):
        """Crée le classificateur pour les catégories d'offres"""
        # Extraction des catégories d'offres
        offre_theme = next((t for t in self.data["themes"] if t["id"] == "offre"), None)
        categories = []
        if offre_theme and "category_offre" in offre_theme:
            for category in offre_theme["category_offre"]:
                categories.append({
                    "id": category["id"],
                    "name": category["name"],
                    "keywords": category.get("keywords", [])
                })
        
        category_schema = [
            ResponseSchema(name="category_id", 
                          description="ID de la catégorie d'offre identifiée"),
            ResponseSchema(name="category_name", 
                          description="Nom de la catégorie d'offre"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        category_parser = StructuredOutputParser.from_response_schemas(category_schema)
        
        category_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des offres Orange Maroc. Ce post concerne une OFFRE. 
        Identifie la CATÉGORIE d'offre parmi les suivantes:
        
        {categories}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Utilise les mots-clés pour identifier la bonne catégorie
        - Choisis la catégorie la plus appropriée
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        category_prompt = category_prompt.partial(
            categories=json.dumps(categories, ensure_ascii=False, indent=2),
            format_instructions=category_parser.get_format_instructions()
        )
        
        return {
            "prompt": category_prompt,
            "parser": category_parser,
            "chain": category_prompt | self.llm | category_parser
        }
    
    def _create_offre_subcategory_classifier(self):
        """Crée le classificateur pour les sous-catégories d'offres"""
        subcategory_schema = [
            ResponseSchema(name="subcategory_id", 
                          description="ID de la sous-catégorie identifiée"),
            ResponseSchema(name="subcategory_name", 
                          description="Nom de la sous-catégorie"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        subcategory_parser = StructuredOutputParser.from_response_schemas(subcategory_schema)
        
        subcategory_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des offres Orange Maroc. Ce post concerne une offre de la catégorie: {category_name}
        
        Identifie la SOUS-CATÉGORIE spécifique parmi les suivantes pour cette catégorie:
        
        {subcategories}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Utilise les mots-clés pour identifier la bonne sous-catégorie
        - Si aucune sous-catégorie ne correspond, retourne "none" comme subcategory_id
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        subcategory_prompt = subcategory_prompt.partial(
            format_instructions=subcategory_parser.get_format_instructions()
        )
        
        return {
            "prompt": subcategory_prompt,
            "parser": subcategory_parser,
            "chain": subcategory_prompt | self.llm | subcategory_parser
        }
    
    def _create_offre_product_classifier(self):
        """Crée le classificateur pour les produits/offres spécifiques"""
        product_schema = [
            ResponseSchema(name="product_id", 
                          description="ID du produit/offre identifié"),
            ResponseSchema(name="product_name", 
                          description="Nom du produit/offre"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        product_parser = StructuredOutputParser.from_response_schemas(product_schema)
        
        product_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des offres Orange Maroc. Ce post concerne une offre de la sous-catégorie: {subcategory_name}
        
        Identifie le PRODUIT/OFFRE EXACTE parmi les suivants:
        
        {products}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Utilise les mots-clés pour identifier le bon produit
        - Si aucun produit spécifique ne correspond, retourne "general" comme product_id
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        product_prompt = product_prompt.partial(
            format_instructions=product_parser.get_format_instructions()
        )
        
        return {
            "prompt": product_prompt,
            "parser": product_parser,
            "chain": product_prompt | self.llm | product_parser
        }
    
    def _create_initiative_classifier(self):
        """Crée le classificateur pour les initiatives"""
        initiative_theme = next((t for t in self.data["themes"] if t["id"] == "initiative"), None)
        initiatives = []
        if initiative_theme and "initiatives" in initiative_theme:
            for initiative in initiative_theme["initiatives"]:
                initiatives.append({
                    "id": initiative["id"],
                    "name": initiative["name"],
                    "keywords": initiative.get("keywords", [])
                })
        
        initiative_schema = [
            ResponseSchema(name="initiative_id", 
                          description="ID de l'initiative identifiée"),
            ResponseSchema(name="initiative_name", 
                          description="Nom de l'initiative"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        initiative_parser = StructuredOutputParser.from_response_schemas(initiative_schema)
        
        initiative_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des initiatives Orange Maroc. Ce post concerne une INITIATIVE. 
        Identifie l'initiative spécifique parmi les suivantes:
        
        {initiatives}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Cherche les correspondances contextuelles et sémantiques
        - Les noms propres peuvent être transcrits différemment
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        initiative_prompt = initiative_prompt.partial(
            initiatives=json.dumps(initiatives, ensure_ascii=False, indent=2),
            format_instructions=initiative_parser.get_format_instructions()
        )
        
        return {
            "prompt": initiative_prompt,
            "parser": initiative_parser,
            "chain": initiative_prompt | self.llm | initiative_parser
        }
    
    def _create_initiative_event_classifier(self):
        """Crée le classificateur pour les événements d'initiatives"""
        event_schema = [
            ResponseSchema(name="event_id", 
                          description="ID de l'événement identifié"),
            ResponseSchema(name="event_name", 
                          description="Nom de l'événement"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        event_parser = StructuredOutputParser.from_response_schemas(event_schema)
        
        event_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des initiatives Orange Maroc. Ce post concerne l'initiative: {initiative_name}
        
        Identifie l'ÉVÉNEMENT spécifique parmi les suivants pour cette initiative:
        
        {events}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Utilise les mots-clés pour identifier le bon événement
        - Si aucun événement spécifique ne correspond, retourne "general" comme event_id
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        event_prompt = event_prompt.partial(
            format_instructions=event_parser.get_format_instructions()
        )
        
        return {
            "prompt": event_prompt,
            "parser": event_parser,
            "chain": event_prompt | self.llm | event_parser
        }
    
    def _create_communication_topic_classifier(self):
        """Crée le classificateur pour les topics de communication"""
        comm_theme = next((t for t in self.data["themes"] if t["id"] == "communication_interaction"), None)
        topics = []
        if comm_theme and "Communication & Engagement Client" in comm_theme:
            for topic in comm_theme["Communication & Engagement Client"]:
                topics.append({
                    "id": topic["id"],
                    "name": topic["name"],
                    "keywords": topic.get("keywords", [])
                })
        
        topic_schema = [
            ResponseSchema(name="topic_id", 
                          description="ID du topic de communication identifié"),
            ResponseSchema(name="topic_name", 
                          description="Nom du topic de communication"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        topic_parser = StructuredOutputParser.from_response_schemas(topic_schema)
        
        topic_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en communication Orange Maroc. Ce post concerne la COMMUNICATION & INTERACTION. 
        Identifie le type de communication parmi les suivants:
        
        {topics}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Considère le contexte et l'intention du message
        - Identifie le type de communication le plus approprié
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        topic_prompt = topic_prompt.partial(
            topics=json.dumps(topics, ensure_ascii=False, indent=2),
            format_instructions=topic_parser.get_format_instructions()
        )
        
        return {
            "prompt": topic_prompt,
            "parser": topic_parser,
            "chain": topic_prompt | self.llm | topic_parser
        }
    
    def _create_communication_subtopic_classifier(self):
        """Crée le classificateur pour les sous-sujets de communication"""
        subtopic_schema = [
            ResponseSchema(name="subtopic_id", 
                          description="ID du sous-sujet identifié"),
            ResponseSchema(name="subtopic_name", 
                          description="Nom du sous-sujet"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de 0 à 1")
        ]
        
        subtopic_parser = StructuredOutputParser.from_response_schemas(subtopic_schema)
        
        subtopic_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en communication Orange Maroc. Ce post concerne le topic: {topic_name}
        
        Identifie le SOUS-SUJET spécifique parmi les suivants:
        
        {subtopics}
        
        IMPORTANT:
        - Le post peut être en arabe mais les mots-clés sont en français
        - Utilise les mots-clés pour identifier le bon sous-sujet
        - Si aucun sous-sujet spécifique ne correspond, retourne "general" comme subtopic_id
        
        Post à analyser:
        {post_text}
        
        {format_instructions}
        """)
        
        subtopic_prompt = subtopic_prompt.partial(
            format_instructions=subtopic_parser.get_format_instructions()
        )
        
        return {
            "prompt": subtopic_prompt,
            "parser": subtopic_parser,
            "chain": subtopic_prompt | self.llm | subtopic_parser
        }
    
    async def analyze_post(self, post_text: str) -> Dict[str, Any]:
        """
        Analyse un post et retourne sa classification complète selon le schéma Orange
        Classification hiérarchique étape par étape
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
        
        # 2. Classification spécifique selon le thème - HIÉRARCHIQUE
        if theme_result["theme_id"] == "offre":
            result = await self._classify_offre_hierarchical(post_text, result)
            
        elif theme_result["theme_id"] == "initiative":
            result = await self._classify_initiative_hierarchical(post_text, result)
            
        elif theme_result["theme_id"] == "communication_interaction":
            result = await self._classify_communication_hierarchical(post_text, result)
        
        return result
    
    async def _classify_offre_hierarchical(self, post_text: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Classification hiérarchique pour les offres: Catégorie -> Sous-catégorie -> Produit"""
        
        # Étape 1: Classification de la catégorie
        category_result = await self.offre_category_classifier["chain"].ainvoke({"post_text": post_text})
        
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
                if category_data and "subcategories" in category_data:
                    subcategories = category_data["subcategories"]
                    
                    subcategory_result = await self.offre_subcategory_classifier["chain"].ainvoke({
                        "post_text": post_text,
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
                        subcategory_data = next((s for s in subcategories if s["id"] == subcategory_result["subcategory_id"]), None)
                        if subcategory_data and "products" in subcategory_data:
                            products = subcategory_data["products"]
                            
                            product_result = await self.offre_product_classifier["chain"].ainvoke({
                                "post_text": post_text,
                                "subcategory_name": subcategory_result["subcategory_name"],
                                "products": json.dumps(products, ensure_ascii=False, indent=2)
                            })
                            
                            if product_result["product_id"] != "general":
                                result["category_offre"]["subcategory_offre"]["offre"] = product_result["product_name"]
                                result["confidence"] = min(result["confidence"], product_result["confidence"])
        
        return result
    
    async def _classify_initiative_hierarchical(self, post_text: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Classification hiérarchique pour les initiatives: Initiative -> Événement"""
        
        # Étape 1: Classification de l'initiative
        initiative_result = await self.initiative_classifier["chain"].ainvoke({"post_text": post_text})
        
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
                        "post_text": post_text,
                        "initiative_name": initiative_result["initiative_name"],
                        "events": json.dumps(events, ensure_ascii=False, indent=2)
                    })
                    
                    if event_result["event_id"] != "general":
                        result["initiative"]["evenement"] = event_result["event_name"]
                        result["confidence"] = min(result["confidence"], event_result["confidence"])
        
        return result
    
    async def _classify_communication_hierarchical(self, post_text: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Classification hiérarchique pour la communication: Topic -> Sous-sujet"""
        
        # Étape 1: Classification du topic
        topic_result = await self.communication_topic_classifier["chain"].ainvoke({"post_text": post_text})
        
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
                        "post_text": post_text,
                        "topic_name": topic_result["topic_name"],
                        "subtopics": json.dumps(subtopics, ensure_ascii=False, indent=2)
                    })
                    
                    if subtopic_result["subtopic_id"] != "general":
                        result["communication_interaction_topic"]["subtopic"] = subtopic_result["subtopic_name"]
                        result["confidence"] = min(result["confidence"], subtopic_result["confidence"])
        
        return result
    
    def analyze_post_sync(self, post_text: str) -> Dict[str, Any]:
        """Version synchrone de l'analyse de post"""
        return asyncio.run(self.analyze_post(post_text))


# Exemple d'utilisation et test
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyBE655-B-jwD-fsU62Iskux2OwMUK-nP0s"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return
    
    try:
        # Initialiser l'analyseur avec la clé API
        analyzer = OrangePostAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            config_file="../config/themes/orange_themes.json"
        )
        
        # Tests avec différents types de posts
        test_posts = [
            '''
النجمة 6 غادي تغادرنا ولكن ماتخافوش راها مامطولاش فالتسافيرة ديالها 😎
تسناوها نهار 23 يونيو 🔥''',
            "كيفاش دايز عندكوم العيد ؟",
            '''
            السخاوة كااااينة مع فورفي Yo 😍
استافدو من ساعة من المكالمات و 11Go من الأنترنيت واللامحدود على الواتساب غييير ب 49 درهم🧡'''
        ]
        
        print("=== Tests de Classification Orange Hiérarchique ===\n")
        
        for i, post in enumerate(test_posts, 1):
            print(f"Test {i}: {post}")
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