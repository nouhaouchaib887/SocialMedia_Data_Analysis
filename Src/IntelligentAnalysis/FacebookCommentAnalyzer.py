import os
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_core.runnables import RunnablePassthrough
from typing import Dict, Any, List, Optional
api_key1 = ""
api_key2 = ""
api_key3 = ""
llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash")
class FacebookCommentAnalyzer:
    def __init__(self, data: Dict[str, Any]):
        """
        Initialise l'analyseur de commentaires Facebook
        
        Args:
            data: Dictionnaire contenant les données structurées pour l'analyse
        """
        self.data = data
        self.setup_analyzers()
    
    def setup_analyzers(self):
        """Configure tous les analyseurs nécessaires pour le traitement des commentaires"""
        # Préparation des données enrichies avec les keywords pour chaque niveau
        self._prepare_keyword_rich_data()
        
        # 1. Analyseur de pertinence
        self.relevance_analyzer = self._create_relevance_analyzer()
        
        # 2. Classificateur général (modifié pour permettre plusieurs classes)
        self.general_classifier = self._create_general_classifier()
        
        # 3. Classificateurs spécifiques (mêmes que pour les posts)
        self.product_classifier = self._create_product_classifier()
        self.outage_classifier = self._create_outage_classifier()
        self.initiative_classifier = self._create_initiative_classifier()
        self.common_topic_classifier = self._create_common_topic_classifier()
        
        # 4. Analyseur d'émotion et de sentiment
        self.emotion_sentiment_analyzer = self._create_emotion_sentiment_analyzer()
    
    def _prepare_keyword_rich_data(self):
        """Prépare des représentations enrichies des données avec les keywords à tous les niveaux"""
        # Enrichissement des catégories de produits
        self.enriched_product_categories = []
        for category in self.data["product_categories"]:
            category_info = {
                "id": category["id"],
                "name": category["name"],
                "keywords": category.get("keywords", [])
            }
            
            # Enrichissement des sous-catégories
            enriched_subcategories = []
            if "subcategories" in category:
                for subcategory in category["subcategories"]:
                    subcategory_info = {
                        "id": subcategory["id"],
                        "name": subcategory["name"],
                        "keywords": subcategory.get("keywords", [])
                    }
                    
                    # Enrichissement des produits
                    enriched_products = []
                    if "products" in subcategory:
                        for product in subcategory["products"]:
                            product_info = {
                                "id": product["id"],
                                "name": product["name"],
                                "keywords": product.get("keywords", [])
                            }
                            enriched_products.append(product_info)
                    
                    subcategory_info["products"] = enriched_products
                    enriched_subcategories.append(subcategory_info)
            
            # Enrichissement des produits au niveau de la catégorie
            enriched_category_products = []
            if "products" in category:
                for product in category["products"]:
                    product_info = {
                        "id": product["id"],
                        "name": product["name"],
                        "keywords": product.get("keywords", [])
                    }
                    enriched_category_products.append(product_info)
            
            category_info["subcategories"] = enriched_subcategories
            category_info["products"] = enriched_category_products
            self.enriched_product_categories.append(category_info)
        
        # Enrichissement des types de pannes
        self.enriched_outage_types = []
        for outage_type in self.data["outage_types"]:
            outage_info = {
                "id": outage_type["id"],
                "name": outage_type["name"],
                "keywords": outage_type.get("keywords", [])
            }
            
            # Enrichissement des sous-catégories de pannes
            enriched_subcategories = []
            if "subcategories" in outage_type:
                for subcategory in outage_type["subcategories"]:
                    subcategory_info = {
                        "id": subcategory["id"],
                        "name": subcategory["name"],
                        "keywords": subcategory.get("keywords", [])
                    }
                    enriched_subcategories.append(subcategory_info)
            
            outage_info["subcategories"] = enriched_subcategories
            self.enriched_outage_types.append(outage_info)
        
        # Enrichissement des initiatives
        self.enriched_initiatives = []
        for initiative in self.data["initiatives"]:
            initiative_info = {
                "id": initiative["id"],
                "name": initiative["name"],
                "keywords": initiative.get("keywords", [])
            }
            
            # Enrichissement des événements
            enriched_events = []
            if "events" in initiative:
                for event in initiative["events"]:
                    event_info = {
                        "id": event["id"],
                        "name": event["name"],
                        "keywords": event.get("keywords", [])
                    }
                    enriched_events.append(event_info)
            
            initiative_info["events"] = enriched_events
            self.enriched_initiatives.append(initiative_info)
        
        # Enrichissement des sujets communs
        self.enriched_common_topics = []
        for topic in self.data["common_topics"]:
            topic_info = {
                "id": topic["id"],
                "name": topic["name"],
                "keywords": topic.get("keywords", []),
                "subtopics": topic.get("subtopics", [])
            }
            self.enriched_common_topics.append(topic_info)
    
    def _create_relevance_analyzer(self):
        """Crée l'analyseur pour déterminer la pertinence du commentaire par rapport au post et à Orange"""
        relevance_schema = [
            ResponseSchema(name="relevance_post", 
                          description="Pertinence du commentaire par rapport au topic du post original (true/false)"),
            ResponseSchema(name="relevance_orange", 
                          description="Pertinence du commentaire par rapport à Orange ou au secteur des télécoms (true/false)"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de l'analyse de 0 à 1")
        ]
        
        relevance_parser = StructuredOutputParser.from_response_schemas(relevance_schema)
        
        relevance_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en analyse de contenu pour Orange Maroc. Analyse ce commentaire Facebook et détermine sa pertinence.
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        Détermine:
        1. Si le commentaire est pertinent par rapport au contenu du post original (reste dans le même sujet)
        2. Si le commentaire est lié à Orange ou au secteur des télécoms en général
        
        IMPORTANT:
        - Les commentaires peuvent être en arabe ou en français
        - Tiens compte du contexte culturel marocain
        - Cherche les liens sémantiques, pas seulement lexicaux
        - Utilise l'analyse du post original pour te guider
        
        {format_instructions}
        """)
        
        relevance_prompt = relevance_prompt.partial(
            format_instructions=relevance_parser.get_format_instructions()
        )
        
        return {
            "prompt": relevance_prompt,
            "parser": relevance_parser,
            "chain": relevance_prompt | llm | relevance_parser
        }
    
    def _create_general_classifier(self):
        """Crée le classificateur général pour déterminer les catégories du commentaire (désormais multiple)"""
        # Extraction des mots-clés pour chaque catégorie générale
        product_keywords = set()
        for category in self.data["product_categories"]:
            product_keywords.update(category.get("keywords", []))
        
        outage_keywords = set()
        for outage in self.data["outage_types"]:
            outage_keywords.update(outage.get("keywords", []))
        
        initiative_keywords = set()
        for initiative in self.data["initiatives"]:
            initiative_keywords.update(initiative.get("keywords", []))
        
        common_topic_keywords = set()
        for topic in self.data["common_topics"]:
            common_topic_keywords.update(topic.get("keywords", []))
        
        # Modification du schéma pour permettre plusieurs catégories
        general_schema = [
            ResponseSchema(name="content_topics", 
                          description="Liste des catégories générales du contenu: peut contenir 'produit_offre', 'initiative_evenement', 'outage', ou 'other_topic'"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        general_parser = StructuredOutputParser.from_response_schemas(general_schema)
        
        general_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en analyse de contenu pour Orange Maroc. Analyse ce commentaire Facebook et classe-le dans une ou plusieurs des catégories suivantes:
        
        - produit_offre: contenu concernant les produits ou offres d'Orange comme forfaits mobiles, internet, smartphones, etc.
          Mots-clés associés: {product_keywords}
          
        - initiative_evenement: contenu concernant les initiatives sociales, événements ou programmes d'Orange
          Mots-clés associés: {initiative_keywords}
          
        - outage: contenu concernant des problèmes de service, pannes, ou interruptions
          Mots-clés associés: {outage_keywords}
          
        - other_topic: autres sujets qui ne correspondent pas aux catégories ci-dessus
          Mots-clés associés: {common_topic_keywords}
        
        IMPORTANT:
        - Les commentaires peuvent être en arabe ou en français
        - Même si le commentaire est en arabe, utilise les mots-clés français pour comprendre le sens et l'intention du message
        - Considère les variations sémantiques et linguistiques entre l'arabe et le français
        - Considère le contexte du post original pour mieux comprendre le commentaire
        - Utilise l'analyse du post original pour guider ta classification
        - UN COMMENTAIRE PEUT APPARTENIR À PLUSIEURS CATÉGORIES SIMULTANÉMENT. Par exemple, il peut parler à la fois de problèmes (outage) d'un produit spécifique (produit_offre)
        - Retourne une LISTE de toutes les catégories qui s'appliquent
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        {format_instructions}
        """)
        
        general_prompt = general_prompt.partial(
            product_keywords=", ".join(product_keywords),
            initiative_keywords=", ".join(initiative_keywords),
            outage_keywords=", ".join(outage_keywords),
            common_topic_keywords=", ".join(common_topic_keywords),
            format_instructions=general_parser.get_format_instructions()
        )
        
        return {
            "prompt": general_prompt,
            "parser": general_parser,
            "chain": general_prompt | llm | general_parser
        }
    
    def _create_product_classifier(self):
        """Crée le classificateur pour les produits avec prise en compte des keywords"""
        product_schema = [
            ResponseSchema(name="category", 
                          description="ID de la catégorie de produit identifiée ou null si non identifiable"),
            ResponseSchema(name="subcategory", 
                          description="ID de la sous-catégorie de produit identifiée ou null si non identifiable"),
            ResponseSchema(name="product", 
                          description="ID du produit spécifique identifié ou null si non identifiable"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        product_parser = StructuredOutputParser.from_response_schemas(product_schema)
        
        product_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des produits Orange Maroc. Analyse ce commentaire Facebook qui concerne un produit ou une offre d'Orange, 
        et identifie la catégorie de produit, la sous-catégorie éventuelle et le produit spécifique.
        
        Voici les catégories de produits et leurs mots-clés associés:
        {product_categories}
        
        IMPORTANT:
        - Le commentaire peut être en arabe mais les mots-clés sont en français
        - Fais correspondre le sens du commentaire avec les mots-clés fournis
        - Une partie du commentaire peut mentionner un produit sans utiliser exactement les mêmes termes que les mots-clés
        - Interprète sémantiquement le contenu pour trouver la meilleure correspondance
        - Considère le contexte du post original pour mieux comprendre le commentaire
        - Si le post original parle déjà d'un produit spécifique, prends cela en compte dans ton analyse
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        {format_instructions}
        """)
        
        product_prompt = product_prompt.partial(
            product_categories=json.dumps(self.enriched_product_categories, ensure_ascii=False, indent=2),
            format_instructions=product_parser.get_format_instructions()
        )
        
        return {
            "prompt": product_prompt,
            "parser": product_parser,
            "chain": product_prompt | llm | product_parser
        }
    
    def _create_outage_classifier(self):
        """Crée le classificateur pour les pannes avec prise en compte des keywords"""
        outage_schema = [
            ResponseSchema(name="outage_type", 
                          description="ID du type de panne identifié ou null si non identifiable"),
            ResponseSchema(name="outage_subcategory", 
                          description="ID de la sous-catégorie de panne identifiée ou null si non identifiable"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        outage_parser = StructuredOutputParser.from_response_schemas(outage_schema)
        
        outage_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert technique d'Orange Maroc. Analyse ce commentaire Facebook qui concerne une panne ou un problème de service, 
        et identifie le type de panne et la sous-catégorie éventuelle.
        
        Voici les types de pannes et leurs mots-clés associés:
        {outage_types}
        
        IMPORTANT:
        - Le commentaire peut être en arabe mais les mots-clés sont en français
        - Fais correspondre le sens du commentaire avec les mots-clés fournis
        - Les utilisateurs peuvent décrire un problème technique sans utiliser exactement les mêmes termes que les mots-clés
        - Interprète sémantiquement le contenu pour trouver la meilleure correspondance
        - Considère le contexte du post original pour mieux comprendre le commentaire
        - Si le post original mentionne déjà une panne spécifique, prends cela en compte dans ton analyse
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        {format_instructions}
        """)
        
        outage_prompt = outage_prompt.partial(
            outage_types=json.dumps(self.enriched_outage_types, ensure_ascii=False, indent=2),
            format_instructions=outage_parser.get_format_instructions()
        )
        
        return {
            "prompt": outage_prompt,
            "parser": outage_parser,
            "chain": outage_prompt | llm | outage_parser
        }
    
    def _create_initiative_classifier(self):
        """Crée le classificateur pour les initiatives avec prise en compte des keywords"""
        initiative_schema = [
            ResponseSchema(name="initiative", 
                          description="ID de l'initiative identifiée ou null si non identifiable"),
            ResponseSchema(name="event", 
                          description="ID de l'événement spécifique identifié ou null si non identifiable"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        initiative_parser = StructuredOutputParser.from_response_schemas(initiative_schema)
        
        initiative_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert des programmes et initiatives d'Orange Maroc. Analyse ce commentaire Facebook qui concerne une initiative sociale, 
        un événement ou un programme d'Orange, et identifie l'initiative et l'événement spécifique éventuel.
        
        Voici les initiatives et leurs mots-clés associés:
        {initiatives}
        
        IMPORTANT:
        - Le commentaire peut être en arabe mais les mots-clés sont en français
        - Fais correspondre le sens du commentaire avec les mots-clés fournis
        - Cherche les correspondances contextuelles et sémantiques, pas juste les correspondances exactes de mots
        - Un nom propre comme "Brahim Diaz" doit être détecté même s'il est écrit en arabe
        - Considère le contexte du post original pour mieux comprendre le commentaire
        - Si le post original concerne déjà une initiative spécifique, prends cela en compte dans ton analyse
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        {format_instructions}
        """)
        
        initiative_prompt = initiative_prompt.partial(
            initiatives=json.dumps(self.enriched_initiatives, ensure_ascii=False, indent=2),
            format_instructions=initiative_parser.get_format_instructions()
        )
        
        return {
            "prompt": initiative_prompt,
            "parser": initiative_parser,
            "chain": initiative_prompt | llm | initiative_parser
        }
    
    def _create_common_topic_classifier(self):
        """Crée le classificateur pour les sujets communs avec prise en compte des keywords"""
        common_topic_schema = [
            ResponseSchema(name="common_topic", 
                          description="ID du sujet commun identifié ou null si non identifiable"),
            ResponseSchema(name="subtopic", 
                          description="Sous-thème spécifique identifié ou null si non identifiable"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de la classification de 0 à 1")
        ]
        
        common_topic_parser = StructuredOutputParser.from_response_schemas(common_topic_schema)
        
        common_topic_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en communication d'Orange Maroc. Analyse ce commentaire Facebook qui concerne un sujet général, 
        et identifie le thème commun et le sous-thème éventuel.
        
        Voici les sujets communs et leurs mots-clés associés:
        {common_topics}
        
        IMPORTANT:
        - Le commentaire peut être en arabe mais les mots-clés sont en français
        - Fais correspondre le sens du commentaire avec les mots-clés fournis
        - Considère les variations culturelles et linguistiques dans l'expression des besoins
        - Considère le contexte général du message pour identifier le sujet principal
        - Considère le contexte du post original pour mieux comprendre le commentaire
        - Si le post original traite déjà d'un sujet commun spécifique, prends cela en compte dans ton analyse
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        {format_instructions}
        """)
        
        common_topic_prompt = common_topic_prompt.partial(
            common_topics=json.dumps(self.enriched_common_topics, ensure_ascii=False, indent=2),
            format_instructions=common_topic_parser.get_format_instructions()
        )
        
        return {
            "prompt": common_topic_prompt,
            "parser": common_topic_parser,
            "chain": common_topic_prompt | llm | common_topic_parser
        }
    
    def _create_emotion_sentiment_analyzer(self):
        """Crée l'analyseur pour déterminer l'émotion et le sentiment du commentaire"""
        emotion_sentiment_schema = [
            ResponseSchema(name="emotion", 
                          description="Émotion principale exprimée: 'frustré', 'satisfait', 'reconnaissant', 'excité', 'interessé', 'indifférent'"),
            ResponseSchema(name="sentiment", 
                          description="Sentiment global: 'positif', 'négatif', 'neutre'"),
            ResponseSchema(name="confidence", 
                          description="Niveau de confiance de l'analyse de 0 à 1")
        ]
        
        emotion_sentiment_parser = StructuredOutputParser.from_response_schemas(emotion_sentiment_schema)
        
        emotion_sentiment_prompt = ChatPromptTemplate.from_template("""
        Tu es un expert en analyse de sentiment pour Orange Maroc. Analyse ce commentaire Facebook et détermine l'émotion principale 
        et le sentiment global exprimés.
        
        Émotions possibles:
        - frustré: le commentaire exprime de la frustration, déception, mécontentement
        - satisfait: le commentaire exprime de la satisfaction, contentement
        - reconnaissant: le commentaire exprime de la gratitude, remerciement
        - excité: le commentaire exprime de l'enthousiasme, impatience positive
        - interessé: le commentaire exprime de la curiosité, intérêt
        - indifférent: le commentaire n'exprime pas d'émotion particulière
        
        Sentiments possibles:
        - positif: le ton général du commentaire est positif
        - négatif: le ton général du commentaire est négatif
        - neutre: le ton général du commentaire est neutre ou ambivalent
        
        IMPORTANT:
        - Le commentaire peut être en arabe ou en français
        - Tiens compte des expressions culturelles marocaines pour exprimer les émotions
        - Considère le contexte du post original pour mieux comprendre le commentaire
        - Cherche les marqueurs émotionnels comme la ponctuation excessive, les majuscules, les émojis
        - Tiens compte du sujet du post original dans ton analyse
        
        Post original:
        {post_text}
        
        Analyse du post original:
        {post_analysis}
        
        Commentaire à analyser:
        {comment_text}
        
        {format_instructions}
        """)
        
        emotion_sentiment_prompt = emotion_sentiment_prompt.partial(
            format_instructions=emotion_sentiment_parser.get_format_instructions()
        )
        
        return {
            "prompt": emotion_sentiment_prompt,
            "parser": emotion_sentiment_parser,
            "chain": emotion_sentiment_prompt | llm | emotion_sentiment_parser
        }
    
    async def analyze_comment(self, comment_text: str, post_text: str, post_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyse un commentaire Facebook en prenant en compte l'analyse déjà effectuée du post original
        
        Args:
            comment_text: Le texte du commentaire à analyser
            post_text: Le texte du post original
            post_analysis: Les résultats de l'analyse du post original
            
        Returns:
            Un dictionnaire contenant les résultats de l'analyse
        """
        # Formater l'analyse du post pour une meilleure lisibilité dans les prompts
        post_analysis_formatted = json.dumps(post_analysis, indent=2, ensure_ascii=False)
        
        # 1. Analyse de la pertinence
        relevance_result = await self.relevance_analyzer["chain"].ainvoke({
            "comment_text": comment_text,
            "post_text": post_text,
            "post_analysis": post_analysis_formatted
        })
        
        # 2. Classification générale (maintenant retourne une liste de catégories)
        general_result = await self.general_classifier["chain"].ainvoke({
            "comment_text": comment_text,
            "post_text": post_text,
            "post_analysis": post_analysis_formatted
        })
        
        # 3. Analyse de l'émotion et du sentiment
        emotion_sentiment_result = await self.emotion_sentiment_analyzer["chain"].ainvoke({
            "comment_text": comment_text,
            "post_text": post_text,
            "post_analysis": post_analysis_formatted
        })
        
        # Structure de base du résultat
        result = {
            # Structure modifiée pour refléter les multiples catégories
            "content_topics": general_result["content_topics"],  # Maintenant une liste
            "category_product": None,
            "subcategory_product": None, 
            "product": None,
            "outage_type": None,
            "outage_subcategory": None,
            "initiative": None,
            "initiative_event": None,
            "common_topic": None,
            "common_subtopic": None,
            "confidence": general_result["confidence"],
            
            # Champs additionnels spécifiques aux commentaires
            "relevance_post": relevance_result["relevance_post"],
            "relevance_orange": relevance_result["relevance_orange"],
            "emotion": emotion_sentiment_result["emotion"],
            "sentiment": emotion_sentiment_result["sentiment"]
        }
        
        # Initialisation du niveau de confiance
        confidence_values = [general_result["confidence"], relevance_result["confidence"], emotion_sentiment_result["confidence"]]
        
        # Pour chaque catégorie dans la liste, exécuter l'analyseur approprié
        if "produit_offre" in general_result["content_topics"]:
            product_result = await self.product_classifier["chain"].ainvoke({
                "comment_text": comment_text,
                "post_text": post_text,
                "post_analysis": post_analysis_formatted
            })
            result["category_product"] = product_result["category"]
            result["subcategory_product"] = product_result["subcategory"]
            result["product"] = product_result["product"]
            confidence_values.append(product_result["confidence"])
            
        if "outage" in general_result["content_topics"]:
            outage_result = await self.outage_classifier["chain"].ainvoke({
                "comment_text": comment_text,
                "post_text": post_text,
                "post_analysis": post_analysis_formatted
            })
            result["outage_type"] = outage_result["outage_type"]
            result["outage_subcategory"] = outage_result["outage_subcategory"]
            confidence_values.append(outage_result["confidence"])
            
        if "initiative_evenement" in general_result["content_topics"]:
            initiative_result = await self.initiative_classifier["chain"].ainvoke({
                "comment_text": comment_text,
                "post_text": post_text,
                "post_analysis": post_analysis_formatted
            })
            result["initiative"] = initiative_result["initiative"]
            result["initiative_event"] = initiative_result["event"]
            confidence_values.append(initiative_result["confidence"])
            
        if "other_topic" in general_result["content_topics"]:
            common_topic_result = await self.common_topic_classifier["chain"].ainvoke({
                "comment_text": comment_text,
                "post_text": post_text,
                "post_analysis": post_analysis_formatted
            })
            result["common_topic"] = common_topic_result["common_topic"]
            result["common_subtopic"] = common_topic_result["subtopic"]
            confidence_values.append(common_topic_result["confidence"])
        
        # Ajuster le niveau de confiance global (prendre le minimum de toutes les confiances)
        result["confidence"] = min(confidence_values)
        
        return result


# Exemple d'utilisation modifié
async def process_facebook_comment(comment_text: str, post_text: str, post_analysis: Dict[str, Any], data_file_path='data/orange_data.json'):
    """
    Traite un commentaire Facebook en utilisant l'analyseur et en prenant en compte l'analyse du post
    
    Args:
        comment_text: Le texte du commentaire à analyser
        post_text: Le texte du post original
        post_analysis: Résultats de l'analyse du post original
        data_file_path: Chemin vers le fichier de données JSON
    
    Returns:
        Un dictionnaire contenant les résultats de l'analyse
    """
  
    
    # Charger les données structurées
    with open(data_file_path, 'r', encoding='utf-8') as f:
        orange_data = json.load(f)
    
    # Créer l'analyseur
    analyzer = FacebookCommentAnalyzer(orange_data)
    
    # Analyser le commentaire en prenant en compte l'analyse du post
    result = await analyzer.analyze_comment(comment_text, post_text, post_analysis)
    
    return result