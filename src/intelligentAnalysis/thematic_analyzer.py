import json
from typing import Dict, Any
from base_analyzer import BaseAnalyzer


class ThematicAnalyzer(BaseAnalyzer):
    """Handles thematic classification and hierarchical analysis"""
    
    def setup_analyzers(self):
        """Setup thematic analyzers"""
        self.theme_classifier = self._create_classifier_from_yaml("thematic")
        self.offre_category_classifier = self._create_classifier_from_yaml("offer_category")
        self.offre_subcategory_classifier = self._create_classifier_from_yaml("offer_subcategory")
        self.offre_product_classifier = self._create_classifier_from_yaml("offer")
        self.initiative_classifier = self._create_classifier_from_yaml("initiative")
        self.initiative_event_classifier = self._create_classifier_from_yaml("initiative_event")
        self.communication_topic_classifier = self._create_classifier_from_yaml("communication_topic")
        self.communication_subtopic_classifier = self._create_classifier_from_yaml("communication_subtopic")
    
    async def classify_theme(self, result: Dict[str, Any],content_type,text: str,post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
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
    
    async def classify_offre_hierarchical(self, content_type,text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
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
    
    async def classify_initiative_hierarchical(self,content_type, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
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
    
    async def classify_communication_hierarchical(self,content_type, text: str, result: Dict[str, Any],post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
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