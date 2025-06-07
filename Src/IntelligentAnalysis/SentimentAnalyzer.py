from google import genai
import json
class sentimentAnalyzer:
    def __init__(self,llm_client,api_key,model_name,feedback_categories, product_service_categories ):
        self.api_key=api_key
        self.llm_client = genai.Client(api_key=api_key)
        self.model_name =model_name
        self.feedback_categories= feedback_categories
        self.product_service_categories = product_service_categories 
        self.base_prompt = self.generate_telecom_analysis_prompt(self, feedback_categories, product_service_categories)

    def generate_telecom_analysis_prompt(self):
        """
        Génère un prompt pour l'analyse des retours clients dans le secteur des télécommunications.
    
    Args:
        categories (list, optional): Liste des catégories à inclure. Si None, toutes les catégories par défaut seront incluses.
        produits_services (list, optional): Liste des produits/services à inclure. Si None, tous les produits/services par défaut seront inclus.
    
    Returns:
        str: Le prompt généré pour l'analyse des avis clients.
        """
        # Catégories par défaut
        default_categories = [
        "Manque de transparence",
        "Défaillance technique persistante",
        "Problème administratif",
        "Expérience client dégradée",
        "satisfaction générale / recommandation",
        "hors sujet / non pertinent",
        "avis équilibré / neutre"
     ]
    
        # Produits et services par défaut
        default_produits_services = [
        "fibre optique",
        "ADSL",
        "Box 4G / Dar Box",
        "téléphonie mobile",
        "IPTV / télévision",
        "services transverses (SAV, service client, facturation)"
        ]
    
        # Utiliser les valeurs fournies ou les valeurs par défaut
        categories_to_use = self.feedback_categories if self.feedback_categories is not None else default_categories
        produits_services_to_use = self.product_service_categories if self.product_service_categories is not None else default_produits_services
    
        # Formater les listes pour l'inclusion dans le prompt
        categories_str = "\n   - ".join([f'"{cat}"' for cat in categories_to_use])
        produits_services_str = "\n   - ".join([f'"{prod}"' for prod in produits_services_to_use])
    
        # Construire le prompt
        base_prompt = f"""
        Tu es un assistant expert en analyse des retours clients dans le secteur des télécommunications.

        Ton objectif est d'analyser l'avis suivant, et de produire une sortie structurée au format JSON STRICTEMENT VALIDE, contenant trois éléments :

1. "sentiment" : sentiment global exprimé dans l'avis (positive, negative, neutre)
2. "categories" : liste des catégories pertinentes parmi les suivantes :
   - {categories_str}

3. "produits_ou_services" : liste des produits ou services concernés dans l'avis, choisis parmi :
   - {produits_services_str}
"""
    
        return base_prompt.strip()
    

    def generate_telecom_analysis(self, avis):
        additional_prompt = f"""
    ### Voici l’avis à analyser :
    <<<
    {avis}
    >>>

    ### Format de réponse attendu (JSON STRICTEMENT VALIDE), SANS AUCUN PRÉFIXE NI SUFFIXE) :
    {{ 
      "sentiment": "...", 
      "categories": [...], 
      "produits_ou_services": [...] 
    ### IMPORTANT :
    Fournis UNIQUEMENT l'objet JSON, sans aucun texte ou explication supplémentaire et NE PAS INCLURE json au début.
    }}
    """
        prompt = self.base_prompt  + additional_prompt
        client = self.llm_client
        response = client.models.generate_content(
        model= self.model_name,
        contents= prompt
    )
    
        try:
            # Attempt to parse the response as JSON
            json_output = json.loads(response.text)
            return json.dumps(json_output, indent=2) # Pretty-print the JSON
        except json.JSONDecodeError:
            print("Error: Could not parse JSON output.")
            return(response.text) #Print the raw output, for debugging.
