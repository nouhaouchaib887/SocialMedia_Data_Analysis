# Example usage
import google.generativeai as genai
import json
from Src.Kafka.TrustPilotKafkaConsumer import TrustpilotKafkaConsumer
import google.generativeai as genai
import json

class SentimentAnalyzer:
    def __init__(self, api_key, model_name, feedback_categories=None, product_service_categories=None):
        # Configure the API key
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.model_name = model_name
        self.feedback_categories = feedback_categories
        self.product_service_categories = product_service_categories
        self.base_prompt = self.generate_telecom_analysis_prompt()

    def generate_telecom_analysis_prompt(self):
        """
        Génère un prompt pour l'analyse des retours clients dans le secteur des télécommunications.
        """
        default_categories = [
            "Manque de transparence",
            "Défaillance technique persistante",
            "Problème administratif",
            "Expérience client dégradée",
            "satisfaction générale / recommandation",
            "hors sujet / non pertinent",
            "avis équilibré / neutre"
        ]

        default_produits_services = [
            "fibre optique",
            "ADSL",
            "Box 4G / Dar Box",
            "téléphonie mobile",
            "IPTV / télévision",
            "services transverses (SAV, service client, facturation)"
        ]

        categories_to_use = self.feedback_categories or default_categories
        produits_services_to_use = self.product_service_categories or default_produits_services

        categories_str = "\n   - ".join([f'"{cat}"' for cat in categories_to_use])
        produits_services_str = "\n   - ".join([f'"{prod}"' for prod in produits_services_to_use])

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
}}

### IMPORTANT :
Fournis UNIQUEMENT l'objet JSON, sans aucun texte ou explication supplémentaire et NE PAS INCLURE json au début.
"""
        full_prompt = self.base_prompt + "\n" + additional_prompt
        response = self.model.generate_content(full_prompt)
        start_index = response.text.find('{')
        end_index = response.text.rfind('}') + 1

        # Extraire le JSON réel
        json_string = response.text[start_index:end_index]
    
        try:
            # Attempt to parse the response as JSON
            json_output = json.loads(json_string )
            print(json.dumps(json_output, indent=2))  # Pretty-print the JSON
        except json.JSONDecodeError:
            print("Error: Could not parse JSON output.")
            print(response.text) #Print the raw output, for debugging.
        return json_output

if __name__ == "__main__":
    import pymongo
    import json 
    import time


    # Configuration MongoDB
    MONGO_URI = "mongodb://localhost:27017/"
    DB_NAME = "social_media_db"
    COLLECTION_NAME = "reviews"

    # Configuration API
    API_KEY = "AIzaSyCdlJxhy5TOOyldf0N9VfTyqbMjCWLnQtM"
    MODEL_NAME = "gemini-2.0-flash"

    # Connexion MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Instancier l'analyseur
    analyzer = SentimentAnalyzer(api_key=API_KEY, model_name=MODEL_NAME)

    # Lire les documents avec categories vide
    query = {"categories": {"$exists": True, "$size": 0}}
    cursor = collection.find(query)

    for doc in cursor:
        try:
            avis = doc.get('review_content', None)
            if not avis:
                print(f"Document {doc['_id']} n'a pas de champ 'avis'.")
                continue

            print(f"Analyse de l'avis du document {doc['_id']}...")

            # Analyse
            analysis_result= analyzer.generate_telecom_analysis(avis)

            # Vérifier si les catégories sont vides
            categories = analysis_result.get("categories", [])
            if not categories:  # Si les catégories sont vides
                print(f"API n'a pas retourné de catégories valides pour le document {doc['_id']}. Arrêt du processus.")
                break  # Arrêter le processus

            # Préparer la mise à jour
            update_fields = {
                "sentiment": analysis_result.get("sentiment", None),
                "categories": categories,
                "products_services": analysis_result.get("produits_ou_services", [])
            }

            # Mettre à jour le document
            collection.update_one(
            {"_id": doc["_id"]},
            {"$set": update_fields}
        )

            print(f"Document {doc['_id']} mis à jour avec succès.")

            # Petite pause pour éviter d'aller trop vite (facultatif)
            time.sleep(3)

        except Exception as e:
            print(f"Erreur lors du traitement du document {doc['_id']}: {e}")
            print("Arrêt du processus.")
            break

    print("Traitement terminé.")
