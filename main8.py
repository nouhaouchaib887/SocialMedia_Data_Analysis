import streamlit as st
import pandas as pd
import pymongo
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict
def init_connection():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        return client
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB: {e}")
        return None

client = init_connection()

# Fonction pour récupérer les données
@st.cache_data(ttl=3600)
def get_data():
    if client:
        try:
            db = client["social_media_db"]
            collection = db["reviews"]
            # Récupérer les avis des 30 derniers jours
            cursor = collection.find()
            
            # Convertir en DataFrame pandas
            df = pd.DataFrame(list(cursor))
            print("df recuperee")
            return df
        except Exception as e:
            st.error(f"Erreur lors de la récupération des données: {e}")
            # Retourner des données fictives en cas d'erreur
            return generate_sample_data()
    else:
        # Si pas de connexion, utiliser des données fictives
        return generate_sample_data()

# Génération de données fictives pour la démo
def generate_sample_data(n=500):
    brands = ["orange", "inwi", "iam"]
    sentiments = ["positive", "neutre", "negative"]
    categories = ["Manque de transparence", "Défaillance technique persistante", "Problème administratif", 
                  "Expérience client dégradée", "Satisfaction générale / recommandation","hors sujet / non pertinent","avis équilibré / neutre"]
    
    data = []
    for _ in range(n):
        brand = np.random.choice(brands)
        rating = np.random.randint(1, 6)
        
        # Définir le sentiment en fonction de la note
        if rating >= 4:
            sentiment = "positive"
        elif rating == 3:
            sentiment = "neutre"
        else:
            sentiment = "negative"
        
        # Distribution des catégories par marque
        if brand == "orange":
            cat_probs = [0.25, 0.12, 0.24, 0.10, 0.29]
        elif brand == "iwi":
            cat_probs = [0.35, 0.18, 0.16, 0.11, 0.20]
        else:  # Maroc Telecom
            cat_probs = [0.15, 0.26, 0.21, 0.08, 0.30]
        
        category = np.random.choice(categories, p=cat_probs)
        
        days_ago = np.random.randint(1, 31)
        review_date = datetime.now() - timedelta(days=days_ago)
        
        data.append({
            "platform": "trustpilot",
            "brand": brand,
            "review_id": f"review_{_}",
            "rating": rating,
            "review_content": f"Exemple d'avis pour {brand}",
            "review_date": review_date,
            "sentiment": sentiment,
            "categories": [category],
            "products_services": ["Internet", "Mobile"] if np.random.random() > 0.5 else ["Internet"]
        })
    
    return pd.DataFrame(data)

def get_problem_percentages(df):
    # Initialiser les structures
    problem_counts = defaultdict(lambda: defaultdict(int))
    total_counts = defaultdict(int)

    # Parcourir les lignes du DataFrame
    for _, row in df.iterrows():
        brand = row.get('brand')
        categories = row.get('categories', [])

        # Vérifier que categories est bien une liste
        if not isinstance(categories, list):
            continue

        for category in categories:
            problem_counts[brand][category] += 1
            total_counts[brand] += 1

    # Calculer les pourcentages
    percentage_data = {}
    for brand, categories in problem_counts.items():
        total = total_counts[brand]
        percentage_data[brand] = {
            category: round((count / total) * 100, 2)  # en pourcentage avec 2 décimales
            for category, count in categories.items()
        }

    return percentage_data

# Récupération des données
df = get_data()
# Sauvegarde du DataFrame en fichier CSV
df.to_csv("avis_clients.csv", index=False, encoding='utf-8-sig')