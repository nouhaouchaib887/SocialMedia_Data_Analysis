import streamlit as st
import pandas as pd
import pymongo
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict

# Configuration de la page
st.set_page_config(
    
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Appliquer un fond noir à la sidebar
# Ajout du style CSS personnalisé
st.markdown("""
    <style>
    /* Fond noir pour la sidebar */
    [data-testid="stSidebar"] {
        background-color: black;
    }

    /* Texte en blanc dans toute la sidebar */
    [data-testid="stSidebar"] * {
        color: black !important;
    }

    /* Titre spécifique ("Tableau de bord") en blanc */
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4 {
        color: white !important;
    }

    /* Style des sliders */
    [data-testid="stSidebar"] .stSlider > div[data-baseweb="slider"] > div {
        background-color: orange !important;  /* Barre de progression */
    }

    /* Curseur du slider (le point rond) */
    [data-testid="stSidebar"] .stSlider span[role="slider"] {
        background-color: orange !important;
        border: 2px solid orange;
    }

    /* Texte à côté du slider */
    [data-testid="stSidebar"] .stSlider > div > div {
        color: white !important;
    }

    /* Boutons radio et checkbox labels en orange */
    [data-testid="stSidebar"] .stRadio > label, 
    [data-testid="stSidebar"] .stCheckbox > label {
        color: orange !important;
    }

    </style>
""", unsafe_allow_html=True)
# Styles CSS personnalisés
st.markdown("""
    <div style="background-color: black; padding: 15px 10px;">
        <h1 style="color: white; text-align: center; margin: 0;">Orange Maroc SocialPulse</h1>
    </div>
""", unsafe_allow_html=True)
st.markdown("""
<style>
    .main-header {
        font-size: 24px;
        font-weight: bold;
        color: white;
        background-color: #FF6600;
        padding: 10px;
        border-radius: 5px;
    }
    .sidebar-content {
        background-color: #333333;
        color: white;
        padding: 10px;
        border-radius: 5px;
    }
    .sidebar-header {
        font-size: 16px;
        font-weight: bold;
        color: white;
        padding: 5px;
        margin-bottom: 10px;
    }
    .comparison-header {
        font-size: 18px;
        font-weight: bold;
        margin-bottom: 15px;
    }
    .card {
        background-color: #f9f9f9;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .brand-circle {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: bold;
        margin-right: 10px;
    }
    .orange {
        background-color: #FF6600;
    }
    .inwi {
        background-color: #8B4AB2;
    }
    .maroc-telecom {
        background-color: #3FA9F5;
    }
    .metric-container {
        display: flex;
        align-items: center;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 16px;
        font-weight: bold;
        margin-left: 10px;
    }
    .progress-bar {
        height: 25px;
        border-radius: 5px;
        margin-top: 10px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 5px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        background-color: #f0f0f0;
        border-radius: 5px 5px 0 0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FF6600;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Connexion à MongoDB - à remplacer par vos propres informations de connexion
@st.cache_resource
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

# Sidebar
with st.sidebar:
    st.markdown('<div class="sidebar-content">', unsafe_allow_html=True)
    st.markdown('<h1 class="sidebar-header">TABLEAU DE BORD</h1>', unsafe_allow_html=True)
    
    menu = st.selectbox("", [
        "Vue d'ensemble", 
        "Facebook", 
        "Instagram", 
        "TikTok", 
        "Google Reviews", 
        "Analyse concurrentielle", 
        "Rapports", 
        "Paramètres"
    ], index=5)
    
    # Période de filtrage
    st.markdown('<h2 class="sidebar-header" style="margin-top:30px;">Période</h2>', unsafe_allow_html=True)
    period = st.selectbox("", ["Derniers 7 jours", "Derniers 30 jours", "Derniers 90 jours"], index=1)
    
    # Appliquer la période sélectionnée
    if period == "Derniers 7 jours":
        days = 7
    elif period == "Derniers 30 jours":
        days = 30
    else:
        days = 90
    
    st.markdown('</div>', unsafe_allow_html=True)

# Filtrage des données par période
# df_filtered = df[df['review_date'] >= (datetime.now() - timedelta(days=days))]

# On considère que les deux sont toujours sélectionnés pour cette démo
brands_to_compare = ["orange", "inwi", "iam"]

# Filtrer les données pour les marques sélectionnées
df_brands = df[df['brand'].isin(brands_to_compare)]

# Calculer les statistiques pour chaque marque
brand_stats = {}
for brand in brands_to_compare:
    df_brand = df_brands[df_brands['brand'] == brand]
    
    # Nombre d'avis
    num_reviews = len(df_brand)
    
    # Moyenne des notes
    avg_rating = round(df_brand['rating'].mean(), 2) if num_reviews > 0 else 0
    
    # Pourcentages des sentiments
    sentiment_counts = df_brand['sentiment'].value_counts(normalize=True).reindex(
        ['positive', 'neutre', 'negative'], fill_value=0)
    
    pos_percent = int(sentiment_counts['positive'] * 100) if 'positive' in sentiment_counts else 0
    neu_percent = int(sentiment_counts['neutre'] * 100) if 'neutre' in sentiment_counts else 0
    neg_percent = int(sentiment_counts['negative'] * 100) if 'negative' in sentiment_counts else 0
    
    brand_stats[brand] = {
        'num_reviews': num_reviews,
        'avg_rating': avg_rating,
        'sentiment_score': pos_percent,
        'positive': pos_percent,
        'neutre': neu_percent,
        'negative': neg_percent
    }

# Cartes de résumé pour chaque marque
cols = st.columns(3)
brand_colors = {
    "orange": "orange",
    "inwi": "inwi",
    "iam": "iam"
}
brand_letters = {
    "orange": "O",
    "inwi": "I",
    "iam": "MT"
}

for i, brand in enumerate(brands_to_compare):
    with cols[i]:
        st.markdown(f"""
        <div class="card">
            <div style="display: flex; align-items: center; margin-bottom: 15px;">
                <div class="brand-circle {brand_colors[brand]}">{brand_letters[brand]}</div>
                <div style="font-weight: bold;">{brand}</div>
            </div>
            <div style="margin-bottom: 5px;">
                {brand_stats[brand]['num_reviews']} avis • {brand_stats[brand]['avg_rating']} étoiles
            </div>
            <div>Score sentiment: {brand_stats[brand]['sentiment_score']}%</div>
        </div>
        """, unsafe_allow_html=True)

# Graphiques de comparaison
col1, col2 = st.columns(2)

# 1. Comparaison des notes Trustpilot
with col1:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<h3>Comparaison des notes Trustpilot</h3>', unsafe_allow_html=True)
    
    # Préparer les données pour le graphique des notes
    rating_data = []
    for brand in brands_to_compare:
        df_brand = df_brands[df_brands['brand'] == brand]
        for rating in range(5, 0, -1):
            count = len(df_brand[df_brand['rating'] == rating])
            rating_data.append({
                'Brand': brand,
                'Rating': f"{rating} ★" + "★" * (rating-1),
                'Count': count
            })
    
    df_ratings = pd.DataFrame(rating_data)
    
    # Créer le graphique avec Plotly
    fig_ratings = px.bar(
        df_ratings, 
        x='Rating', 
        y='Count', 
        color='Brand',
        barmode='group',
        color_discrete_map={
            'orange': '#e76616',
            'inwi': '#ce8f69',
            'iam': '#fbd2b9'
        }
    )
    fig_ratings.update_layout(
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_ratings, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# 2. Comparaison des sentiments
with col2:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<h3>Comparaison des sentiments</h3>', unsafe_allow_html=True)
    
    for brand in brands_to_compare:
        stats = brand_stats[brand]
        
        # Créer une barre de progression pour chaque marque
        st.markdown(f"""
        <div style="margin-bottom: 20px;">
            <div style="margin-bottom: 5px;">{brand}</div>
            <div style="display: flex; width: 100%; height: 25px; background-color: #f0f0f0; border-radius: 5px; overflow: hidden;">
                <div style="width: {stats['positive']}%; background-color:	#2ECC71; height: 100%;"></div>
                <div style="width: {stats['neutre']}%; background-color:	#F39C12; height: 100%;"></div>
                <div style="width: {stats['negative']}%; background-color:#E74C3C; height: 100%;"></div>
            </div>
            <div style="display: flex; justify-content: space-between; margin-top: 5px; font-size: 12px;">
                <div>{stats['positive']}%</div>
                <div>{stats['neutre']}%</div>
                <div>{stats['negative']}%</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Légende
    st.markdown("""
    <div style="display: flex; margin-top: 20px;">
        <div style="display: flex; align-items: center; margin-right: 20px;">
            <div style="width: 15px; height: 15px; background-color: #8B4AB2; margin-right: 5px;"></div>
            <div>Positif</div>
        </div>
        <div style="display: flex; align-items: center; margin-right: 20px;">
            <div style="width: 15px; height: 15px; background-color: #3FA9F5; margin-right: 5px;"></div>
            <div>Neutre</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 15px; height: 15px; background-color: #FF6600 margin-right: 5px;"></div>
            <div>Négatif</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Problèmes récurrents mentionnés par les clients
# Create tab layout
st.markdown('<div style="background-color:#1E1E1E; padding:10px; border-radius:5px;">', unsafe_allow_html=True)

# Brands configuration
brands_to_compare = ["orange", "inwi", "iam"]
brand_colors = {
    "orange": "#F39C12",
    "inwi": "#2ECC71", 
    "iam": "#E74C3C"
}
brand_letters = {"orange": "O", "inwi": "I", "iam": "M"}
brand_names = {"orange": "Orange Maroc", "inwi": "Inwi", "iam": "Maroc Telecom"}
problem_categories = [
    "Manque de transparence", 
    "Défaillance technique persistante", 
    "Problème administratif",
    "Expérience client dégradée"
    
]
problem_data = get_problem_percentages(df)

# Création du DataFrame des problèmes
problem_df = []
for category in problem_categories:
    row = {"Problème": category}
    for brand in brands_to_compare:
        row[brand] = problem_data[brand][category]
    
    # For issues, the lowest percentage is the leader (fewer problems is better)
    if category is not None:
        min_val = min(row[brand] for brand in brands_to_compare)
        leader_brand = [brand for brand in brands_to_compare if row[brand] == min_val][0]
    else:
        # For satisfaction, highest percentage is better
        max_val = max(row[brand] for brand in brands_to_compare)
        leader_brand = [brand for brand in brands_to_compare if row[brand] == max_val][0]
        
    row["Leader"] = leader_brand
    problem_df.append(row)

problem_df = pd.DataFrame(problem_df)

# Constantes
brand_colors = {
    "orange": "#F39C12",
    "inwi": "#2ECC71",
    "iam": "#E74C3C"
}
brand_letters = {"orange": "O", "inwi": "I", "iam": "M"}
brand_names = {"orange": "Orange", "inwi": "Inwi", "iam": "IAM"}
brands_to_compare = ["orange", "inwi", "iam"]
problem_categories = [
    "Manque de transparence", 
    "Défaillance technique persistante", 
    "Problème administratif",
    "Expérience client dégradée"
]

# Création du DataFrame
problem_data = get_problem_percentages(df)
problem_df = []
for category in problem_categories:
    row = {"Problème": category}
    for brand in brands_to_compare:
        row[brand] = problem_data[brand][category]
    min_val = min(row[brand] for brand in brands_to_compare)
    leader_brand = [brand for brand in brands_to_compare if row[brand] == min_val][0]
    row["Leader"] = leader_brand
    problem_df.append(row)
problem_df = pd.DataFrame(problem_df)

# HTML + CSS
html = """
<style>
    table {
        width: 100%;
        border-collapse: collapse;
        font-family: Arial, sans-serif;
        font-size: 14px;
    }
    th, td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: center;
        font-weight: bold;  /* <-- rend tout le texte du tableau gras */
    }
    th {
        background-color: black;
        color: white;
    }
    .bar {
        height: 20px;
        border-radius: 4px;
        color: white;
        font-size: 12px;
        padding-right: 5px;
        line-height: 20px;
        text-align: right;
        font-weight: bold;  /* <-- pourcentages en gras */
    }
    .problem-name {
        font-weight: bold;  /* <-- style appliqué aux noms de problème */
    }
</style>
<table>
    <tr>
        <th>Problème</th>
        <th>Orange Maroc</th>
        <th>Inwi</th>
        <th>Maroc Telecom</th>
        <th>Leader</th>
    </tr>
"""



# Construction dynamique des lignes
for _, row in problem_df.iterrows():
    html += f"<tr><td class='problem-name'> {row['Problème']}</td>"
    for brand in brands_to_compare:
        percent = row[brand]
        color = brand_colors[brand]
        html += f"""<td>
            <div class="bar" style="width:{percent*1.5}px;background-color:{color}">{percent}%</div>
        </td>"""
    leader = row["Leader"]
    html += f"""<td><b style="color:{brand_colors[leader]}">{brand_letters[leader]}</b></td></tr>"""

html += "</table>"

# Affichage dans Streamlit
st.markdown("### Problèmes récurrents mentionnés par les clients")
st.markdown(html, unsafe_allow_html=True)
st.markdown("<br><br>", unsafe_allow_html=True)