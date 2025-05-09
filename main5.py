import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime, timedelta
import calendar

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Dashboard Reviews des Télécom",
    page_icon="📊",
    layout="wide"
)

# Connexion à MongoDB
@st.cache_resource
def init_connection():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        return client
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB: {e}")
        return None

# Récupération des données de MongoDB
@st.cache_data(ttl=600)
def get_data():
    client = init_connection()
    db = client["social_media_db"]  # Remplacez par le nom de votre base de données
    collection = db["reviews"]
    
    # Convertir les documents en liste de dictionnaires
    data = list(collection.find())
    
    # Convertir en DataFrame pandas
    df = pd.DataFrame(data)
    
    # Assurez-vous que les dates sont bien au format datetime
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'])
        
        # Ajout de colonnes pour faciliter les analyses
        df['year_month'] = df['review_date'].dt.strftime('%Y-%m')
        df['date_only'] = df['review_date'].dt.date
    
    return df

# Fonction pour calculer l'évolution par rapport à la veille
def calculate_evolution(df, brand=None, date_field='date_only'):
    if brand:
        filtered_df = df[df['brand'] == brand]
    else:
        filtered_df = df
        
    # Compter les reviews par jour
    daily_counts = filtered_df.groupby(date_field).size().reset_index(name='count')
    
    if len(daily_counts) < 2:
        return 0, 0
    
    # Trier par date
    daily_counts = daily_counts.sort_values(date_field)
    
    # Calculer l'évolution entre les deux derniers jours
    latest_count = daily_counts.iloc[-1]['count']
    previous_count = daily_counts.iloc[-2]['count']
    
    if previous_count == 0:
        evolution_pct = 100
    else:
        evolution_pct = ((latest_count - previous_count) / previous_count) * 100
    
    return evolution_pct, latest_count

# Fonction pour obtenir les données des 12 derniers mois
def get_last_12_months_data(df, brand=None):
    today = datetime.now()
    twelve_months_ago = today - timedelta(days=365)
    
    df_filtered = df[df['review_date'] >= twelve_months_ago]
    
    if brand:
        df_filtered = df_filtered[df_filtered['brand'] == brand]
    
    # Regrouper par mois
    monthly_data = df_filtered.groupby('year_month').size().reset_index(name='count')
    
    # Assurer que tous les 12 derniers mois sont représentés
    all_months = pd.date_range(start=twelve_months_ago, end=today, freq='MS').strftime('%Y-%m').tolist()
    complete_monthly_data = pd.DataFrame({'year_month': all_months})
    
    # Fusion avec les données existantes
    monthly_data = pd.merge(complete_monthly_data, monthly_data, on='year_month', how='left')
    monthly_data['count'] = monthly_data['count'].fillna(0)
    
    # Convertir 'year_month' en datetime pour le tri
    monthly_data['date'] = pd.to_datetime(monthly_data['year_month'] + '-01')
    monthly_data = monthly_data.sort_values('date')
    
    # Créer des libellés de mois plus lisibles
    monthly_data['month_label'] = monthly_data['date'].dt.strftime('%b %Y')
    
    return monthly_data

# Fonction pour obtenir les statistiques des sentiments
def get_sentiment_stats(df, brand=None):
    if brand:
        df_filtered = df[df['brand'] == brand]
    else:
        df_filtered = df
        
    # Vérifier si la colonne sentiment existe
    if 'sentiment' not in df_filtered.columns:
        return pd.DataFrame({'sentiment': ['positive', 'negative', 'neutral'], 'count': [0, 0, 0]})
    
    # Compter les occurrences de chaque sentiment
    sentiment_counts = df_filtered['sentiment'].value_counts().reset_index()
    sentiment_counts.columns = ['sentiment', 'count']
    
    # S'assurer que toutes les catégories sont représentées
    all_sentiments = pd.DataFrame({'sentiment': ['positive', 'negative', 'neutral']})
    sentiment_counts = pd.merge(all_sentiments, sentiment_counts, on='sentiment', how='left')
    sentiment_counts['count'] = sentiment_counts['count'].fillna(0)
    
    # Calculer le pourcentage
    total = sentiment_counts['count'].sum()
    if total > 0:
        sentiment_counts['percentage'] = (sentiment_counts['count'] / total) * 100
    else:
        sentiment_counts['percentage'] = 0
        
    return sentiment_counts

# Fonction pour obtenir les statistiques des catégories
def get_category_stats(df, brand=None):
    if brand:
        df_filtered = df[df['brand'] == brand]
    else:
        df_filtered = df
        
    # Vérifier si la colonne categories existe
    if 'categories' not in df_filtered.columns:
        return pd.DataFrame({'category': ['Aucune catégorie'], 'count': [0]})
    
    # Explode la liste des catégories pour chaque review
    categories_exploded = df_filtered.explode('categories')
    
    # Compter les occurrences de chaque catégorie
    category_counts = categories_exploded['categories'].value_counts().reset_index()
    category_counts.columns = ['category', 'count']
    
    # Calculer le pourcentage
    total = len(df_filtered)
    if total > 0:
        category_counts['percentage'] = (category_counts['count'] / total) * 100
    else:
        category_counts['percentage'] = 0
        
    # Prendre les 10 premières catégories
    top_categories = category_counts.head(10)
    
    return top_categories

# Interface utilisateur Streamlit
def main():
    # Appliquer CSS personnalisé
    st.markdown("""
    <style>
        .main-header {
            font-size: 2.5rem;
            font-weight: bold;
            color: #1E3A8A;
            margin-bottom: 1rem;
            text-align: center;
        }
        .brand-filter {
            background-color: #F3F4F6;
            padding: 1rem;
            border-radius: 10px;
            margin-bottom: 1rem;
        }
        .metric-card {
            background-color: white;
            padding: 1rem;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 1rem;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            color: #1E3A8A;
        }
        .metric-label {
            font-size: 1rem;
            color: #6B7280;
        }
        .evolution-positive {
            color: green;
        }
        .evolution-negative {
            color: red;
        }
        .chart-container {
            background-color: white;
            padding: 1rem;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 1rem;
        }
        .chart-title {
            font-size: 1.2rem;
            font-weight: bold;
            color: #1E3A8A;
            margin-bottom: 0.5rem;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Titre du dashboard
    st.markdown('<div class="main-header">Dashboard d\'Analyse des Reviews Télécom</div>', unsafe_allow_html=True)
    
    # Initialiser la connexion à MongoDB
    client = init_connection()
    
    if client is None:
        st.error("Impossible de se connecter à la base de données MongoDB. Veuillez vérifier votre configuration.")
        return
    
    # Charger les données
    try:
        df = get_data()
        
        if len(df) == 0:
            st.warning("Aucune donnée trouvée dans la collection MongoDB.")
            return
            
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {e}")
        return
    
    # Sélecteur de marque
    st.markdown('<div class="brand-filter">', unsafe_allow_html=True)
    all_brands = sorted(df['brand'].unique())
    default_brands = [brand for brand in ['Orange', 'Inwi', 'IAM'] if brand in all_brands]
    
    selected_brand = st.selectbox(
        "Sélectionner une marque",
        options=["Toutes les marques"] + all_brands,
        index=0
    )
    
    # Filtrer les données selon la marque sélectionnée
    if selected_brand != "Toutes les marques":
        filtered_df = df[df['brand'] == selected_brand]
    else:
        filtered_df = df
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Indicateurs clés de performance (KPI)
    col1, col2, col3 = st.columns(3)
    
    # KPI 1: Nombre total d'avis
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        total_reviews = len(filtered_df)
        
        # Calcul de l'évolution
        evolution_pct, _ = calculate_evolution(df, selected_brand if selected_brand != "Toutes les marques" else None)
        evolution_class = "evolution-positive" if evolution_pct >= 0 else "evolution-negative"
        evolution_sign = "+" if evolution_pct >= 0 else ""
        
        st.markdown(f'<div class="metric-value">{total_reviews}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-label">Nombre total d\'avis <span class="{evolution_class}">({evolution_sign}{evolution_pct:.1f}%)</span></div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # KPI 2: Note moyenne
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        
        avg_rating = filtered_df['rating'].mean() if len(filtered_df) > 0 else 0
        
        st.markdown(f'<div class="metric-value">{avg_rating:.1f}/5</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-label">Note moyenne</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # KPI 3: Répartition des sentiments
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        
        if 'sentiment' in filtered_df.columns:
            positive_count = (filtered_df['sentiment'] == 'positive').sum()
            positive_pct = (positive_count / len(filtered_df)) * 100 if len(filtered_df) > 0 else 0
            
            st.markdown(f'<div class="metric-value">{positive_pct:.1f}%</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-label">Avis positifs</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="metric-value">N/A</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-label">Données de sentiment non disponibles</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Graphiques
    col1, col2 = st.columns(2)
    
    # Graphique 1: Évolution des reviews sur 12 mois
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Évolution des avis sur les 12 derniers mois</div>', unsafe_allow_html=True)
        
        monthly_data = get_last_12_months_data(df, selected_brand if selected_brand != "Toutes les marques" else None)
        
        fig = px.line(
            monthly_data, 
            x='month_label', 
            y='count',
            markers=True,
            labels={'count': 'Nombre d\'avis', 'month_label': 'Mois'},
            template='plotly_white'
        )
        
        fig.update_layout(
            xaxis_title="Mois",
            yaxis_title="Nombre d'avis",
            height=400,
            margin=dict(l=20, r=20, t=30, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Graphique 2: Répartition des sentiments
    with col2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Répartition des sentiments</div>', unsafe_allow_html=True)
        
        sentiment_data = get_sentiment_stats(df, selected_brand if selected_brand != "Toutes les marques" else None)
        
        # Définir les couleurs pour chaque sentiment
        colors = {'positive': 'green', 'negative': 'red', 'neutral': 'gray'}
        
        fig = px.pie(
            sentiment_data, 
            values='count', 
            names='sentiment',
            color='sentiment',
            color_discrete_map=colors,
            hole=0.4,
            labels={'count': 'Nombre d\'avis', 'sentiment': 'Sentiment'}
        )
        
        fig.update_traces(textinfo='percent+label')
        
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=30, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Graphique 3: Top catégories
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Top 10 des catégories mentionnées</div>', unsafe_allow_html=True)
    
    category_data = get_category_stats(df, selected_brand if selected_brand != "Toutes les marques" else None)
    
    if len(category_data) > 0:
        fig = px.bar(
            category_data.sort_values('count', ascending=True).tail(10), 
            y='category', 
            x='count',
            text='percentage',
            labels={'count': 'Nombre de mentions', 'category': 'Catégorie', 'percentage': 'Pourcentage'},
            orientation='h',
            color='count',
            color_continuous_scale='blues'
        )
        
        fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        
        fig.update_layout(
            xaxis_title="Nombre de mentions",
            yaxis_title="Catégorie",
            height=500,
            margin=dict(l=20, r=20, t=30, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée de catégorie disponible pour cette sélection.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Tableau de données récentes
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Derniers avis reçus</div>', unsafe_allow_html=True)
    
    # Sélectionner les colonnes pertinentes
    display_columns = ['brand', 'rating', 'review_content', 'review_date']
    if 'sentiment' in filtered_df.columns:
        display_columns.append('sentiment')
    
    # Trier par date décroissante et afficher les 10 derniers avis
    recent_reviews = filtered_df[display_columns].sort_values('review_date', ascending=False).head(10)
    
    # Formatter la date
    if 'review_date' in recent_reviews.columns:
        recent_reviews['review_date'] = recent_reviews['review_date'].dt.strftime('%d/%m/%Y')

    # Tronquer le contenu de la review pour l'affichage
    if 'review_content' in recent_reviews.columns:
        recent_reviews['review_content'] = recent_reviews['review_content'].str.slice(0, 100) + '...'
    
    st.dataframe(recent_reviews, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown('<div style="text-align: center; color: #6B7280; padding: 1rem;">Dashboard développé avec Streamlit</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()