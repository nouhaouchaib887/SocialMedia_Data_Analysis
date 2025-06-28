import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta
import asyncio
from src.intelligentAnalysis.contentAnalyzer import ContentAnalyzer
from src.collectors.facebook_scrapper.facebook_apify_collector import FacebookAPifyCollector
from src.collectors.facebook_scrapper.facebook_search_collector import FacebookSearchCollector
from src.collectors.instagram_scrapper.instagram_apify_collector import InstagramAPifyCollector
from src.collectors.tiktok_scrapper.tiktok_apify_collector import TiktokAPifyCollector
import yaml
#from langchain.schema.prompt import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
import asyncio


# Import des classes (à adapter selon votre structure de projet)
# from your_module import FacebookAPifyCollector, FacebookSearchCollector, InstagramAPifyCollector, TiktokAPifyCollector, ContentAnalyzer
import yaml
from pathlib import Path
import src.intelligentAnalysis.externalContentAnalyzer as ExternalContentAnalyzer

# Chemin vers le fichier settings.yaml
settings_path = Path(__file__).parent.parent / "config" / "Settings.yaml"


# Lecture du fichier YAML
with open(settings_path, "r") as f:
    config = yaml.safe_load(f)

# Récupération du token Apify
apify_token = config.get("api_keys", {}).get("apify", "")
GOOGLE_API_KEY =config.get("api_keys", {}).get("gemini_key", "")
print(apify_token)
# Configuration de la page
st.set_page_config(
    page_title="Analyse des Retours Clients",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration des marques et leurs informations
BRANDS_INFO = {
    "Orange Maroc": {
        "facebook": {"brand_name": "orangemaroc", "page_name": "orangemaroc"},
        "instagram": {"brand_name": "orangemaroc", "user_name": "orangemaroc"},
        "tiktok": {"brand_name": "orangemaroc", "profile_name": "orangemaroc"},
        "logo": "🟠"
    },
    "Inwi": {
        "facebook": {"brand_name": "inwi.ma", "page_name": "inwi.ma"},
        "instagram": {"brand_name": "inwi_maroc", "user_name": "inwi_maroc"},
        "tiktok": {"brand_name": "inwi.maroc", "profile_name": "inwi.maroc"},
        "logo": "🔴"
    },
    "Maroc Telecom": {
        "facebook": {"brand_name": "maroctelecom", "page_name": "maroctelecom"},
        "instagram": {"brand_name": "maroctelecom", "user_name": "maroctelecom"},
        "tiktok": {"brand_name": "maroctelecom", "profile_name": "maroctelecom"},
        "logo": "🔵"
    }
}

# Sidebar Navigation
def sidebar_navigation():
    st.sidebar.title("📊 Navigation")
    
    pages = {
        "🏠 Introduction": "introduction",
        "🏗️ Architecture": "architecture", 
        "🧪 Tests de Fonctionnalités": "tests",
        "📊 Benchmarking des LLMs": "benchmarking"
    }
    
    # Affichage des logos des marques
    st.sidebar.markdown("### 📱 Marques")
    cols = st.sidebar.columns(3)
    with cols[0]:
        st.markdown("🟠 Orange")
    with cols[1]:
        st.markdown("🔴 Inwi")
    with cols[2]:
        st.markdown("🔵 Maroc Telecom")
    
    st.sidebar.markdown("### 🌐 Plateformes")
    st.sidebar.markdown("📘 Facebook | 📷 Instagram | 🎵 TikTok")
    
    st.sidebar.markdown("---")
    
    selected_page = st.sidebar.selectbox(
        "Choisir une page:",
        options=list(pages.keys()),
        index=0
    )
    
    return pages[selected_page]

# Page Introduction
def show_introduction():
    st.title("🏠 Introduction")
    st.markdown("""
    ## Bienvenue dans l'Application d'Analyse des Retours Clients
    
    Cette application permet de :
    - **Collecter** des données depuis Facebook, Instagram et TikTok
    - **Analyser** les retours clients et la veille concurrentielle
    - **Visualiser** les résultats d'analyse thématique et de sentiment
    
    ### 🎯 Objectifs
    - Surveiller les mentions des marques télécoms marocaines
    - Analyser les sentiments et thématiques des conversations
    - Comparer les performances des différentes marques
    """)

# Page Architecture
def show_architecture():
    st.title("🏗️ Architecture")
    st.markdown("""
    ## Architecture du Système
    
    ### 📊 Flux de Données
    1. **Collecte** : Extraction des données via APIs
    2. **Traitement** : Nettoyage et structuration
    3. **Analyse** : Classification thématique et analyse de sentiment
    4. **Visualisation** : Présentation des résultats
    
    ### 🛠️ Technologies Utilisées
    - **Streamlit** : Interface utilisateur
    - **LangChain** : Traitement du langage naturel
    - **Apify** : Collecte de données
    - **Pandas** : Manipulation des données
    """)

# Page Tests de Fonctionnalités
def show_tests():
    st.title("🧪 Tests de Fonctionnalités")
    
    # Tabs pour séparer collecte et analyse
    tab1, tab2 = st.tabs(["📥 Collecte de Données", "🔍 Analyse des Données"])
    
    with tab1:
        show_collection_interface()
    
    with tab2:
        show_analysis_interface_sync()

def show_collection_interface():
    st.header("📥 Fonctionnalité de Collecte")
    
    # Sélection de la source
    source = st.selectbox(
        "🌐 Sélectionner une source pour la collecte:",
        ["Facebook", "Instagram", "TikTok"]
    )
    
    if source == "Facebook":
        show_facebook_collection()
    elif source == "Instagram":
        show_instagram_collection()
    elif source == "TikTok":
        show_tiktok_collection()

def show_facebook_collection():
    st.subheader("📘 Collecte Facebook")
    
    collection_type = st.radio(
        "Type de collecte:",
        ["Pages Officielles", "Search Query (Veille Extérieure)"]
    )
    
    if collection_type == "Pages Officielles":
        st.markdown("### Pages Officielles des Opérateurs")
        
        brand = st.selectbox("Choisir la marque:", list(BRANDS_INFO.keys()))
        
        col1, col2, col3 = st.columns(3)
        with col1:
            max_posts = st.number_input("Max Posts", min_value=1, max_value=100, value=10)
        with col2:
            max_comments_per_post = st.number_input("Max Comments par Post", min_value=1, max_value=50, value=5)
        with col3:
            days_back = st.number_input("Jours en arrière", min_value=1, max_value=90, value=7)
        
        if st.button("🚀 Lancer la Collecte (Pages Officielles)"):
            brand_info = BRANDS_INFO[brand]["facebook"]
            
            # Initialiser collected_data si nécessaire
            if 'collected_data' not in st.session_state:
                st.session_state.collected_data = {
                    "orange_posts": [],
                    "competitor_posts": [],
                    "external_posts": []
                }
            
            with st.spinner("Collecte en cours..."):
                st.info(f"""
                **Paramètres de collecte:**
                - Brand: {brand}
                - Brand Name: {brand_info['brand_name']}
                - Page Name: {brand_info['page_name']}
                - Max Posts: {max_posts}
                - Max Comments: {max_comments_per_post}
                - Days Back: {days_back}
                """)
                
                # Appel à la classe FacebookAPifyCollector
                collector = FacebookAPifyCollector(
                    apify_token=apify_token,
                    brand_name=brand_info['brand_name'],
                    page_name=brand_info['page_name'],
                    max_posts=max_posts,
                    max_comments_per_post=max_comments_per_post,
                    days_back=days_back
                )
                posts = collector.collect_posts()
                
                # Collecte des commentaires pour chaque post
                for post in posts: 
                    if post.get("permalink"):
                        try:
                            print(f"🔍 Collecting comments for post {post.get('post_id')}...")
                            comments = collector.collect_comments_for_post(post["permalink"])
                            post["comments"] = comments
                        except Exception as e: 
                            print(f"Erreur lors de la collecte des commentaires: {e}")
                            post["comments"] = []  # Assurer qu'il y a toujours une liste de commentaires
                
                # Traitement des posts selon la marque
                if brand_info['brand_name'] == "orangemaroc":     
                    for post in posts:
                        post_data = {
                            "content": post.get("message", ""),
                            "date": post.get("created_time", ""),
                            "platform": "facebook",
                            "comments": [{"text": comment.get("message", "")} for comment in post.get("comments", [])]
                        }
                        print(post_data)
                        st.session_state.collected_data["orange_posts"].append(post_data)
                else:
                    for post in posts:
                        post_data = {
                            "brand": brand_info["brand_name"],
                            "content": post.get("message", ""),
                            "date": post.get("created_time", ""),
                            "platform": "facebook",
                            "comments": [{"text": comment.get("message", "")} for comment in post.get("comments", [])]  # Correction: post.get au lieu de posts.get
                        }
                        st.session_state.collected_data["competitor_posts"].append(post_data)
                
                display_collection_results(posts, brand_info["brand_name"], "facebook")
    
    else:  # Search Query
        st.markdown("### Veille Extérieure")
        
        search_query = st.text_input("🔍 Search Query:", placeholder="Ex: Orange Maroc problème réseau")
        brand_name = st.text_input("Brand Name:", placeholder="Ex: orangemaroc")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            max_posts = st.number_input("Max Posts", min_value=1, max_value=100, value=10, key="search_posts")
        with col2:
            max_comments_per_post = st.number_input("Max Comments par Post", min_value=1, max_value=50, value=5, key="search_comments")
        with col3:
            post_time_range = st.selectbox("Période", ["30d", "90d"])
        
        if st.button("🚀 Lancer la Collecte (Search Query)"):
            if search_query and brand_name:
                # Initialiser collected_data si nécessaire
                if 'collected_data' not in st.session_state:
                    st.session_state.collected_data = {
                        "orange_posts": [],
                        "competitor_posts": [],
                        "external_posts": []
                    }
                
                with st.spinner("Collecte en cours..."):
                    st.info(f"""
                    **Paramètres de recherche:**
                    - Search Query: {search_query}
                    - Brand Name: {brand_name}
                    - Max Posts: {max_posts}
                    - Max Comments: {max_comments_per_post}
                    - Période: {post_time_range}
                    """)
                    
                    collector = FacebookSearchCollector(
                        apify_token=apify_token,
                        brand_name=brand_name,
                        search_query=search_query,
                        max_posts=max_posts,
                        max_comments_per_post=max_comments_per_post,
                        post_time_range=post_time_range
                    )
                    posts = collector.collect_all_data()
                    
                    for post in posts:
                        post_data = {
                            "brand": brand_name,
                            "content": post.get("message", ""),
                            "date": post.get("created_time", ""),
                            "platform": "facebook",
                            "search_query": search_query,  # Correction: underscore au lieu d'espace
                            "source": "Facebook Search",
                            "brand_name": post.get("brand_name", ""),
                            "comments": [{"text": comment.get("message", "")} for comment in post.get("comments", [])]
                        }
                        st.session_state.collected_data["external_posts"].append(post_data)
                    
                    display_collection_results(posts, brand_name, "facebook")
            else:
                st.error("Veuillez remplir tous les champs obligatoires")

def show_instagram_collection():
    st.subheader("📷 Collecte Instagram")
    st.markdown("### Profils Officiels uniquement")
    
    brand = st.selectbox("Choisir la marque:", list(BRANDS_INFO.keys()), key="insta_brand")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        max_posts = st.number_input("Max Posts", min_value=1, max_value=100, value=10, key="insta_posts")
    with col2:
        max_comments_per_post = st.number_input("Max Comments par Post", min_value=1, max_value=50, value=5, key="insta_comments")
    with col3:
        days_back = st.number_input("Jours en arrière", min_value=1, max_value=90, value=7, key="insta_days")
    
    if st.button("🚀 Lancer la Collecte Instagram"):
        brand_info = BRANDS_INFO[brand]["instagram"]
        
        # Initialiser collected_data si nécessaire
        if 'collected_data' not in st.session_state:
            st.session_state.collected_data = {
                "orange_posts": [],
                "competitor_posts": [],
                "external_posts": []
            }
        
        with st.spinner("Collecte en cours..."):
            st.info(f"""
            **Paramètres de collecte Instagram:**
            - Brand: {brand}
            - Brand Name: {brand_info['brand_name']}
            - User Name: {brand_info['user_name']}
            - Max Posts: {max_posts}
            - Max Comments: {max_comments_per_post}
            - Days Back: {days_back}
            """)
            
            collector = InstagramAPifyCollector(
                apify_token=apify_token,
                brand_name=brand_info['brand_name'],
                user_name=brand_info['user_name'],
                max_posts=max_posts,
                max_comments_per_post=max_comments_per_post,
                days_back=days_back
            )
            posts = collector.collect_posts()
            
            # Collecte des commentaires pour chaque post
            for post in posts:
                if post.get("permalink"):
                    try:
                        print(f"🔍 Collecting comments for post {post.get('post_id')}...")
                        comments = collector.collect_comments_for_post(post["permalink"])
                        post["comments"] = comments
                    except Exception as e:
                        print(f"Erreur lors de la collecte des commentaires: {e}")
                        post["comments"] = []
            
            # Traitement des posts selon la marque
            for post in posts:
                post_data = {
                    "brand": brand_info['brand_name'],
                    "content": post.get("message", ""),
                    "date": post.get("created_time", ""),
                    "platform": "instagram",
                    "comments": [{"text": comment.get("message", "")} for comment in post.get("comments", [])]
                }
                
                if brand_info['brand_name'] == "orangemaroc":
                    st.session_state.collected_data["orange_posts"].append(post_data)
                else:
                    st.session_state.collected_data["competitor_posts"].append(post_data)
            
            display_collection_results(posts, brand_info['brand_name'], "instagram")

def show_tiktok_collection():
    st.subheader("🎵 Collecte TikTok")
    st.markdown("### Profils Officiels uniquement")
    
    brand = st.selectbox("Choisir la marque:", list(BRANDS_INFO.keys()), key="tiktok_brand")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        max_posts = st.number_input("Max Posts", min_value=1, max_value=100, value=10, key="tiktok_posts")
    with col2:
        max_comments_per_post = st.number_input("Max Comments par Post", min_value=1, max_value=50, value=5, key="tiktok_comments")
    with col3:
        days_back = st.number_input("Jours en arrière", min_value=1, max_value=90, value=7, key="tiktok_days")
    
    if st.button("🚀 Lancer la Collecte TikTok"):
        brand_info = BRANDS_INFO[brand]["tiktok"]
        
        # Initialiser collected_data si nécessaire
        if 'collected_data' not in st.session_state:
            st.session_state.collected_data = {
                "orange_posts": [],
                "competitor_posts": [],
                "external_posts": []
            }
        
        with st.spinner("Collecte en cours..."):
            st.info(f"""
            **Paramètres de collecte TikTok:**
            - Brand: {brand}
            - Brand Name: {brand_info['brand_name']}
            - Profile Name: {brand_info['profile_name']}
            - Max Posts: {max_posts}
            - Max Comments: {max_comments_per_post}
            - Days Back: {days_back}
            """)
            
            collector = TiktokAPifyCollector(
                apify_token=apify_token,
                brand_name=brand_info['brand_name'],
                profile_name=brand_info['profile_name'],
                max_posts=max_posts,
                max_comments_per_post=max_comments_per_post,
                days_back=days_back
            )
            brand_name = brand_info['brand_name']
            posts = collector.collect_posts()
            
            # Collecte des commentaires pour chaque post
            for post in posts:
                if post.get("permalink"):
                    try:
                        print(f"🔍 Collecting comments for post {post.get('post_id')}...")
                        comments = collector.collect_comments_for_post(post["permalink"])
                        post["comments"] = comments
                    except Exception as e:
                        print(f"Erreur lors de la collecte des commentaires: {e}")
                        post["comments"] = []
            
            # Traitement des posts selon la marque
            for post in posts:
                post_data = {
                    "brand": brand_name,
                    "content": post.get("message", ""),
                    "date": post.get("created_time", ""),
                    "platform": "tiktok",
                    "comments": [{"text": comment.get("message", "")} for comment in post.get("comments", [])]
                }
                
                if brand_name == "orangemaroc":
                    st.session_state.collected_data["orange_posts"].append(post_data)
                else:
                    st.session_state.collected_data["competitor_posts"].append(post_data)
            
            display_collection_results(posts, brand_name, "tiktok")

def display_collection_results(posts, brand, platform):
    st.success("✅ Collecte terminée!")
    results_posts = {
        "brand": brand,
        "platform": platform,
        "collection_date": datetime.now().isoformat(),
        "posts": posts  # Correction: directement posts au lieu de []
    }
    
    print(posts)
    print(type(posts))
  
    # Affichage du JSON brut
    with st.expander("📄 Données JSON brutes des posts"):
        st.json(results_posts)
    
    # Affichage sous forme de tableau
    st.markdown("### 📊 Résultats sous forme de tableau")
    
    # Correction: vérifier directement posts au lieu de results_posts["posts"]
    if posts:
        posts_data = []
        for post in posts:
            posts_data.append({
                "ID": post.get("post_id", "N/A"),
                "Date": post.get("created_time", "N/A"),
                "Contenu": post.get("content", "")[:100] + "..." if len(post.get("content", "")) > 100 else post.get("content", ""),
                "Likes": post.get("like_count", 0),
                "Commentaires": post.get("comments_count", 0),
                "Partages": post.get("shares", 0)
            })
    
        df_posts = pd.DataFrame(posts_data)
        st.dataframe(df_posts, use_container_width=True)
def show_analysis_interface_sync():
    st.header("🔍 Fonctionnalité d'Analyse")
    analysis_type = st.selectbox(
        "Type d'analyse:",
        ["Veille Extérieure", "Veille Concurrentielle", "Analyse de Page Officielle"]
    )
    
    # Initialisation de collected_data
    if 'collected_data' not in st.session_state:
        st.session_state.collected_data = {
            "orange_posts": [],
            "competitor_posts": [],
            "external_posts": []
        }
    
    if analysis_type == "Veille Extérieure":
        sync_show_external_monitoring_analysis()
    elif analysis_type == "Veille Concurrentielle":
        sync_show_competitive_analysis()
    elif analysis_type == "Analyse de Page Officielle":
        sync_show_official_page_analysis()
def sync_show_analysis_interface():
    """Wrapper synchrone pour la fonction asynchrone"""
    asyncio.run(show_analysis_interface())

def sync_show_external_monitoring_analysis():
    """Wrapper synchrone pour la fonction asynchrone"""
    asyncio.run(show_external_monitoring_analysis())

def sync_show_competitive_analysis():
    """Wrapper synchrone pour la fonction asynchrone"""
    asyncio.run(show_competitive_analysis())

def sync_show_official_page_analysis():
    """Wrapper synchrone pour la fonction asynchrone"""
    asyncio.run(show_official_page_analysis())

async def show_analysis_interface():
    st.header("🔍 Fonctionnalité d'Analyse")
    analysis_type = st.selectbox(
        "Type d'analyse:",
        ["Veille Extérieure", "Veille Concurrentielle", "Analyse de Page Officielle"]
    )
    
    # Initialisation de collected_data
    if 'collected_data' not in st.session_state:
        st.session_state.collected_data = {
            "orange_posts": [],
            "competitor_posts": [],
            "external_posts": []
        }
    
    if analysis_type == "Veille Extérieure":
        await show_external_monitoring_analysis()
    elif analysis_type == "Veille Concurrentielle":
        await show_competitive_analysis()
    elif analysis_type == "Analyse de Page Officielle":
        await show_official_page_analysis()

async def show_external_monitoring_analysis():
    st.subheader("🌐 Veille Extérieure")
    
    # Liste des posts pour la veille extérieure
    posts = st.session_state.collected_data["external_posts"]
    
    if not posts:
        st.info("Aucun post de veille extérieure collecté pour le moment.")
        return
    
    st.markdown("### Posts collectés:")
    for i, post in enumerate(posts):
        with st.expander(f"Post {i+1}: {post['content'][:50]}..."):
            st.write(f"**Contenu:** {post['content']}")
            st.write(f"**Date:** {post['date']}")
            st.write(f"**Source:** {post['source']}")
            st.write(f"**Search Query:** {post['search_query']}"),
    
            # Affichage des commentaires
            st.markdown("**Commentaires:**")
            for j, comment in enumerate(post['comments']):
                st.write(f"- {comment['text']}")
                if st.button(f"🔍 Analyser ce commentaire", key=f"analyze_ext_comment_{i}_{j}"):
                    await analyze_single_comment(post["brand_name"],comment['text'], post['content'])
            
            if st.button(f"🔍 Analyser ce post", key=f"analyze_ext_post_{i}"):
                await analyze_single_post_(post['brand_name'], post['content'])

async def show_competitive_analysis():
    st.subheader("🏢 Veille Concurrentielle")
    
    competitor = st.selectbox("Choisir le concurrent:", ["Inwi", "Maroc Telecom"])
    
    # Posts du concurrent sélectionné
    posts = [p for p in st.session_state.collected_data["competitor_posts"] if p.get("brand") == competitor]
    
    if competitor == "Inwi":
        brand_name = "inwi"
    else:
        brand_name = "maroc_telecom"
    
    if not posts:
        st.info(f"Aucun post de {competitor} collecté pour le moment.")
        return
    
    st.markdown(f"### Posts de {competitor}:")
    for i, post in enumerate(posts):
        with st.expander(f"Post {i+1}: {post['content'][:50]}..."):
            st.write(f"**Contenu:** {post['content']}")
            st.write(f"**Date:** {post['date']}")
            st.write(f"**Plateforme:** {post['platform']}")
            
            # Affichage des commentaires
            st.markdown("**Commentaires:**")
            for j, comment in enumerate(post['comments']):
                st.write(f"- {comment['text']}")
                if st.button(f"🔍 Analyser ce commentaire", key=f"analyze_comp_comment_{competitor}_{i}_{j}"):
                    await analyze_single_comment(brand_name,comment['text'], post['content'])
            
            if st.button(f"🔍 Analyser ce post", key=f"analyze_comp_post_{competitor}_{i}"):
                await analyze_single_post(brand_name, post['content'])

async def show_official_page_analysis():
    st.subheader("🟠 Analyse de Page Officielle - Orange")
    
    # Posts d'Orange
    posts = st.session_state.collected_data["orange_posts"]
    
    if not posts:
        st.info("Aucun post Orange collecté pour le moment.")
        return
    
    st.markdown("### Posts d'Orange Maroc:")
    for i, post in enumerate(posts):
        with st.expander(f"Post {i+1}: {post['content'][:50]}..."):
            st.write(f"**Contenu:** {post['content']}")
            st.write(f"**Date:** {post['date']}")
            
            # Affichage des commentaires
            st.markdown("**Commentaires:**")
            for j, comment in enumerate(post['comments']):
                st.write(f"- {comment['text']}")
                if st.button(f"🔍 Analyser ce commentaire", key=f"analyze_orange_comment_{i}_{j}"):
                    await analyze_single_comment("orangemaroc",comment['text'], post['content'])
            
            if st.button(f"🔍 Analyser ce post", key=f"analyze_orange_post_{i}"):
                await analyze_single_post("orangemaroc", post['content'])

async def analyze_single_post(brand_name, post_content):
    st.markdown("---")
    st.markdown("### 📊 Analyse du Post")
    
    yaml_file_path = "../config.prompts/prompts_o.yaml"
    
    # Affichage du prompt thématique
    analyze_type = "thematic"
    
    st.code("""
Analysez le thème principal de ce post:
- Offre commerciale
- Initiative de l'entreprise
- Communication/Interaction
- Autre
""")
    
    # Analyse thématique
    with st.spinner("Analyse thématique en cours..."):
        result = {
            "theme": {
                "id": "",
                "name": ""
            },
            "confidence":""
        }
        
        analyzer = ContentAnalyzer(google_api_key=GOOGLE_API_KEY, brand_name=brand_name)
        theme_result = await analyzer._classify_theme(result, "post", post_content, "", None)
        print(theme_result)
        theme_id = theme_result["theme"]["id"]
        
        # Affichage des résultats sous forme de tableau
        st.markdown("#### 📋 Résultats d'Analyse thématique:")
        results_df = pd.DataFrame([
            ["Thème Principal", theme_result["theme"]["name"]],
            ["ID Thème", theme_result["theme"]["id"]],
            ["Confiance", theme_result["confidence"]],
        ], columns=["Métrique", "Valeur"])
        st.dataframe(results_df, use_container_width=True)
    
    # Classification spécifique selon le thème - HIÉRARCHIQUE
    if theme_id == "offre":
        with st.spinner("Analyse des offres en cours..."):
            result = await analyzer._classify_offre_hierarchical("post", post_content, theme_result, "", None)
            offre_result = result["category_offre"]
            print(offre_result)
            # Affichage des résultats sous forme de tableau
            st.markdown("#### 📋 Résultats d'Analyse des Offres:")
            results_df = pd.DataFrame([
                ["Catégorie d'offre", offre_result["name"]],
                ["Sous Catégorie d'offre", offre_result["subcategory_offre"]["name"]],
                ["Confiance", result["confidence"]],
                ["Offre", offre_result["subcategory_offre"]["offre"]]
            ], columns=["Métrique", "Valeur"])
            st.dataframe(results_df, use_container_width=True)
    
    elif theme_id == "initiative":
        with st.spinner("Analyse des initiatives en cours..."):
            result = await analyzer._classify_initiative_hierarchical("post", post_content, theme_result, "", None)
            initiative_result = result["initiative"]
            
            # Affichage des résultats sous forme de tableau
            st.markdown("#### 📋 Résultats d'Analyse des Initiatives:")
            results_df = pd.DataFrame([
                ["Initiative", initiative_result["name"]],
                ["Confiance", result["confidence"]],
                ["Evenement", initiative_result["event"] or ""]
            ], columns=["Métrique", "Valeur"])
            st.dataframe(results_df, use_container_width=True)
    
    elif theme_id == "communication_interaction":
        with st.spinner("Analyse de communication en cours..."):
            result = await analyzer._classify_communication_hierarchical("post", post_content, theme_result, "", None)
            communication_result = result["communication_interaction_topic"]
            
            # Affichage des résultats sous forme de tableau
            st.markdown("#### 📋 Résultats d'Analyse de Communication:")
            results_df = pd.DataFrame([
                ["Sujet", communication_result["name"]],
                ["Confiance", result["confidence"]],
                ["Subtopic", communication_result["subtopic"]]
            ], columns=["Métrique", "Valeur"])
            st.dataframe(results_df, use_container_width=True)
    
    # Classification des intentions basée sur le thème
    if theme_id != "none":
        with st.spinner("Analyse des intentions en cours..."):
            result = await analyzer._classify_intent("post", post_content, theme_result, theme_id, "", None)
            intent_result = result["intent"]
            
            # Affichage des résultats d'intention
            st.markdown("#### 📋 Résultats d'Analyse des Intentions:")
            intent_df = pd.DataFrame([
                ["Intention", intent_result.get("name", "N/A")],
                ["Confiance", result.get("confidence", "N/A")]
            ], columns=["Métrique", "Valeur"])
            st.dataframe(intent_df, use_container_width=True)
async def analyze_single_post_(brand_name, post_content):
    st.markdown("---")
    st.markdown("### 📊 Analyse du Post")
    

    st.code("""
Analysez la,pertinence de ce post:
""")
    
    # Analyse de pertinence
    with st.spinner("Analyse de pertinence en cours..."):

        
        
        analyzer = ExternalContentAnalyzer(google_api_key=GOOGLE_API_KEY, brand_name=brand_name)
        result = {"relevance":{"general_relevance" : ""} ,"confidence":""}

        relevance_result = await analyzer._analyze_relevance("post", post_content,result)
        print(theme_result)
        relevance = relevance_result["relevance"]["general_relevance"]
        
        # Affichage des résultats sous forme de tableau
        st.markdown("#### 📋 Résultats d'Analyse thématique:")
        results_df = pd.DataFrame([
            ["Pertinence", relevance],
            ["Confiance", relevance_result["confidence"]],
        ], columns=["Métrique", "Valeur"])
        st.dataframe(results_df, use_container_width=True)


        if relevance == "true":

            theme_result = await analyzer._analyze_topic("post", post_content)
            print(theme_result)
            theme_id = theme_result["topic"]["id"]
        
            # Affichage des résultats sous forme de tableau
            st.markdown("#### 📋 Résultats d'Analyse thématique:")
            results_df = pd.DataFrame([
            ["Thème Principal", theme_result["topic"]["name"]],
            ["ID Thème", theme_result["topic"]["id"]],
            ["Confiance", theme_result["confidence"]],
        ], columns=["Métrique", "Valeur"])
            st.dataframe(results_df, use_container_width=True)
    
        
            if theme_id and theme_id != "none" and theme_id != "error":
                intent_result = await analyzer._classify_intent("post",
                text=post_content,
                post_text="",
                post_analysis=None,
                result=theme_result
            )

    
            
            # Affichage des résultats d'intention
            st.markdown("#### 📋 Résultats d'Analyse des Intentions:")
            intent_df = pd.DataFrame([
                ["Intention", intent_result.get("name", "N/A")],
                ["Confiance", intent_result.get("confidence", "N/A")]
            ], columns=["Métrique", "Valeur"])
            st.dataframe(intent_df, use_container_width=True)

async def analyze_single_comment(brand_name,comment_text, post_text):
    st.markdown("---")
    st.markdown("### 💬 Analyse du Commentaire")
    
    # Analyse de pertinence
    st.markdown("#### 🎯 Analyse de Pertinence:")
    with st.spinner("Analyse de pertinence..."):

        analyzer = ContentAnalyzer(google_api_key=GOOGLE_API_KEY, brand_name=brand_name)
        result = {"relevance":{"general_relevance" : ""} ,"confidence":""}

        relevance_result = await analyzer._analyze_relevance(comment_text, post_text,result)
        
        relevance = relevance_result["relevance"]["general_relevance"]
        
        # Affichage des résultats sous forme de tableau
        st.markdown("#### 📋 Résultats d'Analyse thématique:")
        results_df = pd.DataFrame([
            ["Pertinence générale", relevance_result["relevance"]["general_relevance"]],
            ["Lié au post", relevance_result["relevance"]["relevance_post"]],
            ["Confiance", relevance_result["confidence"]],
        ], columns=["Métrique", "Valeur"])
        st.dataframe(results_df, use_container_width=True)
        
    st.success(f"Pertinence: {relevance_result['relevance']} (Confiance: {relevance_result['confidence']})")
    

    if relevance =="true" :
        with st.spinner("Analyse thématique..."):
        # Ici vous devriez utiliser ContentAnalyzer pour l'analyse thématique asynchrone
        # analyzer = ContentAnalyzer(google_api_key=GOOGLE_API_KEY, brand_name="orangemaroc")
        # theme_result = await analyzer._classify_theme_comment(comment_text, post_text)
        # Pour l'instant, simulation:
            theme_result = await analyzer._classify_theme(result, "comment", comment_text, post_text, None)
            print(theme_result)
            theme_id = theme_result["theme"]["id"]
        
            # Affichage des résultats sous forme de tableau
            st.markdown("#### 📋 Résultats d'Analyse thématique:")
            results_df = pd.DataFrame([
            ["Thème Principal", theme_result["theme"]["name"]],
            ["ID Thème", theme_result["theme"]["id"]],
            ["Confiance", theme_result["confidence"]],
        ], columns=["Métrique", "Valeur"])
            st.dataframe(results_df, use_container_width=True)
    
        # Classification spécifique selon le thème - HIÉRARCHIQUE
        if theme_id == "offre":
            with st.spinner("Analyse des offres en cours..."):
                result = await analyzer._classify_offre_hierarchical("comment", comment_text, theme_result, post_text, None)
                offre_result = result["category_offre"]
                print(offre_result)
                # Affichage des résultats sous forme de tableau
                st.markdown("#### 📋 Résultats d'Analyse des Offres:")
                results_df = pd.DataFrame([
                ["Catégorie d'offre", offre_result["name"]],
                ["Sous Catégorie d'offre", offre_result["subcategory_offre"]["name"]],
                ["Confiance", result["confidence"]],
                ["Offre", offre_result["subcategory_offre"]["offre"]]
            ], columns=["Métrique", "Valeur"])
                st.dataframe(results_df, use_container_width=True)
    
        elif theme_id == "initiative":
            with st.spinner("Analyse des initiatives en cours..."):
                result = await analyzer._classify_initiative_hierarchical("comment", comment_text, theme_result, post_text, None)
                initiative_result = result["initiative"]
            
                # Affichage des résultats sous forme de tableau
                st.markdown("#### 📋 Résultats d'Analyse des Initiatives:")
                results_df = pd.DataFrame([
                ["Initiative", initiative_result["name"]],
                ["Confiance", result["confidence"]],
                ["Evenement", initiative_result["event"] or ""]
            ], columns=["Métrique", "Valeur"])
                st.dataframe(results_df, use_container_width=True)
    
        elif theme_id == "communication_interaction":
            with st.spinner("Analyse de communication en cours..."):
                result = await analyzer._classify_communication_hierarchical("comment", comment_text, theme_result, post_text, None)
                communication_result = result["communication_interaction_topic"]
            
             # Affichage des résultats sous forme de tableau
                st.markdown("#### 📋 Résultats d'Analyse de Communication:")
                results_df = pd.DataFrame([
                ["Sujet", communication_result["name"]],
                ["Confiance", result["confidence"]],
                ["Subtopic", communication_result["subtopic"]]
            ], columns=["Métrique", "Valeur"])
                st.dataframe(results_df, use_container_width=True)
    
    # Classification des intentions basée sur le thème
        if theme_id != "none":
            with st.spinner("Analyse des intentions en cours..."):
                result = await analyzer._classify_intent("comment", comment_text, theme_result,  theme_id, post_text, None)
                intent_result = result["intent"]
            
                # Affichage des résultats d'intention
                st.markdown("#### 📋 Résultats d'Analyse des Intentions:")
                intent_df = pd.DataFrame([
                ["Intention", intent_result.get("name", "N/A")],
                ["Confiance", result.get("confidence", "N/A")]
            ], columns=["Métrique", "Valeur"])
                st.dataframe(intent_df, use_container_width=True)
       
    
        # Analyse de sentiment
        st.markdown("#### 😊 Analyse de Sentiment:")
        with st.spinner("Analyse de sentiment..."):
        # Ici vous devriez appeler votre méthode asynchrone
        # result = await analyzer._analyze_sentiment(comment_text, result, post_text, post_analysis)
        # Pour l'instant, simulation:
            result = {"sentiment": {"sentiment":"", "emotion":"", "polarity_score": ""},"confidence":""}
                
            result = await analyzer._analyze_sentiment(comment_text,result,post_text)
            print(result)
            sentiment_result = result["sentiment"]
            # Affichage des résultats
            st.markdown("#### 📊 Résultats d'Analyse du Sentiment:")
            comment_results_df = pd.DataFrame([
            
            ["Sentiment", sentiment_result["sentiment"]],
            ["Emotion", sentiment_result["emotion"]],
            ["Score Sentiment", sentiment_result["polarity_score"]],
            ["Confiance ", result["confidence"]]
    ], columns=["Métrique", "Valeur"])
            st.dataframe(comment_results_df, use_container_width=True)
    
             #Affichage spécial du sentiment
            sentiment_label = sentiment_result["sentiment"]
            if sentiment_label == "positif":
                st.success(f"😊 Sentiment: {sentiment_label}")
            elif sentiment_label == "négatif":
                st.error(f"😞 Sentiment: {sentiment_label}")
            else:
                st.info(f"😐 Sentiment: {sentiment_label}")

            
# Page Benchmarking
def show_benchmarking():
    st.title("📊 Benchmarking des LLMs")
    st.markdown("""
    ## Comparaison des Modèles de Langage
    
    ### 🤖 Modèles Testés
    - **GPT-4** : Performance optimale, coût élevé
    - **Claude-3** : Équilibre performance/coût
    - **Gemini Pro** : Spécialisé en multimodal
    
    ### 📈 Métriques de Performance
    - Précision de classification thématique
    - Analyse de sentiment
    - Temps de réponse
    - Coût par requête
    """)

# Fonctions utilitaires pour générer des données d'exemple
def generate_sample_facebook_data(brand, max_posts):
    return {
        "brand": brand,
        "platform": "Facebook",
        "collection_date": datetime.now().isoformat(),
        "posts": [
            {
                "post_id": f"fb_{i}",
                "content": f"Nouveau forfait {brand} avec 50GB pour seulement 199 DH !",
                "date": (datetime.now() - timedelta(days=i)).isoformat(),
                "likes": 150 - i*10,
                "shares": 20 - i*2,
                "comments": [
                    {"text": "Excellente offre !", "author": "user1"},
                    {"text": "Quand sera-t-elle disponible ?", "author": "user2"}
                ]
            } for i in range(min(max_posts, 3))
        ]
    }

def generate_sample_search_data(query, max_posts):
    return {
        "search_query": query,
        "platform": "Facebook",
        "collection_date": datetime.now().isoformat(),
        "posts": [
            {
                "post_id": f"search_{i}",
                "content": f"Problème de réseau avec {query.split()[0]} depuis ce matin...",
                "date": (datetime.now() - timedelta(hours=i*2)).isoformat(),
                "likes": 50 - i*5,
                "comments": [
                    {"text": "Même problème ici", "author": "user_a"},
                    {"text": "Contactez le service client", "author": "user_b"}
                ]
            } for i in range(min(max_posts, 3))
        ]
    }

def generate_sample_instagram_data(brand, max_posts):
    return generate_sample_facebook_data(brand, max_posts) | {"platform": "Instagram"}

def generate_sample_tiktok_data(brand, max_posts):
    return generate_sample_facebook_data(brand, max_posts) | {"platform": "TikTok"}

def generate_sample_analysis_data():
    collected_data = {}
    return {
        "external_posts": [
            {
                "content": "Problème de connexion internet depuis hier avec Orange",
                "date": "2024-01-15",
                "source": "Facebook Search"
            },
            {
                "content": "Nouvelle offre Inwi très intéressante pour les jeunes",
                "date": "2024-01-14", 
                "source": "Facebook Search"
            }
        ],
        "competitor_posts": [
            {
                "brand": "Inwi",
                "content": "Découvrez notre nouveau forfait illimité !",
                "date": "2024-01-16",
                "comments": [
                    {"text": "Prix très attractif !"},
                    {"text": "Disponible quand ?"}
                ]
            },
            {
                "brand": "Maroc Telecom",
                "content": "Amélioration du réseau 5G dans toutes les villes",
                "date": "2024-01-15",
                "comments": [
                    {"text": "Enfin ! J'attendais ça"},
                    {"text": "Ça marche vraiment bien maintenant"}
                ]
            }
        ],
        "orange_posts": [
            {
                "content": "Orange vous souhaite une excellente année 2024 !",
                "date": "2024-01-01",
                "comments": [
                    {"text": "Merci Orange ! Bonne année à vous aussi"},
                    {"text": "En espérant de meilleures offres cette année"}
                ]
            }
        ]
    }

# Application principale
def main():
    # Sidebar navigation
    current_page = sidebar_navigation()
    
    # Affichage de la page sélectionnée
    if current_page == "introduction":
        show_introduction()
    elif current_page == "architecture":
        show_architecture()
    elif current_page == "tests":
        show_tests()
    elif current_page == "benchmarking":
        show_benchmarking()

if __name__ == "__main__":
    main()