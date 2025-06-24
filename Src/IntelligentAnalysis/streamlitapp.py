import streamlit as st
import json
import asyncio
from datetime import datetime
import time
import os
from pathlib import Path

# Import de votre vraie classe ContentAnalyzer
from ContentAnalyzer import ContentAnalyzer  # Assurez-vous que le fichier est accessible

# Configuration de la page
st.set_page_config(
    page_title="Test Analyseur Posts Télécoms",
    page_icon="📱",
    layout="wide"
)

# Configuration de la clé API
@st.cache_data
def get_api_key():
    """Récupération de la clé API Google Gemini"""
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API
    return "AIzaSyCzL7OwRGWTW2VVgeGY1NopDnnu7zBP-sw"

# Chargement des données depuis les fichiers JSON
@st.cache_data
def load_posts_from_json():
    """Chargement des posts depuis les fichiers JSON du dossier data/"""
    data_folder = Path("/home/nouha/Desktop/SocialMediaTracker/data/official_pages/facebook_data_backup")
    all_posts = []
    
    if not data_folder.exists():
        st.error(f"❌ Le dossier 'data' n'existe pas. Créez-le et ajoutez vos fichiers JSON.")
        return []
    
    # Recherche de tous les fichiers JSON dans le dossier data
    json_files = list(data_folder.glob("*.json"))
    
    if not json_files:
        st.error(f"❌ Aucun fichier JSON trouvé dans le dossier 'data/'.")
        return []
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extraire le nom de la marque depuis le nom du fichier
            brand_name = json_file.stem  # nom du fichier sans l'extension
            
            # Si les données sont une liste de posts
            if isinstance(data, list):
                for post in data:
                    post['source_file'] = str(json_file)
                    all_posts.append(post)
            
            # Si les données sont un dictionnaire avec une clé 'posts' ou similaire
            elif isinstance(data, dict):
                posts_data = data.get('posts', data.get('data', [data]))
                if not isinstance(posts_data, list):
                    posts_data = [posts_data]
                
                for post in posts_data:

                    post['source_file'] = str(json_file)
                    all_posts.append(post)
            
        except json.JSONDecodeError as e:
            st.warning(f"⚠️ Erreur de format JSON dans {json_file}: {e}")
        except Exception as e:
            st.warning(f"⚠️ Erreur lors du chargement de {json_file}: {e}")
    
    return all_posts

@st.cache_data
def get_available_brands():
    """Récupération des marques disponibles depuis les fichiers JSON"""
    
    
    return ["orange", "inwi", "maroc_telecom"]

@st.cache_resource
def initialize_analyzer(brand_name):
    """Initialisation de l'analyseur réel avec cache"""
    try:
        api_key = get_api_key()
        if not api_key or api_key == "VOTRE_CLE_API_GOOGLE_ICI":
            st.error("⚠️ Veuillez configurer votre clé API Google Gemini")
            return None
        
        analyzer = ContentAnalyzer(
            google_api_key=api_key,
            brand_name=brand_name
        )
        return analyzer
    except Exception as e:
        st.error(f"Erreur lors de l'initialisation de l'analyseur: {e}")
        return None

def display_file_info():
    """Affichage des informations sur les fichiers chargés"""
    data_folder = Path("../data/official_pages/facebook_data_backup")
    
    st.subheader("📁 Informations sur les Fichiers de Données")
    
    if not data_folder.exists():
        st.error("❌ Le dossier 'data' n'existe pas.")
        st.info("💡 Créez un dossier 'data' et ajoutez vos fichiers JSON (ex: inwi.json, orange.json, maroc_telecom.json)")
        return
    
    json_files = list(data_folder.glob("*.json"))
    
    if not json_files:
        st.warning("⚠️ Aucun fichier JSON trouvé dans le dossier 'data'.")
        return
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Compter les posts
            post_count = 0
            if isinstance(data, list):
                post_count = len(data)
            elif isinstance(data, dict):
                posts_data = data.get('posts', data.get('data', []))
                if isinstance(posts_data, list):
                    post_count = len(posts_data)
                else:
                    post_count = 1
            
            file_size = json_file.stat().st_size
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(f"📄 {json_file.name}", f"{post_count} posts")
            with col2:
                st.metric("Taille", f"{file_size} bytes")
            with col3:
                st.metric("Marque", json_file.stem)
                
        except Exception as e:
            st.error(f"❌ Erreur avec {json_file.name}: {e}")

def display_analysis_results(analysis_type: str, content: str, results: dict):
    """Affichage des résultats d'analyse"""
    
    st.subheader(f"📊 Résultats d'analyse - {analysis_type}")
    
    # Contenu analysé
    with st.expander("📝 Contenu analysé"):
        st.text_area("", content, height=100, disabled=True)
    
    # Vérification si les résultats sont valides
    if not results or 'theme' not in results:
        st.error("❌ Erreur dans l'analyse - Résultats invalides")
        return
    
    # Résultats principaux
    col1, col2 = st.columns(2)
    
    with col1:
        # Thème
        st.markdown("**🎯 Thème Principal**")
        if results.get("theme"):
            theme = results['theme']
            st.success(f"**{theme.get('name', 'N/A')}** (ID: {theme.get('id', 'N/A')})")
        
        # Intention
        st.markdown("**💭 Intention**")
        if results.get("intent"):
            st.info(results['intent'].get('name', 'N/A'))
        else:
            st.warning("Non déterminée")
        
        # Topic de communication
        st.markdown("**💬 Topic de Communication**")
        if results.get("communication_interaction_topic"):
            topic = results['communication_interaction_topic']
            topic_name = topic.get('name', 'N/A')
            subtopic = topic.get('subtopic', '')
            if subtopic:
                st.info(f"**{topic_name}** - {subtopic}")
            else:
                st.info(f"**{topic_name}**")
        else:
            st.warning("Aucun topic identifié")
    
    with col2:
        # Sentiment (pour les commentaires)
        st.markdown("**😊 Sentiment**")
        if results.get("sentiment"):
            sent = results['sentiment']
            sentiment_colors = {
                'positif': '🟢',
                'neutre': '🟡',
                'negatif': '🔴'
            }
            sentiment_val = sent.get('sentiment', 'unknown')
            st.markdown(f"{sentiment_colors.get(sentiment_val, '⚪')} **{sentiment_val.title()}**")
            st.write(f"Émotion: {sent.get('emotion', 'N/A')}")
            st.write(f"Score: {sent.get('polarity_score', 'N/A')}")
        else:
            st.info("Sentiment non analysé (Post)")
        
        # Pertinence (pour les commentaires)
        st.markdown("**📊 Pertinence**")
        if results.get("relevance"):
            rel = results['relevance']
            st.write(f"Pertinence au post: {'✅' if rel.get('relevance_post') else '❌'}")
            st.write(f"Pertinence générale: {'✅' if rel.get('general_relevance') else '❌'}")
        else:
            st.info("Pertinence non analysée (Post)")
    
    # Informations supplémentaires
    col3, col4 = st.columns(2)
    
    with col3:
        # Initiative
        st.markdown("**🎪 Initiative**")
        if results.get("initiative"):
            init = results['initiative']
            init_name = init.get('name', 'N/A')
            event = init.get('evenement', init.get('event', ''))
            if event and event != init_name:
                st.success(f"**{init_name}** ({event})")
            else:
                st.success(f"**{init_name}**")
        else:
            st.info("Aucune initiative détectée")
    
    with col4:
        # Catégorie d'offre
        st.markdown("**🏷️ Catégorie d'Offre**")
        if results.get("category_offre"):
            cat = results['category_offre']
            st.success(f"**{cat.get('name', 'N/A')}**")
            if cat.get("subcategory_offre"):
                sub = cat['subcategory_offre']
                subcategory_name = sub.get('name', 'N/A')
                offre = sub.get('offre', '')
                if offre:
                    st.write(f"↳ {subcategory_name} - {offre}")
                else:
                    st.write(f"↳ {subcategory_name}")
        else:
            st.info("Aucune catégorie identifiée")
    
    # Confiance
    if results.get("confidence"):
        st.markdown("**🎯 Score de Confiance**")
        confidence = float(results['confidence'])
        st.progress(confidence)
        st.write(f"Confiance: {confidence:.2%}")
    
    # JSON complet dans un expander
    with st.expander("📋 Résultat JSON Complet"):
        st.json(results)

def main():
    st.title("📱 Test Analyseur Posts Télécoms Maroc")
    st.markdown("### 🚀 Version avec données JSON locales + analyseur IA réel")
    st.markdown("---")
    
    # Affichage des informations sur les fichiers
    display_file_info()
    st.markdown("---")
    
    # Chargement des données depuis les fichiers JSON
    all_posts = load_posts_from_json()
    
    if not all_posts:
        st.stop()
    
    st.success(f"✅ {len(all_posts)} posts chargés depuis les fichiers JSON")
    
    # Sélection du post
    st.subheader("📋 Sélection du Post à Analyser")
    
    # Filtre par marque
    available_brands = get_available_brands()
    if available_brands:
        selected_brand = st.selectbox(
            "🏷️ Filtrer par marque (optionnel)",
            ["Toutes les marques"] + available_brands
        )
        
        if selected_brand != "Toutes les marques":
            filtered_posts = [post for post in all_posts if post.get('brand_name') == selected_brand]
            print(post.get("brand_name"))
        else:
            filtered_posts = all_posts
    else:
        filtered_posts = all_posts
    
    if not filtered_posts:
        st.warning("Aucun post trouvé avec les critères sélectionnés.")
        st.stop()
    
    # Créer les options pour le selectbox
    post_options = []
    for i, post in enumerate(filtered_posts):
        
        brand = post.get('brand_name', 'Unknown')
        print(brand)
        platform = post.get('platform', 'Unknown')
        message = post.get('message', post.get('text', post.get('content', '')))
        print(message)
        message_preview = message[:50] + "..." if len(message) > 50 else message
        post_options.append(f"Post {i+1} - {brand} ({platform}): {message_preview}")
    
    selected_post_idx = st.selectbox(
        "Choisir un post",
        range(len(filtered_posts)),
        format_func=lambda x: post_options[x]
    )
    
    selected_post = filtered_posts[selected_post_idx]
    
    # Initialisation de l'analyseur pour la marque sélectionnée
    brand_name = selected_post.get('brand_name', 'unknown')
    print(brand_name)
    analyzer = initialize_analyzer(brand_name)
    
    if not analyzer:
        st.error("❌ Impossible d'initialiser l'analyseur. Vérifiez la configuration.")
        return
    
    # Affichage des détails du post
    st.subheader("📝 Détails du Post Sélectionné")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Marque", brand_name)
    with col2:
        st.metric("Plateforme", selected_post.get('platform', 'N/A'))
    with col3:
        st.metric("Likes", selected_post.get('like_count', selected_post.get('likes', 0)))
    with col4:
        st.metric("Commentaires", selected_post.get('comments_count', len(selected_post.get('comments', []))))
    
    # Affichage du message
    post_message = selected_post.get('message', selected_post.get('text', selected_post.get('content', '')))
    st.text_area("Message du post", post_message, height=120, disabled=True)
    
    # Affichage du fichier source
    st.info(f"📁 Source: {selected_post.get('source_file', 'N/A')}")
    
    # Vérification et affichage des commentaires
    comments = selected_post.get('comments', [])
    
    if not comments:
        st.warning("⚠️ Aucun commentaire trouvé pour ce post.")
        comments_to_analyze = []
    else:
        st.subheader(f"💬 Commentaires disponibles ({len(comments)})")
        
        # Sélection du nombre de commentaires à analyser
        max_comments = min(len(comments), 5)  # Maximum 5 commentaires
        num_comments = st.slider("Nombre de commentaires à analyser", 1, max_comments, min(2, max_comments))
        
        comments_to_analyze = comments[:num_comments]
        
        for i, comment in enumerate(comments_to_analyze):
            with st.expander(f"💬 Commentaire {i+1} - {comment.get('user_name', comment.get('author', 'Anonyme'))}"):
                comment_text = comment.get('message', comment.get('text', comment.get('content', '')))
                st.write(f"**Message:** {comment_text}")
                st.write(f"**Likes:** {comment.get('like_count', comment.get('likes', 0))} | **Date:** {comment.get('created_time', comment.get('date', 'N/A'))}")
    
    # Bouton d'analyse
    st.markdown("---")
    
    if st.button("🚀 Analyser le Post et les Commentaires", type="primary", use_container_width=True):
        
        try:
            # Analyser le post principal
            with st.spinner("🔍 Analyse du post principal..."):
                post_analysis = analyzer.analyze_content_sync("post", post_message)
            
            st.success("✅ Analyse du post terminée!")
            display_analysis_results("Post Principal", post_message, post_analysis)
            
            st.markdown("---")
            
            # Analyser les commentaires si disponibles
            if comments_to_analyze:
                for i, comment in enumerate(comments_to_analyze):
                    with st.spinner(f"🔍 Analyse du commentaire {i+1}..."):
                        comment_text = comment.get('message', comment.get('text', comment.get('content', '')))
                        comment_analysis = analyzer.analyze_content_sync(
                            "comment", 
                            comment_text, 
                            post_message,
                            post_analysis
                        )
                    
                    st.success(f"✅ Analyse du commentaire {i+1} terminée!")
                    display_analysis_results(f"Commentaire {i+1}", comment_text, comment_analysis)
                    
                    if i < len(comments_to_analyze) - 1:
                        st.markdown("---")
            
            # Résumé final
            st.markdown("---")
            st.subheader("📊 Résumé de l'Analyse")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Éléments analysés", f"{1 + len(comments_to_analyze)}")
            
            with col2:
                if post_analysis.get('sentiment'):
                    st.metric("Sentiment post", post_analysis['sentiment'].get('sentiment', 'N/A').title())
                else:
                    st.metric("Thème principal", post_analysis.get('theme', {}).get('name', 'N/A'))
            
            with col3:
                st.metric("Marque analysée", brand_name)
            
            st.balloons()
            
        except Exception as e:
            st.error(f"❌ Erreur lors de l'analyse: {str(e)}")
            st.exception(e)

if __name__ == "__main__":
    main()