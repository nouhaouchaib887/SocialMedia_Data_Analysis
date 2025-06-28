import time
from datetime import datetime, timedelta
from api_client import ApifyClient
from data_transformer import TikTokDataTransformer


class TikTokScraper:
    """
    Classe principale pour collecter et traiter les données TikTok.
    Orchestre l'utilisation de l'API client et du transformer de données.
    """

    def __init__(self, apify_token, brand_name, profile_name, max_posts=2, max_comments_per_post=3, days_back=15):
        """
        Initialize the TikTok scraper.

        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name associated with the TikTok profile
            profile_name (str): TikTok profile handle (e.g., '@tiktok')
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            days_back (int): Number of days to go back for post collection
        """
        if not apify_token:
            raise ValueError("Apify API token must be provided.")
        if not brand_name:
            raise ValueError("Brand name must be provided.")
        if not profile_name:
            raise ValueError("Profile name must be provided.")

        self.brand_name = brand_name
        self.profile_name = profile_name.strip().lstrip('@')
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.days_back = days_back

        # Initialize client and transformer
        self.api_client = ApifyClient(apify_token)
        self.data_transformer = TikTokDataTransformer(platform="tiktok", brand_name=brand_name)

        # Actor IDs
        self.posts_actor_id = 'clockworks~tiktok-profile-scraper'
        self.comments_actor_id = 'clockworks~tiktok-comments-scraper'

        # Calculate date range (less relevant for the TikTok profile actor's filtering)
        self.from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")

    def collect_posts(self):
        """
        Collect posts (videos) from the TikTok profile using the profile scraper actor.

        Returns:
            list: List of transformed posts, or empty list if collection fails or no posts found/transformed.
        """
        print(f"\n📱 Début de la collecte des posts pour le profil TikTok '{self.profile_name}'...")
        print(f"📊 Paramètres: Max {self.max_posts} posts, {self.days_back} jours (estimation, acteur peut ignorer la date)")

        # Input for the TikTok profile scraper actor
        actor_input = {
            "profiles": [self.profile_name],
            "resultsPerPage": self.max_posts,
            "shouldDownloadAvatars": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSlideshowImages": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadVideos": False,
            "profileScrapeSections": ["videos"],
            "profileSorting": "latest"
        }

        run_data = self.api_client.run_actor(self.posts_actor_id, actor_input)

        if run_data is None:
            print("❌ L'exécution de l'acteur posts a échoué. Impossible de continuer la collecte des posts.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Erreur interne: datasetId manquant après un run d'acteur posts qui a rapporté succès. Impossible de récupérer les données.")
            return []

        posts_data = self.api_client.get_dataset_items(dataset_id)

        if not posts_data:
            print(f"⚠️ Aucun item brut récupéré du dataset '{dataset_id}' de posts.")
            return []

        # Transform collected raw items using the data transformer
        posts = self.data_transformer.transform_posts_batch(posts_data)
        return posts

    def collect_comments_for_post(self, post_url):
        """
        Collect comments for a specific post using the comments scraper actor.

        Args:
            post_url (str): URL of the TikTok post (video)

        Returns:
            list: List of transformed comments, or empty list if collection fails or no comments found/transformed.
        """
        print(f"\n💬 Début de la collecte des commentaires pour la vidéo : {post_url}")
        print(f"📊 Paramètres: Max {self.max_comments_per_post} commentaires par vidéo.")

        # Basic URL validation
        if not post_url or not post_url.startswith("http"):
            print(f"❌ URL de post invalide fournie pour la collecte de commentaires : '{post_url}'. Collection ignorée.")
            return []

        # Input for the TikTok comments scraper actor
        actor_input = {
            "postURLs": [post_url],
            "commentsPerPost": self.max_comments_per_post,
            "maxRepliesPerComment": 0,
            "resultsPerPage": 100
        }

        run_data = self.api_client.run_actor(self.comments_actor_id, actor_input)

        if run_data is None:
            print(f"❌ L'exécution de l'acteur commentaires a échoué pour la vidéo {post_url}.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Erreur interne: datasetId manquant après un run d'acteur commentaires qui a rapporté succès pour {post_url}. Impossible de récupérer les données.")
            return []

        comments_data = self.api_client.get_dataset_items(dataset_id)

        if not comments_data:
            print(f"⚠️ Aucun item brut récupéré du dataset '{dataset_id}' de commentaires pour {post_url}.")
            return []

        # Transform collected raw items using the data transformer
        comments = self.data_transformer.transform_comments_batch(comments_data)
        return comments

    def collect_all_data(self):
        """
        Collect all posts and their comments for the specified profile.

        Returns:
            list: List of posts with their comments, or empty list if initial post collection fails.
        """
        print(f"\n--- Début du processus de collecte complet pour le profil TikTok '{self.profile_name}' ---")

        # Collect posts first
        posts = self.collect_posts()

        if not posts:
            print("❌ Aucun post collecté avec succès. Arrêt du processus de collecte complet.")
            return []

        print(f"\nTraitement des commentaires pour les {len(posts)} posts collectés...")
        processed_posts = []

        # Collect comments for each post
        for i, post in enumerate(posts, 1):
            post_id_safe = post.get('post_id', 'N/A')
            post_url = post.get("permalink")

            if post_url:
                print(f"\n--- Traitement Post {i}/{len(posts)} (ID: {post_id_safe}) ---")
                comments = self.collect_comments_for_post(post_url)
                post["comments"] = comments
                processed_posts.append(post)
            else:
                print(f"\n--- Traitement Post {i}/{len(posts)} (ID: {post_id_safe}) ---")
                print(f"⚠️ Le post {post_id_safe} n'a pas de 'permalink' valide. La collecte des commentaires est ignorée pour ce post.")
                post["comments"] = []
                processed_posts.append(post)

            # Add a delay between comment collection calls for different posts
            if i < len(posts):
                print("⏳ Attente de 10 secondes avant de collecter les commentaires du prochain post...")
                time.sleep(10)

        print("\n--- Fin du processus de collecte complet ---")
        print(f"🏁 Total posts traités : {len(processed_posts)} (sur {len(posts)} initialement collectés)")
        total_comments = sum(len(p.get('comments', [])) for p in processed_posts)
        print(f"🏁 Total commentaires collectés : {total_comments}")

        return processed_posts


