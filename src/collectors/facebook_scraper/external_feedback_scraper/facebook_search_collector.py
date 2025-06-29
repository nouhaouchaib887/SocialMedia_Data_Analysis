import time
from api_client import ApifyAPIClient, FacebookDataAPI
from data_transformers import FacebookDataTransformer


class FacebookCollector:
    """
    Collecteur principal pour les données Facebook.
    Orchestre la récupération des données et leur transformation.
    """
    
    def __init__(self, apify_token, brand_name, search_query=None, max_posts=2, max_comments_per_post=3, post_time_range="30d"):
        """
        Initialize the FacebookCollector.
        
        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for identification
            search_query (str): Search query for posts (e.g., '#orangemaroc', 'orangemaroc')
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            post_time_range (str): Time range for posts (e.g., '30d', '7d', '90d')
        """
        if not apify_token:
            raise ValueError("Apify token cannot be None or empty.")
        if not brand_name:
            raise ValueError("Brand name cannot be None or empty.")
        
        self.brand_name = brand_name
        self.search_query = search_query or f"#{brand_name}"
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.post_time_range = post_time_range
        
        # Initialize API client and data API
        self.api_client = ApifyAPIClient(apify_token)
        self.facebook_api = FacebookDataAPI(self.api_client)
        
        # Initialize transformer
        self.transformer = FacebookDataTransformer(brand_name)
    
    def collect_posts(self):
        """
        Collect and transform Facebook posts.
        
        Returns:
            list: List of transformed posts or empty list on failure
        """
        print(f"\n--- Début de la collecte des posts ---")
        print(f"📱 Collecte des posts avec la requête: '{self.search_query}'")
        print(f"🔢 Nombre max de posts: {self.max_posts}")
        
        try:
            # Get raw data from API
            raw_posts = self.facebook_api.get_posts_data(
                search_query=self.search_query,
                max_posts=self.max_posts
            )
            
            if not raw_posts:
                print(f"ℹ️ Aucun post trouvé pour la requête '{self.search_query}'")
                return []
            
            # Transform the data
            transformed_posts = self.transformer.transform_posts_batch(raw_posts)
            return transformed_posts
            
        except Exception as e:
            print(f"❌ Une erreur inattendue s'est produite lors de la collecte des posts : {type(e).__name__} - {e}")
            return []
    
    def collect_comments_for_post(self, post_url):
        """
        Collect and transform comments for a specific post.
        
        Args:
            post_url (str): URL of the Facebook post
            
        Returns:
            list: List of transformed comments or empty list on failure
        """
        if not post_url or not isinstance(post_url, str):
            print("⚠️ URL du post invalide ou manquante pour la collecte des commentaires.")
            return []
        
        print(f"\n--- Début de la collecte des commentaires ---")
        print(f"💬 Collecte des commentaires pour : {post_url}")
        print(f"🔢 Nombre max de commentaires par post: {self.max_comments_per_post}")