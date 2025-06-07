import requests
import time
import re
from datetime import datetime, timedelta
import json
import os
from urllib.parse import urlparse, parse_qs


class FacebookAPifyCollector:
    """
    A class to collect Facebook posts and their comments from a specific brand page.
    """
    
    def __init__(self, apify_token, brand_name,page_name, max_posts=2, max_comments_per_post=3, days_back=15):
        """
        Initialize the FacebookCollector.
        
        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for Facebook page (e.g., 'inwi.ma', 'orangemaroc')
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            days_back (int): Number of days to go back for post collection
        """
        self.apify_token = apify_token
        self.brand_name = brand_name
        self.page_name = page_name
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.days_back = days_back
        self.facebook_url = f"https://www.facebook.com/{page_name}"
        self.posts_actor_id = 'apify~facebook-posts-scraper'
        self.comments_actor_id = 'apify~facebook-comments-scraper'
        self.platform = "facebook"
        
        # Calculate date range
        self.from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")
        
    def _run_apify_actor(self, actor_id, actor_input):
        """
        Run an Apify actor and wait for completion.
        
        Args:
            actor_id (str): Apify actor ID
            actor_input (dict): Input parameters for the actor
            
        Returns:
            dict: Actor run data or None if failed
        """
        print(f"🚀 Lancement de l'acteur {actor_id}...")
        
        # Start the actor
        start_response = requests.post(
            f'https://api.apify.com/v2/acts/{actor_id}/runs?token={self.apify_token}',
            json=actor_input
        )
        
        if start_response.status_code != 201:
            print(f"❌ Erreur au lancement : {start_response.status_code} - {start_response.text}")
            return None
            
        run_data = start_response.json()
        run_id = run_data.get('data', {}).get('id')
        
        if not run_id:
            print("❌ Impossible de récupérer l'ID du run.")
            return None
            
        # Wait for completion
        print("⏳ Exécution en cours...")
        while True:
            run_status_response = requests.get(
                f'https://api.apify.com/v2/actor-runs/{run_id}?token={self.apify_token}'
            )
            run_status_data = run_status_response.json().get('data', {})
            status = run_status_data.get('status')
            
            print(f"🔄 Statut : {status}")
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                break
                
            time.sleep(5)
            
        if status != 'SUCCEEDED':
            print(f"❌ Échec de l'exécution. Statut final : {status}")
            return None
            
        return run_status_data
        
    def _get_dataset_items(self, dataset_id):
        """
        Retrieve items from an Apify dataset.
        
        Args:
            dataset_id (str): Dataset ID
            
        Returns:
            list: Dataset items or empty list if failed
        """
        dataset_response = requests.get(
            f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
        )
        
        if dataset_response.status_code != 200:
            print(f"❌ Erreur lors de la récupération des données : {dataset_response.status_code}")
            return []
            
        return dataset_response.json()
        
    def _transform_post(self, post):
        """
        Transform raw post data to standardized format.
        
        Args:
            post (dict): Raw post data from Apify
            
        Returns:
            dict: Transformed post data
        """
        media = post.get("media")
        media_type = ""
        media_url = ""
        media_thumbnail = ""
        
        if isinstance(media, dict):
            media_type = media.get("__typename", "")
            media_url = media.get("url", "")
            media_thumbnail = media.get("thumbnail", "")
        elif isinstance(media, list) and media:
            first_media = media[0]
            if isinstance(first_media, dict):
                media_type = first_media.get("__typename", "")
                media_url = first_media.get("url", "")
                media_thumbnail = first_media.get("thumbnail", "")
                
        return {
            "post_id": post.get("postId"),
            "source_id": post.get("user", {}).get("id"),
            "created_time": datetime.utcfromtimestamp(post["timestamp"]).isoformat() if post.get("timestamp") else None,
            "permalink": f"https://www.facebook.com/{self.page_name}/posts/{post.get('postId')}",
            "page_id": post.get("user", {}).get("id"),
            "page_name": self.page_name,
            "message": post.get("text", ""),
            "media_type": media_type,
            "media_url": media_url,
            "thumbnail_url": media_thumbnail,
            "can_share": True,
            "shares": post.get("shares", 0),
            "can_comment": True,
            "comments_count": post.get("comments", 0),
            "can_like": True,
            "like_count": post.get("likes", 0),
            "hashtags": re.findall(r"#(\w+)", post.get("text", "")),
            "mentions": re.findall(r"@(\w+)", post.get("text", "")),
            "platform": self.platform,
            "brand_name": self.brand_name,
            "comments": []  # Will be populated later
        }
        
    def _transform_comment(self, comment):
        """
        Transform raw comment data to standardized format.
        
        Args:
            comment (dict): Raw comment data from Apify
            
        Returns:
            dict: Transformed comment data
        """
        return {
            "comment_id":  parse_qs(urlparse(comment.get("commentUrl")).query).get('comment_id', [None])[0]
,
            "comment_url":comment.get("commentUrl"),
            "user_id": comment.get("profileId"),
            "user_name": comment.get("profileName"),
            "user_url": comment.get("profileUrl"),
            "created_time": comment.get("date"),
            "message": comment.get("text", ""),
            "like_count": comment.get("likesCount", 0),
            "reply_count": comment.get("commentsCount", 0),
            "hashtags": re.findall(r"#(\w+)", comment.get("text", "")),
            "mentions": re.findall(r"@(\w+)", comment.get("text", "")),
            "platform": self.platform,
            "brand_name": self.brand_name
        }
        
    def collect_posts(self):
        """
        Collect posts from the Facebook page.
        
        Returns:
            list: List of transformed posts
        """
        print(f"📱 Collecte des posts de {self.facebook_url} depuis {self.from_date}")
        
        actor_input = {
            "startUrls": [{"url": self.facebook_url}],
            "resultsLimit": self.max_posts,
            "mode": "page",
            "commentsMode": "none",
            "reactions": True,
            #"onlyPostsNewerThan": self.from_date,
            "proxyConfig": {"useApifyProxy": True},
            "captionText": False
        }
        
        run_data = self._run_apify_actor(self.posts_actor_id, actor_input)
        if not run_data:
            return []
            
        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts.")
            return []
            
        posts_data = self._get_dataset_items(dataset_id)
        posts = [self._transform_post(post) for post in posts_data]
        
        print(f"✅ {len(posts)} posts collectés avec succès.")
        return posts
        
    def collect_comments_for_post(self, post_url):
        """
        Collect comments for a specific post.
        
        Args:
            post_url (str): URL of the Facebook post
            
        Returns:
            list: List of transformed comments
        """
        print(f"💬 Collecte des commentaires pour : {post_url}")
        
        actor_input = {
            "startUrls": [{"url": post_url}],
            "resultsLimit": self.max_comments_per_post,
            "reactions": True,
            "proxyConfig": {"useApifyProxy": True}
        }
        
        run_data = self._run_apify_actor(self.comments_actor_id, actor_input)
        if not run_data:
            return []
            
        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des commentaires.")
            return []
            
        comments_data = self._get_dataset_items(dataset_id)
        comments = [self._transform_comment(comment) for comment in comments_data]
        
        print(f"✅ {len(comments)} commentaires collectés.")
        return comments
        
    def collect_all_data(self):
        """
        Collect all posts and their comments.
        
        Returns:
            list: List of posts with their comments
        """
        print(f"🔍 Début de la collecte pour {self.brand_name}")
        print(f"📊 Paramètres: {self.max_posts} posts, {self.max_comments_per_post} commentaires/post, {self.days_back} jours")
        
        # Collect posts
        posts = self.collect_posts()
        
        if not posts:
            print("❌ Aucun post collecté.")
            return []
            
        # Collect comments for each post
        for i, post in enumerate(posts, 1):
            print(f"\n--- Post {i}/{len(posts)} ---")
            if post.get("permalink"):
                post_url = post["permalink"]
                comments = self.collect_comments_for_post(post_url)
                print(post_url)
                post["comments"] = comments
                
                # Add delay between requests to avoid rate limiting
                if i < len(posts):
                    print("⏳ Attente de 10 secondes...")
                    time.sleep(10)
            else:
                print("⚠️ Pas de permalink disponible pour ce post.")
                
        return posts
        
    def save_to_file(self, data, filename=None):
        """
        Save collected data to JSON file.
        
        Args:
            data (list): Data to save
            filename (str, optional): Output filename
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.brand_name}_facebook_data_{timestamp}.json"
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        print(f"💾 Données sauvegardées dans '{filename}'")
        return filename
        
    def run_collection(self, output_file=None):
        """
        Run the complete collection process.
        
        Args:
            output_file (str, optional): Output filename
            
        Returns:
            tuple: (collected_data, output_filename)
        """
        try:
            data = self.collect_all_data()
            
            if data:
                filename = self.save_to_file(data, output_file)
                
                # Print summary
                total_comments = sum(len(post.get("comments", [])) for post in data)
                print(f"\n🎉 Collecte terminée avec succès !")
                print(f"📊 Résumé: {len(data)} posts, {total_comments} commentaires au total")
                
                return data, filename
            else:
                print("❌ Aucune donnée collectée.")
                return [], None
                
        except Exception as e:
            print(f"❌ Erreur durant la collecte : {str(e)}")
            return [], None

