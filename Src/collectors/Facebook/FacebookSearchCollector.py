import requests
import time
import re
from datetime import datetime, timezone
import json
import os
from urllib.parse import urlparse, parse_qs
class FacebookSearchCollector:
    """
    A class to collect Facebook posts and their comments using EasyAPI actors.
    """
    
    def __init__(self, apify_token, brand_name, search_query=None, page_name=None, max_posts=2, max_comments_per_post=3, post_time_range="30d"):
        """
        Initialize the FacebookEasyAPICollector.
        
        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for identification
            search_query (str): Search query for posts (e.g., '#orangemaroc', 'orangemaroc')
            page_name (str): Page name for direct page scraping (alternative to search)
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            post_time_range (str): Time range for posts (e.g., '30d', '7d', '90d')
        """
        self.apify_token = apify_token
        self.brand_name = brand_name
        self.search_query = search_query or f"#{brand_name}"
        self.page_name = page_name
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.post_time_range = post_time_range
        
        # Actor IDs
        self.posts_actor_id = 'easyapi~facebook-posts-search-scraper'
        self.comments_actor_id = 'apify~facebook-comments-scraper'  # Using the standard Apify actor
        self.platform = "facebook"
        
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
            f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.apify_token}&clean=true&format=json'
        )
        
        if dataset_response.status_code != 200:
            print(f"❌ Erreur lors de la récupération des données : {dataset_response.status_code}")
            return []
            
        return dataset_response.json()
    
    def _parse_human_readable_number(self, value):
        """Converts numbers like '2.4K', '1M', '1,234' to an integer."""
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip().lower()
            value = value.replace(',', '')
            if 'k' in value:
                return int(float(value.replace('k', '')) * 1000)
            if 'm' in value:
                return int(float(value.replace('m', '')) * 1000000)
            try:
                return int(value)
            except ValueError:
                return 0
        return 0

    def _get_media_type_from_link(self, link_url, thumb_url):
        """
        Determines media type based on the 'link' URL.
        """
        if not link_url or not isinstance(link_url, str):
            if thumb_url:
                return "IMAGE"
            return "STATUS"

        parsed_url = urlparse(link_url)
        if "facebook.com" not in parsed_url.netloc:
            if thumb_url:
                return "LINK_WITH_IMAGE"
            return "LINK"

        path_segments = [segment for segment in parsed_url.path.split('/') if segment]

        if not path_segments:
            if thumb_url: 
                return "IMAGE"
            return "STATUS"

        first_segment = path_segments[0].lower()

        if first_segment == "photo" or first_segment == "photos" or "set=a." in parsed_url.query:
            return "PHOTO"
        if first_segment == "video" or first_segment == "videos" or "watch" == first_segment:
            return "VIDEO"
        if first_segment == "story" or first_segment == "stories":
            return "STORY"
        if first_segment == "events":
            return "EVENT"
        
        if "posts" in path_segments and thumb_url:
             return "IMAGE"
        if "notes" in path_segments:
            return "NOTE"

        if thumb_url:
            return "IMAGE"

        return "STATUS"

    def _parse_fb_timestamp(self, ts_input_raw, time_str_raw):
        """
        Parses Facebook timestamp (prefers Unix timestamp, falls back to time string).
        """
        if ts_input_raw is not None:
            try:
                return datetime.fromtimestamp(float(ts_input_raw), timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass

        if time_str_raw is not None and isinstance(time_str_raw, str):
            try:
                dt_obj = datetime.strptime(time_str_raw, "%Y-%m-%d %H:%M:%S")
                return dt_obj.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                print(f"⚠️ Warning: Could not parse timestamp string: {time_str_raw}")
                return time_str_raw
        return None
        
    def _transform_post(self, post):
        """
        Transform raw post data to standardized format.
        
        Args:
            post (dict): Raw post data from EasyAPI
            
        Returns:
            dict: Transformed post data
        """
        text_content = post.get("text", "")
        link_url = post.get("link")
        thumb_url = post.get("thumb")
        
        created_time_iso = self._parse_fb_timestamp(post.get("timestamp"), post.get("time"))
                
        return {
            "post_id": post.get("postId"),
            "source_id": post.get("pageId"),
            "created_time": created_time_iso,
            "updated_time": None,
            "permalink": post.get("url"),
            "page_id": post.get("pageId"),
            "page_name": post.get("pageName"),
            "message": text_content,
            "media_type": self._get_media_type_from_link(link_url, thumb_url),
            "media_url": link_url,
            "thumbnail_url": thumb_url,
            "can_share": True,
            "shares": self._parse_human_readable_number(post.get("shares", 0)),
            "can_comment": True,
            "comments_count": self._parse_human_readable_number(post.get("comments", 0)),
            "can_like": True,
            "like_count": self._parse_human_readable_number(post.get("likes", 0)),
            "hashtags": re.findall(r"#(\w+)", text_content) if text_content else [],
            "mentions": re.findall(r"@(\w+)", text_content) if text_content else [],
            "caption": text_content,
            "description": "",
            "platform": self.platform,
            "brand_name": self.brand_name,
            "comments": []  # Will be populated later
        }
        
    def _transform_comment(self, comment):
        """
        Transform raw comment data to standardized format.
        
        Args:
            comment (dict): Raw comment data from Apify standard actor
            
        Returns:
            dict: Transformed comment data
        """
        # Standard Apify Facebook comments scraper format
        comment_url = comment.get("commentUrl") or comment.get("url")
        comment_id = None
        
        if comment_url:
            parsed_url = urlparse(comment_url)
            comment_id = parse_qs(parsed_url.query).get('comment_id', [None])[0]
        
        # Handle different timestamp formats
        created_time = comment.get("date") or comment.get("time") or comment.get("created_time")
        
        return {
            "comment_id": comment_id or comment.get("commentId") or comment.get("id"),
            "comment_url": comment_url,
            "user_id": comment.get("profileId") or comment.get("userId") or comment.get("user", {}).get("id"),
            "user_name": comment.get("profileName") or comment.get("userName") or comment.get("user", {}).get("name"),
            "user_url": comment.get("profileUrl") or comment.get("userUrl") or comment.get("user", {}).get("url"),
            "created_time": created_time,
            "message": comment.get("text", ""),
            "like_count": self._parse_human_readable_number(comment.get("likesCount", 0) or comment.get("likes", 0)),
            "reply_count": self._parse_human_readable_number(comment.get("commentsCount", 0) or comment.get("replies", 0)),
            "hashtags": re.findall(r"#(\w+)", comment.get("text", "")),
            "mentions": re.findall(r"@(\w+)", comment.get("text", "")),
            "platform": self.platform,
            "brand_name": self.brand_name
        }
        
    def collect_posts(self):
        """
        Collect posts using the EasyAPI Facebook posts search scraper.
        
        Returns:
            list: List of transformed posts
        """
        print(f"📱 Collecte des posts avec la requête: {self.search_query}")
        
        actor_input = {
            "searchQuery": self.search_query,
            "maxPosts": self.max_posts,
            #"postTimeRange": self.post_time_range,
            "proxyConfig": {"useApifyProxy": True}
        }
        
        run_data = self._run_apify_actor(self.posts_actor_id, actor_input)
        if not run_data:
            return []
            
        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts.")
            return []
            
        posts_data = self._get_dataset_items(dataset_id)
        
        if not posts_data:
            print(f"ℹ️ Aucun post trouvé pour la requête '{self.search_query}'")
            return []
        
        posts = []
        for i, post_item in enumerate(posts_data):
            if i == 0:  # Debug: print first post structure
                print("\n--- Structure du premier post (debug) ---")
                print(json.dumps(post_item, indent=2, ensure_ascii=False))
                print("---------------------------------------\n")
            
            try:
                transformed_post = self._transform_post(post_item)
                posts.append(transformed_post)
            except Exception as e:
                print(f"❌ Erreur lors de la transformation du post {post_item.get('postId', 'Unknown')}: {str(e)}")
                continue
        
        print(f"✅ {len(posts)} posts collectés avec succès.")
        return posts
        
    def collect_comments_for_post(self, post_url):
        """
        Collect comments for a specific post using the standard Apify Facebook comments scraper.
        
        Args:
            post_url (str): URL of the Facebook post
            
        Returns:
            list: List of transformed comments
        """
        print(f"💬 Collecte des commentaires pour : {post_url}")
        
        # Using the standard Apify Facebook comments scraper format
        actor_input = {
            "startUrls": [{"url": post_url}],
            "resultsLimit": self.max_comments_per_post,
            "reactions": True,
            "proxyConfig": {"useApifyProxy": True}
        }
        
        run_data = self._run_apify_actor(self.comments_actor_id, actor_input)
        if not run_data:
            print("⚠️ Impossible de collecter les commentaires")
            return []
            
        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des commentaires.")
            return []
            
        comments_data = self._get_dataset_items(dataset_id)
        
        comments = []
        for i, comment_item in enumerate(comments_data):
            if i == 0 and comments_data:  # Debug: print first comment structure
                print("\n--- Structure du premier commentaire (debug) ---")
                print(json.dumps(comment_item, indent=2, ensure_ascii=False))
                print("--------------------------------------------\n")
            
            try:
                transformed_comment = self._transform_comment(comment_item)
                comments.append(transformed_comment)
            except Exception as e:
                print(f"❌ Erreur lors de la transformation du commentaire: {str(e)}")
                continue
        
        print(f"✅ {len(comments)} commentaires collectés.")
        return comments
        
    def collect_all_data(self):
        """
        Collect all posts and their comments.
        
        Returns:
            list: List of posts with their comments
        """
        print(f"🔍 Début de la collecte pour {self.brand_name}")
        print(f"📊 Paramètres: {self.max_posts} posts, {self.max_comments_per_post} commentaires/post")
        print(f"🔎 Requête de recherche: {self.search_query}")
        print(f"⏰ Période: {self.post_time_range}")
        
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
                post["comments"] = comments
                
                # Add delay between requests to avoid rate limiting
                if i < len(posts):
                    print("⏳ Attente de 2 secondes...")
                    time.sleep(2)
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
            safe_query = self.search_query.replace('#', '').replace(' ', '_').lower()
            filename = f"{self.brand_name}_{safe_query}_facebook_data_{timestamp}.json"
            
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
