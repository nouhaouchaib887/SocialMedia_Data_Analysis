import requests
import time
import re
from datetime import datetime, timedelta
import json
import os
from urllib.parse import urlparse, parse_qs
import requests.exceptions # Import exceptions module

class InstagramAPifyCollector:
    """
    A class to collect Instagram posts and their comments from a specific user page using Apify.
    """

    def __init__(self, apify_token, brand_name, user_name, max_posts=2, max_comments_per_post=3, days_back=15):
        """
        Initialize the InstagramAPifyCollector.

        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name associated with the Instagram page (for internal labeling)
            user_name (str): Instagram username (e.g., 'instagram', 'natgeo')
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post per post
            days_back (int): Number of days to go back for post collection (Note: Apify Instagram scraper doesn't directly support `onlyPostsNewerThan`, but we keep the variable)
        """
        if not apify_token:
            raise ValueError("Apify API token is required.")
        if not brand_name:
             raise ValueError("Brand name is required.")
        if not user_name:
             raise ValueError("Instagram user name is required.")

        self.apify_token = apify_token
        self.brand_name = brand_name
        self.user_name = user_name
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.days_back = days_back
        self.posts_actor_id = 'apify~instagram-post-scraper'
        self.comments_actor_id = 'apify~instagram-comment-scraper'
        self.platform = "instagram"

        # Calculate date range - Note: This isn't directly used by the current Apify Instagram scraper,
        # but included for potential future use or context.
        self.from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")

        # --- Retry Configuration ---
        self._max_retries = 3
        self._retry_delay_seconds = 5
        # --------------------------


    def _make_request(self, method, url, **kwargs):
        """
        Helper function to make robust API requests with retries.

        Args:
            method (str): HTTP method ('GET', 'POST')
            url (str): URL to request
            **kwargs: Additional arguments for requests.request

        Returns:
            requests.Response: The response object if successful
            None: If request fails after retries
        """
        for attempt in range(self._max_retries):
            try:
                print(f"Tentative {attempt + 1}/{self._max_retries} pour {method} {url}...")
                response = requests.request(method, url, **kwargs)
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                return response # Success

            except requests.exceptions.RequestException as e:
                print(f"⚠️ Erreur de requête (Tentative {attempt + 1}): {e}")
                if attempt < self._max_retries - 1:
                    print(f"⏳ Nouvelle tentative dans {self._retry_delay_seconds} secondes...")
                    time.sleep(self._retry_delay_seconds)
                else:
                    print(f"❌ Échec de la requête après {self._max_retries} tentatives.")
                    return None
            except Exception as e:
                 # Catch any other unexpected errors during the request
                 print(f"⚠️ Erreur inattendue lors de la requête (Tentative {attempt + 1}): {e}")
                 if attempt < self._max_retries - 1:
                    print(f"⏳ Nouvelle tentative dans {self._retry_delay_seconds} secondes...")
                    time.sleep(self._retry_delay_seconds)
                 else:
                    print(f"❌ Échec de la requête après {self._max_retries} tentatives.")
                    return None


    def _run_apify_actor(self, actor_id, actor_input):
        """
        Run an Apify actor and wait for completion.

        Args:
            actor_id (str): Apify actor ID
            actor_input (dict): Input parameters for the actor

        Returns:
            dict: Actor run data if successful, None otherwise.
        """
        print(f"🚀 Lancement de l'acteur {actor_id}...")

        start_url = f'https://api.apify.com/v2/acts/{actor_id}/runs?token={self.apify_token}'
        start_response = self._make_request('POST', start_url, json=actor_input)

        if start_response is None:
             print(f"❌ Impossible de démarrer l'acteur {actor_id} (échec de la requête).")
             return None

        try:
            run_data = start_response.json()
        except json.JSONDecodeError:
            print(f"❌ Erreur: Réponse invalide (pas de JSON) lors du lancement de l'acteur {actor_id}.")
            print(f"Réponse brute: {start_response.text}")
            return None

        run_id = run_data.get('data', {}).get('id')

        if not run_id:
            print(f"❌ Impossible de récupérer l'ID du run pour l'acteur {actor_id}. Données de réponse: {run_data}")
            return None

        # Wait for completion
        print(f"⏳ Exécution de l'acteur {actor_id} (ID du run: {run_id}) en cours...")
        status_url = f'https://api.apify.com/v2/actor-runs/{run_id}?token={self.apify_token}'

        while True:
            run_status_response = self._make_request('GET', status_url)

            if run_status_response is None:
                 # If status check fails, we can't know the outcome. Assume failure or temporary issue.
                 print(f"❌ Échec de la récupération du statut du run {run_id}. Abandon.")
                 return None # Cannot proceed if status cannot be checked

            try:
                run_status_data = run_status_response.json().get('data', {})
                status = run_status_data.get('status')
            except json.JSONDecodeError:
                print(f"❌ Erreur: Réponse invalide (pas de JSON) lors de la vérification du statut du run {run_id}. Réponse brute: {run_status_response.text}")
                # Continue waiting or break? Let's wait, maybe it was a transient issue.
                time.sleep(self._retry_delay_seconds)
                continue # Try fetching status again

            if status:
                print(f"🔄 Statut du run {run_id} : {status}")
                if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                    break
            else:
                 # Status field is missing in the response data
                 print(f"⚠️ Le champ 'status' est manquant dans la réponse du statut du run {run_id}. Données: {run_status_data}")
                 # Wait and try again, maybe the data structure is temporarily inconsistent
                 time.sleep(self._retry_delay_seconds)
                 continue


            # Wait before polling again
            time.sleep(5) # Keep 5s polling interval as in original code

        if status != 'SUCCEEDED':
            print(f"❌ Exécution de l'acteur {actor_id} échouée. Statut final : {status}")
            return None

        print(f"✅ Exécution de l'acteur {actor_id} terminée avec succès.")
        return run_status_data


    def _get_dataset_items(self, dataset_id):
        """
        Retrieve items from an Apify dataset.

        Args:
            dataset_id (str): Dataset ID

        Returns:
            list: Dataset items or empty list if failed
        """
        print(f"Fetching data from dataset {dataset_id}...")
        dataset_url = f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
        dataset_response = self._make_request('GET', dataset_url)

        if dataset_response is None:
            print(f"❌ Échec de la récupération des données du dataset {dataset_id} (échec de la requête).")
            return []

        try:
            items = dataset_response.json()
            print(f"✅ {len(items)} items récupérés du dataset {dataset_id}.")
            return items
        except json.JSONDecodeError:
            print(f"❌ Erreur: Réponse invalide (pas de JSON) lors de la récupération des items du dataset {dataset_id}.")
            print(f"Réponse brute: {dataset_response.text}")
            return []


    def _transform_post(self, post_data):
        """
        Transform raw post data to standardized format.

        Args:
            post_data (dict): Raw post data from Apify

        Returns:
            dict: Transformed post data or None if input is invalid
        """
        if not isinstance(post_data, dict):
            print(f"⚠️ Skipping non-dictionary post data: {post_data}")
            return None

        # Use .get() for safety, but explicitly check for essential keys if needed
        post_id = post_data.get("id")
        if not post_id:
             print(f"⚠️ Skipping post with missing ID: {post_data}")
             return None

        # Handle potential errors or missing data during transformation gracefully
        try:
            transformed = {
                "post_id": post_id,
                "source_id": post_data.get("ownerId"),
                "created_time": post_data.get("timestamp"),
                "updated_time": None, # Instagram API doesn't typically provide update time
                "permalink": post_data.get("url"),
                "page_id": post_data.get("ownerId"), # Same as source_id for Instagram
                "page_name": post_data.get("ownerUsername"),
                "message": post_data.get("caption", ""), # Use caption for the main text
                "media_type": post_data.get("type"),
                "media_url": post_data.get("videoUrl") or post_data.get("url"), # Video or image URL
                "thumbnail_url": post_data.get("displayUrl", ""), # Thumbnail for videos/images
                "can_share": True, # Instagram generally allows sharing
                "shares": post_data.get("shares", 0), # 'shares' field
                "can_comment": not post_data.get("isCommentsDisabled", False), # 'isCommentsDisabled' boolean
                "comments_count": post_data.get("commentsCount", 0), # 'commentsCount' field
                "can_like": True, # Instagram generally allows liking
                "like_count": post_data.get("likesCount", 0), # 'likesCount' field
                # Hashtags and mentions can be extracted from caption if not provided directly
                "hashtags": post_data.get("hashtags") or re.findall(r"#(\w+)", post_data.get("caption", "")),
                "mentions": post_data.get("mentions") or re.findall(r"@(\w+)", post_data.get("caption", "")),
                "caption": post_data.get("caption", ""), # Explicitly keep original caption
                "description": post_data.get("caption", ""), # Use caption as description too if no separate field
                "platform": self.platform,
                "brand_name": self.brand_name,
                "comments": []  # Will be populated later
            }
            return transformed
        except Exception as e:
            print(f"❌ Erreur lors de la transformation du post {post_id or 'unknown'}: {e}")
            # Print the problematic data for debugging
            # print(f"Probleatic post data: {post_data}")
            return None # Return None if transformation fails


    def _transform_comment(self, comment_data):
        """
        Transform raw comment data to standardized format.

        Args:
            comment_data (dict): Raw comment data from Apify

        Returns:
            dict: Transformed comment data or None if input is invalid
        """
        if not isinstance(comment_data, dict):
            print(f"⚠️ Skipping non-dictionary comment data: {comment_data}")
            return None

        comment_id = comment_data.get("id")
        if not comment_id:
             print(f"⚠️ Skipping comment with missing ID: {comment_data}")
             return None

        try:
            # Extract user info safely
            owner_username = comment_data.get("ownerUsername")
            owner_id = comment_data.get("owner", {}).get("id") # Access nested dict safely

            transformed = {
                "comment_id": str(comment_id), # Ensure ID is a string
                "comment_url": comment_data.get("commentUrl"),
                "user_id": owner_id,
                "user_name": owner_username,
                "user_url": f"https://www.instagram.com/{owner_username}/" if owner_username else None,
                "created_time": comment_data.get("timestamp"),
                "message": comment_data.get("text", ""),
                "like_count": comment_data.get("likesCount", 0),
                "reply_count": comment_data.get("repliesCount", 0),
                # Extract hashtags and mentions from text field
                "hashtags": re.findall(r"#(\w+)", comment_data.get("text", "")),
                "mentions": re.findall(r"@(\w+)", comment_data.get("text", "")),
                "platform": self.platform,
                "brand_name": self.brand_name,
                # No nested comments in this scraper's output structure based on input parameter
                # "replies": []
            }
            return transformed
        except Exception as e:
            print(f"❌ Erreur lors de la transformation du commentaire {comment_id or 'unknown'}: {e}")
             # Print the problematic data for debugging
            # print(f"Probleatic comment data: {comment_data}")
            return None # Return None if transformation fails


    def collect_posts(self):
        """
        Collect posts from the Instagram page.

        Returns:
            list: List of transformed posts
        """
        print(f"📱 Collecte des posts de l'utilisateur Instagram '{self.user_name}'...")

        actor_input = {
            "username": [self.user_name],
            "resultsLimit": self.max_posts,
            "commentsMode": "none", # Don't collect comments in the post run
            "reactions": True, # Collect likes, comments counts
            # "onlyPostsNewerThan": self.from_date, # Apify IG actor doesn't support this directly
            "proxyConfig": {"useApifyProxy": True},
            "captionText": True, # Ensure caption is included
             # Add other potentially useful fields from the actor documentation if needed
             "mediaUrl": True,
             "timestamp": True,
             "ownerInfo": True
        }

        run_data = self._run_apify_actor(self.posts_actor_id, actor_input)
        if run_data is None:
            print("❌ Échec de l'exécution de l'acteur de posts.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts après une exécution réussie.")
            return []

        posts_data = self._get_dataset_items(dataset_id)
        if not posts_data:
            print("❌ Aucun item trouvé dans le dataset des posts.")
            return []

        # Transform posts, filtering out any failed transformations
        posts = [
            self._transform_post(post_item)
            for post_item in posts_data
            if self._transform_post(post_item) is not None # Only include successful transformations
        ]

        print(f"✅ {len(posts)} posts transformés et collectés avec succès (après filtrage des erreurs).")
        return posts


    def collect_comments_for_post(self, post_url):
        """
        Collect comments for a specific post URL.

        Args:
            post_url (str): URL of the Instagram post

        Returns:
            list: List of transformed comments
        """
        if not post_url:
             print("⚠️ Cannot collect comments: Post URL is missing.")
             return []

        print(f"💬 Collecte des commentaires pour : {post_url}")

        actor_input = {
            "directUrls": [post_url],
            "resultsLimit": self.max_comments_per_post,
            "reactions": True, # Collect likes on comments
            "proxyConfig": {"useApifyProxy": True},
            "includeNestedComments": False, # As per requirement
            "isNewestComments": False, # As per requirement (assuming oldest/most relevant first is default)
            # Add ownerInfo for comments
            "ownerInfo": True
        }

        run_data = self._run_apify_actor(self.comments_actor_id, actor_input)
        if run_data is None:
            print(f"❌ Échec de l'exécution de l'acteur de commentaires pour {post_url}.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Impossible de récupérer l'ID du dataset des commentaires pour {post_url} après une exécution réussie.")
            return []

        comments_data = self._get_dataset_items(dataset_id)
        if not comments_data:
            print(f"✅ Aucun commentaire trouvé pour {post_url}.")
            return []

         # Transform comments, filtering out any failed transformations
        comments = [
            self._transform_comment(comment_item)
            for comment_item in comments_data
            if self._transform_comment(comment_item) is not None # Only include successful transformations
        ]


        print(f"✅ {len(comments)} commentaires transformés et collectés pour {post_url} (après filtrage des erreurs).")
        return comments


    def collect_all_data(self):
        """
        Collect all posts and their comments.

        Returns:
            list: List of posts with their comments. Returns empty list if the initial post collection fails.
        """
        print(f"\n--- Début de la collecte pour '{self.brand_name}' (Instagram: '{self.user_name}') ---")
        print(f"📊 Paramètres: {self.max_posts} posts, {self.max_comments_per_post} commentaires/post, {self.days_back} jours (approximatif).")

        try:
            # Collect posts
            posts = self.collect_posts()

            if not posts:
                print("❌ Aucun post collecté ou transformé. Arrêt de la collecte.")
                return []

            # Collect comments for each post
            print(f"\n--- Collecte des commentaires pour les {len(posts)} posts collectés ---")
            for i, post in enumerate(posts, 1):
                post_url = post.get("permalink")

                if not post_url:
                    print(f"\n--- Post {i}/{len(posts)} (ID: {post.get('post_id', 'unknown')}) ---")
                    print("⚠️ Ce post n'a pas de permalink. Impossible de collecter les commentaires.")
                    post["comments"] = [] # Ensure comments field exists, even if empty
                    continue # Skip to the next post

                print(f"\n--- Post {i}/{len(posts)} (ID: {post.get('post_id', 'unknown')}) ---")
                comments = self.collect_comments_for_post(post_url)
                post["comments"] = comments

                # Add delay between comment collection calls to avoid rate limiting or overloading
                if i < len(posts):
                    delay = 15 # Increased delay slightly for comments
                    print(f"⏳ Attente de {delay} secondes avant le prochain post...")
                    time.sleep(delay)

            print("\n--- Collecte terminée ---")
            return posts

        except Exception as e:
            # Catch any unexpected exception during the overall process
            print(f"\n❌ Une erreur inattendue est survenue pendant la collecte : {e}")
            # In a real application, you might want to log this exception properly
            return [] # Return empty list on critical failure