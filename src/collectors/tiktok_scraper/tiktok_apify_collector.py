import requests
import time
import re
from datetime import datetime, timedelta
import json
import os
# Import specific exceptions for better handling
from requests.exceptions import RequestException, HTTPError
from json.decoder import JSONDecodeError
from urllib.parse import urlparse, parse_qs


class TiktokAPifyCollector:
    """
    A class to collect TikTok posts and their comments from a specific profile.
    Uses Apify actors 'clockworks~tiktok-profile-scraper' and 'clockworks~tiktok-comments-scraper'.
    """

    def __init__(self, apify_token, brand_name, profile_name, max_posts=2, max_comments_per_post=3, days_back=15):
        """
        Initialize the TiktokAPifyCollector.

        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name associated with the TikTok profile
            profile_name (str): TikTok profile handle (e.g., '@tiktok')
            max_posts (int): Maximum number of posts to collect. Note: The actor might collect slightly more or less based on its logic.
            max_comments_per_post (int): Maximum number of comments per post.
            days_back (int): Number of days to go back for post collection. Note: The TikTok profile actor doesn't strictly support date ranges; it typically scrapes the latest N posts. This parameter is less effective for TikTok than for platforms like Facebook with date filtering APIs.
        """
        if not apify_token:
             raise ValueError("Apify API token must be provided.")
        if not brand_name:
             raise ValueError("Brand name must be provided.")
        if not profile_name:
             raise ValueError("Profile name must be provided.")

        self.apify_token = apify_token
        self.brand_name = brand_name
        self.profile_name = profile_name.strip().lstrip('@') # Remove leading @ if present
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.days_back = days_back
        self.posts_actor_id = 'clockworks~tiktok-profile-scraper' # Actor for posts/profile videos
        self.comments_actor_id = 'clockworks~tiktok-comments-scraper' # Actor for comments on specific videos
        self.platform = "tiktok"

        # Calculate date range (less relevant for the TikTok profile actor's filtering)
        self.from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")

    def _run_apify_actor(self, actor_id, actor_input):
        """
        Run an Apify actor and wait for completion.

        Args:
            actor_id (str): Apify actor ID
            actor_input (dict): Input parameters for the actor

        Returns:
            dict: Actor run data including datasetId, or None if failed
        """
        print(f"🚀 Lancement de l'acteur Apify '{actor_id}'...")

        # Start the actor
        start_url = f'https://api.apify.com/v2/acts/{actor_id}/runs?token={self.apify_token}'
        try:
            start_response = requests.post(start_url, json=actor_input)
            # Raise HTTPError for bad responses (4xx or 5xx)
            start_response.raise_for_status()
        except RequestException as e:
            print(f"❌ Erreur réseau/API lors du lancement de l'acteur {actor_id}: {e}")
            print(f"URL: {start_url}")
            print(f"Input: {actor_input}")
            return None
        except Exception as e: # Catch other unexpected errors during the request
            print(f"❌ Erreur inattendue lors du lancement de l'acteur {actor_id}: {e}")
            return None

        # Check for successful start status code (should be 201 Created)
        if start_response.status_code != 201:
            # This check might be redundant after raise_for_status but provides a specific message
            print(f"❌ Erreur au lancement de l'acteur {actor_id} (status {start_response.status_code}): {start_response.text}")
            return None

        try:
            run_data = start_response.json()
        except JSONDecodeError:
            print(f"❌ Réponse invalide (non-JSON) au lancement de l'acteur {actor_id}.")
            print(f"Réponse: {start_response.text[:500]}...") # Print part of the problematic response
            return None
        except Exception as e:
             print(f"❌ Erreur inattendue lors de l'analyse JSON de la réponse de lancement: {e}")
             return None


        run_id = run_data.get('data', {}).get('id')

        if not run_id:
            print(f"❌ Impossible de récupérer l'ID du run pour l'acteur {actor_id} dans la réponse de démarrage.")
            return None

        # Wait for completion
        print(f"⏳ Exécution du run {run_id} en cours...")
        status_url = f'https://api.apify.com/v2/actor-runs/{run_id}?token={self.apify_token}'
        while True:
            try:
                run_status_response = requests.get(status_url)
                run_status_response.raise_for_status()
            except RequestException as e:
                print(f"❌ Erreur réseau/API lors du suivi du run {run_id}: {e}")
                # Decide how to handle: break and report failure, or retry? Let's break.
                print(f"Arrêt du suivi du run {run_id} suite à une erreur réseau.")
                return None
            except Exception as e:
                print(f"❌ Erreur inattendue lors du suivi du run {run_id}: {e}")
                print(f"Arrêt du suivi du run {run_id} suite à une erreur inattendue.")
                return None

            try:
                # Get status from nested 'data' object
                run_status_data_full = run_status_response.json()
                run_status_data = run_status_data_full.get('data', {})
                status = run_status_data.get('status')

                # Check if required data structure is missing
                if not run_status_data or not status:
                    print(f"❌ Réponse de statut invalide ou incomplète pour le run {run_id}.")
                    print(f"Réponse: {run_status_response.text[:500]}...")
                    return None

            except JSONDecodeError:
                print(f"❌ Réponse de statut invalide (non-JSON) pour le run {run_id}.")
                print(f"Réponse: {run_status_response.text[:500]}...")
                return None
            except Exception as e:
                 print(f"❌ Erreur inattendue lors de l'analyse JSON de la réponse de statut pour le run {run_id}: {e}")
                 return None


            print(f"🔄 Statut run {run_id}: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                break

            time.sleep(5) # Wait before checking status again

        if status != 'SUCCEEDED':
            print(f"❌ L'exécution du run {run_id} a échoué. Statut final : {status}")
            # Optionally retrieve and print logs here if needed for debugging
            return None

        # Check for the dataset ID after a successful run
        dataset_id = run_status_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Le run {run_id} a réussi, mais l'ID du dataset par défaut est manquant dans les données de statut finales.")
            return None # Treat as failure if dataset ID isn't available

        print(f"✅ Run {run_id} terminé avec succès. Dataset ID : {dataset_id}")
        # Return the relevant data, including the dataset ID
        return run_status_data

    def _get_dataset_items(self, dataset_id):
        """
        Retrieve items from an Apify dataset.

        Args:
            dataset_id (str): Dataset ID

        Returns:
            list: Dataset items or empty list if failed
        """
        print(f"📚 Récupération des items du dataset '{dataset_id}'...")
        dataset_url = f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'

        try:
            dataset_response = requests.get(dataset_url)
            dataset_response.raise_for_status() # Handle non-2xx status codes
        except RequestException as e:
            print(f"❌ Erreur réseau/API lors de la récupération du dataset {dataset_id}: {e}")
            print(f"URL: {dataset_url}")
            return []
        except Exception as e:
             print(f"❌ Erreur inattendue lors de la récupération du dataset {dataset_id}: {e}")
             return []


        try:
            items = dataset_response.json()
            # Optional: Add a check if items is actually a list, as expected
            if not isinstance(items, list):
                print(f"❌ Réponse du dataset {dataset_id} n'est pas une liste d'items.")
                print(f"Réponse: {dataset_response.text[:500]}...")
                return []
            print(f"✅ {len(items)} items récupérés du dataset {dataset_id}.")
            return items
        except JSONDecodeError:
            print(f"❌ Réponse du dataset {dataset_id} invalide (non-JSON).")
            print(f"Réponse: {dataset_response.text[:500]}...")
            return []
        except Exception as e:
             print(f"❌ Erreur inattendue lors de l'analyse JSON des items du dataset {dataset_id}: {e}")
             return []


    def _transform_post(self, post):
        """
        Transform raw post data from Apify TikTok profile actor to standardized format.
        Handles potential missing keys gracefully.

        Args:
            post (dict): Raw post data from Apify

        Returns:
            dict: Transformed post data, or None if transformation fails or data is invalid
        """
        if not isinstance(post, dict):
             print(f"⚠️ Entrée de transformation post invalide (pas un dictionnaire): {post}")
             return None

        try:
            # Access nested data safely using .get() and providing empty dict defaults
            author_meta = post.get("authorMeta", {})
            video_meta = post.get("videoMeta", {})

            transformed = {
                "post_id": post.get("id"),
                "source_id": author_meta.get("id"),  # ID de l'utilisateur/page qui a posté
                "created_time": post.get("createTimeISO"),
                "permalink": post.get("webVideoUrl"),
                "page_id": author_meta.get("id"),  # Même que source_id
                "page_name": author_meta.get("name"),  # Nom de la page
                "message": post.get("text", ""), # Primary text content
                "media_type": "photo" if video_meta.get("duration", 0) == 0 else "video", # Determine type based on video duration
                "media_url": post.get("webVideoUrl"),
                "thumbnail_url": video_meta.get("coverUrl"),
                "can_share": True, # Assume sharing is possible unless API indicates otherwise
                "shares": post.get("shareCount", 0),
                "can_comment": True, # Assume commenting is possible unless API indicates otherwise
                "comments_count": post.get("commentCount", 0),
                "can_like": True, # Assume liking is possible unless API indicates otherwise
                "like_count": post.get("diggCount", 0),
                "playCount" : post.get("playCount"), # Specific TikTok metric
                "hashtags": post.get("hashtags", []), # Default to empty list if not present
                "mentions": post.get("mentions", []), # Default to empty list if not present
                "caption":  post.get("text", ""), # TikTok often uses text field as caption/description
                "description": post.get("desc", ""), # Some actors might have a separate 'desc' field
                "platform": self.platform,
                "brand_name": self.brand_name,
                "comments": []  # Will be populated later
            }

            # Basic validation for essential fields
            if not transformed.get("post_id"):
                print(f"⚠️ Transformation post échouée: ID manquant pour l'item raw: {json.dumps(post)[:200]}...")
                return None
            if not transformed.get("permalink"):
                 print(f"⚠️ Transformation post: permalink manquant pour post {transformed['post_id']}.")
                 # We might still return the post if permalink is missing, but comments can't be collected.
                 # For now, let's return None to indicate this post is problematic.
                 return None
            if not transformed.get("created_time"):
                 print(f"⚠️ Transformation post: created_time manquant pour post {transformed['post_id']}.")
                 # Decide if this is critical; let's allow it for now but print warning

            return transformed

        except Exception as e:
            # Catch any unexpected error during transformation of a single item
            post_id_safe = post.get('id', 'Inconnu')
            print(f"❌ Erreur inattendue lors de la transformation du post (ID raw: {post_id_safe}): {e}")
            return None # Indicate failure for this specific item


    def _transform_comment(self, comment):
        """
        Transform raw comment data from Apify TikTok comments actor to standardized format.
        Handles potential missing keys gracefully.

        Args:
            comment (dict): Raw comment data from Apify

        Returns:
            dict: Transformed comment data, or None if transformation fails or data is invalid
        """
        if not isinstance(comment, dict):
             print(f"⚠️ Entrée de transformation commentaire invalide (pas un dictionnaire): {comment}")
             return None

        try:
            user_info = comment.get("user", {}) # Access user info safely
            user_unique_id = user_info.get("uniqueId") # Get uniqueId from user info

            transformed = {
                "comment_id": comment.get("cid"),
                "comment_url": comment.get("commentUrl", ""), # Default to empty string
                "user_id": user_info.get("uid"), # Get user ID from user info
                "user_name": user_unique_id,
                "user_url": f"https://www.tiktok.com/@{user_unique_id}/" if user_unique_id else None,
                "created_time": comment.get("createTimeISO"),
                "message": comment.get("text", ""),
                "like_count": comment.get("diggCount", 0),
                "reply_count": comment.get("replyCommentTotal", 0),
                # Extract hashtags and mentions from the text field if not provided separately
                "hashtags": re.findall(r"#(\w+)", comment.get("text", "")),
                "mentions": re.findall(r"@(\w+)", comment.get("text", "")),
                "platform": self.platform,
                "brand_name": self.brand_name
            }

            # Basic validation for essential fields
            if not transformed.get("comment_id"):
                 print(f"⚠️ Transformation commentaire échouée: ID (cid) manquant pour l'item raw: {json.dumps(comment)[:200]}...")
                 return None
            if not transformed.get("user_id"):
                 print(f"⚠️ Transformation commentaire échouée: user ID (uid) manquant pour commentaire {transformed['comment_id']}.")
                 return None


            return transformed

        except Exception as e:
            # Catch any unexpected error during transformation of a single item
            comment_id_safe = comment.get('cid', 'Inconnu')
            print(f"❌ Erreur inattendue lors de la transformation du commentaire (ID raw: {comment_id_safe}): {e}")
            return None # Indicate failure for this specific item


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
            "resultsPerPage": self.max_posts, # Max number of videos to scrape
            "shouldDownloadAvatars": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSlideshowImages": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadVideos": False,
            "profileScrapeSections": ["videos"], # Ensure videos are scraped
            "profileSorting": "latest" # Sort by latest videos
            # Note: This actor doesn't have a strict date filter like oldestPostDateUnified
            # We rely on max_posts to limit the recent videos
        }

        run_data = self._run_apify_actor(self.posts_actor_id, actor_input)

        if run_data is None: # _run_apify_actor failed and returned None
            print("❌ L'exécution de l'acteur posts a échoué. Impossible de continuer la collecte des posts.")
            return []

        # dataset_id is checked within _run_apify_actor before returning success,
        # so we can assume it's present if run_data is not None
        dataset_id = run_data.get('defaultDatasetId')
        # Double-check just in case, although _run_apify_actor should prevent this state
        if not dataset_id:
             print("❌ Erreur interne: datasetId manquant après un run d'acteur posts qui a rapporté succès. Impossible de récupérer les données.")
             return []


        posts_data = self._get_dataset_items(dataset_id)

        if not posts_data: # _get_dataset_items failed or returned empty list
            print(f"⚠️ Aucun item brut récupéré du dataset '{dataset_id}' de posts.")
            return []

        # Transform collected raw items, filtering out any that failed transformation
        transformed_posts = [self._transform_post(post) for post in posts_data]
        posts = [post for post in transformed_posts if post is not None]

        print(f"✅ {len(posts)} posts transformés avec succès (sur {len(posts_data)} items bruts).")
        if len(posts) == 0 and len(posts_data) > 0:
            print("⚠️ Aucun post n'a pu être transformé. Vérifiez la structure des données brutes.")

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
        # Optional: Add more specific TikTok URL validation

        # Input for the TikTok comments scraper actor
        actor_input = {
            "postURLs": [post_url],
            "commentsPerPost": self.max_comments_per_post,
            "maxRepliesPerComment": 0, # Set to 0 to avoid collecting replies
            "resultsPerPage": 100 # This is an internal batch size, not total results limit
        }

        run_data = self._run_apify_actor(self.comments_actor_id, actor_input)

        if run_data is None: # _run_apify_actor failed and returned None
             print(f"❌ L'exécution de l'acteur commentaires a échoué pour la vidéo {post_url}.")
             return []

        # dataset_id is checked within _run_apify_actor
        dataset_id = run_data.get('defaultDatasetId')
        # Double-check
        if not dataset_id:
             print(f"❌ Erreur interne: datasetId manquant après un run d'acteur commentaires qui a rapporté succès pour {post_url}. Impossible de récupérer les données.")
             return []

        comments_data = self._get_dataset_items(dataset_id)

        if not comments_data: # _get_dataset_items failed or returned empty list
            print(f"⚠️ Aucun item brut récupéré du dataset '{dataset_id}' de commentaires pour {post_url}.")
            return []

        # Transform collected raw items, filtering out any that failed transformation
        transformed_comments = [self._transform_comment(comment) for comment in comments_data]
        comments = [comment for comment in transformed_comments if comment is not None]

        print(f"✅ {len(comments)} commentaires transformés avec succès (sur {len(comments_data)} items bruts) pour la vidéo {post_url}.")
        if len(comments) == 0 and len(comments_data) > 0:
            print(f"⚠️ Aucun commentaire n'a pu être transformé pour la vidéo {post_url}. Vérifiez la structure des données brutes.")

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
        processed_posts = [] # List to store posts after attempting comment collection

        # Collect comments for each post
        for i, post in enumerate(posts, 1):
            post_id_safe = post.get('post_id', 'N/A')
            post_url = post.get("permalink") # Use .get() for safety

            if post_url:
                print(f"\n--- Traitement Post {i}/{len(posts)} (ID: {post_id_safe}) ---")
                # collect_comments_for_post handles its own errors and returns [] on failure
                comments = self.collect_comments_for_post(post_url)
                post["comments"] = comments
                # Append the post to the list regardless of comment collection success/failure for this post
                processed_posts.append(post)
            else:
                print(f"\n--- Traitement Post {i}/{len(posts)} (ID: {post_id_safe}) ---")
                print(f"⚠️ Le post {post_id_safe} n'a pas de 'permalink' valide. La collecte des commentaires est ignorée pour ce post.")
                post["comments"] = [] # Ensure the key exists, even if empty
                processed_posts.append(post) # Still include the post data collected

            # Add a delay between comment collection calls for different posts
            if i < len(posts):
                print("⏳ Attente de 10 secondes avant de collecter les commentaires du prochain post...")
                time.sleep(10)

        print("\n--- Fin du processus de collecte complet ---")
        print(f"🏁 Total posts traités : {len(processed_posts)} (sur {len(posts)} initialement collectés)")
        total_comments = sum(len(p.get('comments', [])) for p in processed_posts)
        print(f"🏁 Total commentaires collectés : {total_comments}")


        return processed_posts

