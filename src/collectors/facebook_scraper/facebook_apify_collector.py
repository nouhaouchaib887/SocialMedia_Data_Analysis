import requests
import time
import re
from datetime import datetime, timedelta
import json
import os
from urllib.parse import urlparse, parse_qs
import requests.exceptions # Import needed for network errors

class FacebookAPifyCollector:
    """
    A class to collect Facebook posts and their comments from a specific brand page
    using the Apify API.
    """

    def __init__(self, apify_token, brand_name, page_name, max_posts=2, max_comments_per_post=3, days_back=15):
        """
        Initialize the FacebookCollector.

        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for Facebook page (e.g., 'inwi.ma', 'orangemaroc')
            page_name (str): Facebook page name (e.g., 'inwi', 'OrangeMaroc')
            max_posts (int): Maximum number of posts to collect. Note: Actor's resultsLimit
                             might prioritize count over strict date range.
            max_comments_per_post (int): Maximum number of comments per post.
            days_back (int): Number of days to go back for post collection (used to
                             calculate `from_date`, but actor limits/filters apply).

        Raises:
            ValueError: If required input parameters are missing or invalid.
        """
        if not apify_token or not isinstance(apify_token, str):
            raise ValueError("apify_token must be a non-empty string.")
        if not brand_name or not isinstance(brand_name, str):
             raise ValueError("brand_name must be a non-empty string.")
        if not page_name or not isinstance(page_name, str):
             raise ValueError("page_name must be a non-empty string.")
        if not isinstance(max_posts, int) or max_posts < 0:
             raise ValueError("max_posts must be a non-negative integer.")
        if not isinstance(max_comments_per_post, int) or max_comments_per_post < 0:
             raise ValueError("max_comments_per_post must be a non-negative integer.")
        if not isinstance(days_back, int) or days_back < 0:
             raise ValueError("days_back must be a non-negative integer.")

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

        try:
            start_response = requests.post(
                f'https://api.apify.com/v2/acts/{actor_id}/runs?token={self.apify_token}',
                json=actor_input
            )
            start_response.raise_for_status() # Raise HTTPError for bad responses
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors du lancement de l'acteur {actor_id}: {e}")
            return None

        run_data = start_response.json()
        run_id = run_data.get('data', {}).get('id')

        if not run_id:
            print(f"❌ Impossible de récupérer l'ID du run pour l'acteur {actor_id}.")
            return None

        print(f"⏳ Exécution en cours (Run ID: {run_id})...")
        while True:
            try:
                run_status_response = requests.get(
                    f'https://api.apify.com/v2/actor-runs/{run_id}?token={self.apify_token}'
                )
                run_status_response.raise_for_status()
                run_status_data = run_status_response.json().get('data', {})
                status = run_status_data.get('status')

                print(f"🔄 Statut : {status}")
                if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                    break

            except requests.exceptions.RequestException as e:
                # Network error during status check, retry after delay
                print(f"⚠️ Erreur réseau ou HTTP lors de la vérification du statut du run {run_id}: {e}. Réessai...")
                status = 'RETRYING' # Indicate that we are retrying

            if status == 'RETRYING':
                 time.sleep(10)
            elif status not in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                time.sleep(5)
            else:
                pass

        if status != 'SUCCEEDED':
            print(f"❌ Échec de l'exécution de l'acteur {actor_id}. Statut final : {status}")
            return None

        print(f"✅ Acteur {actor_id} exécuté avec succès.")
        return run_status_data


    def _get_dataset_items(self, dataset_id):
        """
        Retrieve items from an Apify dataset.

        Args:
            dataset_id (str): Dataset ID

        Returns:
            list: Dataset items or empty list if failed
        """
        print(f"📦 Récupération des données du dataset {dataset_id}...")
        try:
            dataset_response = requests.get(
                f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.apify_token}&clean=true'
            )
            dataset_response.raise_for_status()
            return dataset_response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors de la récupération des données du dataset {dataset_id}: {e}")
            return []
        except json.JSONDecodeError:
            print(f"❌ Erreur de décodage JSON lors de la lecture du dataset {dataset_id}. Données inattendues.")
            return []


    def _transform_post(self, post):
        """
        Transform raw post data to standardized format.

        Args:
            post (dict): Raw post data from Apify

        Returns:
            dict: Transformed post data or None if transformation fails
        """
        try:
            if not isinstance(post, dict) or 'postId' not in post or 'timestamp' not in post or 'user' not in post or 'id' not in post.get('user', {}):
                 print(f"⚠️ Post skipped due to missing essential keys or unexpected structure: {post}")
                 return None

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

            timestamp = post["timestamp"]
            if not isinstance(timestamp, (int, float)):
                 print(f"⚠️ Post skipped: Invalid timestamp format {timestamp}")
                 return None

            created_time_iso = datetime.utcfromtimestamp(timestamp).isoformat()

            post_id = post["postId"]
            permalink = f"https://www.facebook.com/{self.page_name}/posts/{post_id}" if self.page_name and post_id else None
            if not permalink:
                 print(f"⚠️ Post skipped: Could not create permalink for post ID {post_id}")
                 return None

            transformed_post = {
                "post_id": post_id,
                "source_id": post["user"]["id"], # Assuming 'id' is always present if 'user' is
                "created_time": created_time_iso,
                "permalink": permalink,
                "page_id": post["user"]["id"],
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
                "comments": []
            }

            return transformed_post

        except Exception as e:
            print(f"❌ Erreur lors de la transformation du post (ID potentiel: {post.get('postId', 'N/A')}): {e}")
            return None


    def _transform_comment(self, comment):
        """
        Transform raw comment data to standardized format.

        Args:
            comment (dict): Raw comment data from Apify

        Returns:
            dict: Transformed comment data or None if transformation fails
        """
        try:
            if not isinstance(comment, dict) or 'commentUrl' not in comment or 'profileId' not in comment:
                print(f"⚠️ Comment skipped due to missing essential keys or unexpected structure: {comment}")
                return None

            comment_url = comment.get("commentUrl")
            comment_id = None
            if comment_url:
                try:
                    parsed_url = urlparse(comment_url)
                    query_params = parse_qs(parsed_url.query)
                    comment_id = query_params.get('comment_id', [None])[0]
                except Exception as e:
                    print(f"⚠️ Could not parse comment_id from URL {comment_url}: {e}")


            transformed_comment = {
                "comment_id": comment_id,
                "comment_url": comment_url,
                "user_id": comment.get("profileId"),
                "user_name": comment.get("profileName"),
                "user_url": comment.get("profileUrl"),
                "created_time": comment.get("date"),
                "message": comment.get("text", ""),
                "like_count": comment.get("likesCount", 0),
                "reply_count": comment.get("commentsCount", 0), # This is reply count from Apify, not comment count on the post
                "hashtags": re.findall(r"#(\w+)", comment.get("text", "")),
                "mentions": re.findall(r"@(\w+)", comment.get("text", "")),
                "platform": self.platform,
                "brand_name": self.brand_name
            }

            return transformed_comment

        except Exception as e:
            print(f"❌ Erreur lors de la transformation du commentaire (URL potentielle: {comment.get('commentUrl', 'N/A')}): {e}")
            return None


    def collect_posts(self):
        """
        Collect posts from the Facebook page using the Apify actor.

        Returns:
            list: List of transformed posts (excluding failed transformations).
                  Note: max_posts is the primary limit, date filtering is less strict.
        """
        print(f"📱 Collecte des posts de {self.facebook_url} depuis {self.from_date}")

        actor_input = {
            "startUrls": [{"url": self.facebook_url}],
            "resultsLimit": self.max_posts,
            "mode": "page",
            "commentsMode": "none", # Do not collect comments with posts actor
            "reactions": True,
            # "onlyPostsNewerThan": self.from_date, # This input might not be reliable across actor versions
            "proxyConfig": {"useApifyProxy": True},
            "captionText": False
        }

        run_data = self._run_apify_actor(self.posts_actor_id, actor_input)
        if not run_data:
            print("⚠️ Le run pour les posts a échoué ou n'a pas produit de données de run.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts depuis les données du run.")
            return []

        posts_data = self._get_dataset_items(dataset_id)

        transformed_posts = [self._transform_post(post) for post in posts_data]

        # Filter out any posts where transformation failed
        posts = [post for post in transformed_posts if post is not None]

        print(f"✅ {len(posts_data)} items bruts reçus du dataset posts. {len(posts)} posts transformés avec succès.")
        return posts


    def collect_comments_for_post(self, post_url):
        """
        Collect comments for a specific post URL using the Apify actor.

        Args:
            post_url (str): URL of the Facebook post

        Returns:
            list: List of transformed comments (excluding failed transformations).
                  Limited by max_comments_per_post.
        """
        if not post_url:
            print("⚠️ collect_comments_for_post appelé avec une URL de post vide.")
            return []

        print(f"💬 Collecte des commentaires pour : {post_url}")

        actor_input = {
            "startUrls": [{"url": post_url}],
            "resultsLimit": self.max_comments_per_post,
            "reactions": True,
            "proxyConfig": {"useApifyProxy": True}
        }

        run_data = self._run_apify_actor(self.comments_actor_id, actor_input)
        if not run_data:
            print(f"⚠️ Le run pour les commentaires du post {post_url} a échoué.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Impossible de récupérer l'ID du dataset des commentaires pour le post {post_url}.")
            return []

        comments_data = self._get_dataset_items(dataset_id)

        transformed_comments = [self._transform_comment(comment) for comment in comments_data]

        # Filter out any comments where transformation failed
        comments = [comment for comment in transformed_comments if comment is not None]

        print(f"✅ {len(comments_data)} items bruts reçus du dataset commentaires. {len(comments)} commentaires transformés avec succès pour {post_url}.")
        return comments


    def collect_all_data(self):
        """
        Collect all posts and their comments.

        First collects a limited number of recent posts, then collects a limited
        number of comments for each collected post. Includes delays between Apify calls.

        Returns:
            list: List of posts with their comments. Each post is a dict, and
                  includes a 'comments' key containing a list of comment dicts.
        """
        print(f"🔍 Début de la collecte pour {self.brand_name}")
        print(f"📊 Paramètres: {self.max_posts} posts max, {self.max_comments_per_post} commentaires max/post, recherche jusqu'à {self.days_back} jours en arrière")

        posts = self.collect_posts()

        if not posts:
            print("❌ Aucun post collecté ou transformé avec succès.")
            return []

        print(f"\n--- Début de la collecte des commentaires pour {len(posts)} posts ---")

        for i, post in enumerate(posts, 1):
            print(f"\n--- Traitement Post {i}/{len(posts)} (ID: {post.get('post_id', 'N/A')}) ---")

            post_url = post.get("permalink")

            if post_url:
                if i == 1:
                     time.sleep(5) # Add initial delay before first comment collection

                comments = self.collect_comments_for_post(post_url)
                post["comments"] = comments

                if i < len(posts):
                    time.sleep(10) # Add delay between collecting comments for different posts
            else:
                print(f"⚠️ Pas de permalink disponible pour le post ID {post.get('post_id', 'N/A')}. Skipping comment collection.")
                post["comments"] = [] # Ensure comments key exists

        print("\n--- Collecte terminée ---")
        return posts
