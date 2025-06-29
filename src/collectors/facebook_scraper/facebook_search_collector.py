import requests
import time
import re
from datetime import datetime, timezone
import json
import os
from urllib.parse import urlparse, parse_qs

from requests.exceptions import RequestException
from json.decoder import JSONDecodeError

class FacebookSearchCollector:
    """
    A class to collect Facebook posts and their comments using EasyAPI and standard Apify actors.
    Includes basic error handling for API interactions.
    """

    def __init__(self, apify_token, brand_name, search_query=None, page_name=None, max_posts=2, max_comments_per_post=3, post_time_range="30d"):
        """
        Initialize the FacebookEasyAPICollector.

        Args:
            apify_token (str): Apify API token
            brand_name (str): Brand name for identification
            search_query (str): Search query for posts (e.g., '#orangemaroc', 'orangemaroc')
            page_name (str): Page name for direct page scraping (alternative to search - Note: check actor support)
            max_posts (int): Maximum number of posts to collect
            max_comments_per_post (int): Maximum number of comments per post
            post_time_range (str): Time range for posts (e.g., '30d', '7d', '90d') - Note: May not be supported by selected posts actor.
        """
        if not apify_token:
            raise ValueError("Apify token cannot be None or empty.")
        if not brand_name:
            raise ValueError("Brand name cannot be None or empty.")

        self.apify_token = apify_token
        self.brand_name = brand_name
        # Default to searching for the brand name as a hashtag if no query is provided
        self.search_query = search_query or f"#{brand_name}"
        self.max_posts = max_posts
        self.max_comments_per_post = max_comments_per_post
        self.post_time_range = post_time_range # Parameter for potential future use or different actor

        # Actor IDs used for the collection process
        # easyapi~facebook-posts-search-scraper: Searches for posts by query, page, etc.
        # apify~facebook-comments-scraper: Collects comments from a specific post URL.
        self.posts_actor_id = 'easyapi~facebook-posts-search-scraper'
        self.comments_actor_id = 'apify~facebook-comments-scraper'
        self.platform = "facebook"

    def _run_apify_actor(self, actor_id, actor_input):
        """
        Run an Apify actor and wait for completion.

        Args:
            actor_id (str): Apify actor ID
            actor_input (dict): Input parameters for the actor

        Returns:
            dict: Actor run data (status, datasetId, etc.) or None if failed
        """
        print(f"🚀 Lancement de l'acteur {actor_id}...")
        run_data = None

        try:
            start_response = requests.post(
                f'https://api.apify.com/v2/acts/{actor_id}/runs?token={self.apify_token}',
                json=actor_input
            )
            start_response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

        except RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors du lancement de l'acteur {actor_id}: {e}")
            return None
        except JSONDecodeError as e:
             print(f"❌ Erreur de décodage JSON lors du lancement de l'acteur {actor_id}: {e}")
             return None

        try:
             run_data = start_response.json()
             run_id = run_data.get('data', {}).get('id')
        except JSONDecodeError as e:
             print(f"❌ Erreur de décodage JSON dans la réponse de lancement pour l'acteur {actor_id}: {e}")
             return None

        if not run_id:
            print(f"❌ Impossible de récupérer l'ID du run pour l'acteur {actor_id}.")
            return None

        print(f"⏳ Exécution du run {run_id} en cours...")
        while True:
            try:
                run_status_response = requests.get(
                    f'https://api.apify.com/v2/actor-runs/{run_id}?token={self.apify_token}'
                )
                run_status_response.raise_for_status()

                run_status_data = run_status_response.json().get('data', {})
                status = run_status_data.get('status')

            except RequestException as e:
                print(f"❌ Erreur réseau ou HTTP lors de la vérification du statut du run {run_id}: {e}")
                return None
            except JSONDecodeError as e:
                 print(f"❌ Erreur de décodage JSON dans la réponse de statut pour le run {run_id}: {e}")
                 return None
            except Exception as e:
                 print(f"❌ Erreur inattendue lors de la vérification du statut du run {run_id}: {e}")
                 return None

            print(f"🔄 Statut du run {run_id}: {status}")
            # Break the loop if the run reaches a final state
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                break
            elif status in ['RUNNING', 'READY']:
                 # Still running or ready to run, wait
                 pass
            else:
                # Handle unexpected status values
                print(f"⚠️ Statut du run {run_id} inattendu: {status}. Réessai dans 5s.")

            time.sleep(5)

        if status != 'SUCCEEDED':
            print(f"❌ Échec de l'exécution du run {run_id}. Statut final : {status}")
            return run_status_data

        print(f"✅ Run {run_id} terminé avec succès.")
        return run_status_data

    def _get_dataset_items(self, dataset_id):
        """
        Retrieve items from an Apify dataset.

        Args:
            dataset_id (str): Dataset ID

        Returns:
            list: Dataset items or empty list if failed
        """
        if not dataset_id:
            print("⚠️ L'ID du dataset est manquant. Impossible de récupérer les items.")
            return []

        print(f"⬇️ Récupération des items du dataset {dataset_id}...")
        try:
            dataset_response = requests.get(
                f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.apify_token}&clean=true&format=json'
            )
            dataset_response.raise_for_status()

        except RequestException as e:
            print(f"❌ Erreur réseau ou HTTP lors de la récupération des données du dataset {dataset_id}: {e}")
            return []

        try:
            return dataset_response.json()
        except JSONDecodeError as e:
            print(f"❌ Erreur de décodage JSON lors de la lecture des items du dataset {dataset_id}: {e}")
            return []
        except Exception as e:
            print(f"❌ Erreur inattendue lors de la récupération des items du dataset {dataset_id}: {e}")
            return []

    def _parse_human_readable_number(self, value):
        """Converts numbers like '2.4K', '1M', '1,234' to an integer."""
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip().lower().replace(',', '')
            try:
                if 'k' in value:
                    return int(float(value.replace('k', '')) * 1000)
                if 'm' in value:
                    return int(float(value.replace('m', '')) * 1000000)
                return int(value)
            except (ValueError, TypeError):
                print(f"⚠️ Warning: Could not parse number string: '{value}'. Returning 0.")
                return 0
        # Handles None or other unexpected types gracefully
        if value is not None:
             print(f"⚠️ Warning: Unexpected type for number: {type(value)}. Returning 0.")
        return 0


    def _get_media_type_from_link(self, link_url, thumb_url):
        """
        Determines media type based on the 'link' URL and thumbnail presence.
        Analyzes common Facebook URL patterns.
        """
        if not link_url or not isinstance(link_url, str):
            return "IMAGE" if thumb_url else "STATUS"

        parsed_url = urlparse(link_url)
        if "facebook.com" not in parsed_url.netloc:
            # External link
            return "LINK_WITH_IMAGE" if thumb_url else "LINK"

        path_segments = [segment for segment in parsed_url.path.split('/') if segment]
        query_params = parse_qs(parsed_url.query)

        # Check common Facebook path segments and query params
        if "photo" in path_segments or "photos" in path_segments or "set=a." in parsed_url.query:
            return "PHOTO"
        if "video" in path_segments or "videos" in path_segments or "watch" in path_segments:
            return "VIDEO"
        if "reel" in path_segments:
             return "REEL"
        if "story" in path_segments or "stories" in path_segments:
             return "STORY"
        if "events" in path_segments:
            return "EVENT"
        if "notes" in path_segments:
            return "NOTE"
        # Often posts with single images have 'posts' in URL along with a thumbnail
        if "posts" in path_segments and thumb_url:
             return "IMAGE"

        # Fallback based on thumbnail if URL is ambiguous
        if thumb_url:
            return "IMAGE"

        return "STATUS"


    def _parse_fb_timestamp(self, ts_input_raw, time_str_raw):
        """
        Parses Facebook timestamp (prefers Unix timestamp, falls back to time string).
        Returns ISO 8601 formatted string with UTC timezone.
        """
        # 1. Try Unix timestamp (number)
        if isinstance(ts_input_raw, (int, float)):
            try:
                return datetime.fromtimestamp(float(ts_input_raw), timezone.utc).isoformat()
            except (ValueError, TypeError) as e:
                print(f"⚠️ Warning: Could not parse numeric timestamp '{ts_input_raw}': {e}")

        # 2. Try String format (e.g., "YYYY-MM-DD HH:MM:SS")
        elif isinstance(time_str_raw, str):
            try:
                dt_obj = datetime.strptime(time_str_raw, "%Y-%m-%d %H:%M:%S")
                return dt_obj.replace(tzinfo=timezone.utc).isoformat()
            except ValueError as e:
                print(f"⚠️ Warning: Could not parse string timestamp '{time_str_raw}': {e}")
                # Fallback: Return the raw string if parsing failed, maybe it's another known format?
                return time_str_raw

        # If both fail or inputs are None/wrong type
        if ts_input_raw is not None or time_str_raw is not None:
             print(f"⚠️ Warning: Timestamp inputs '{ts_input_raw}', '{time_str_raw}' were unparsable. Returning None.")

        return None


    def _transform_post(self, post):
        """
        Transform raw post data from easyapi~facebook-posts-search-scraper to standardized format.

        Args:
            post (dict): Raw post data

        Returns:
            dict: Transformed post data or None if input is invalid.
        """
        if not isinstance(post, dict):
             print(f"❌ Transformation Erreur: L'élément n'est pas un dictionnaire: {post}")
             return None

        text_content = post.get("text", "")
        link_url = post.get("link")
        thumb_url = post.get("thumb")

        post_id = post.get("postId")
        page_id = post.get("pageId")
        page_name = post.get("pageName")
        url = post.get("url")
        shares_raw = post.get("shares", 0)
        comments_raw = post.get("comments", 0)
        likes_raw = post.get("likes", 0)
        timestamp_raw = post.get("timestamp")
        time_str_raw = post.get("time")

        created_time_iso = self._parse_fb_timestamp(timestamp_raw, time_str_raw)

        return {
            "post_id": post_id,
            "source_id": page_id, # Assuming pageId is the source ID for search results
            "created_time": created_time_iso,
            "updated_time": None, # Not provided by this actor
            "permalink": url,
            "page_id": page_id,
            "page_name": page_name,
            "message": text_content,
            "media_type": self._get_media_type_from_link(link_url, thumb_url),
            "media_url": link_url,
            "thumbnail_url": thumb_url,
            "can_share": True, # Assuming possible
            "shares": self._parse_human_readable_number(shares_raw),
            "can_comment": True, # Assuming possible
            "comments_count": self._parse_human_readable_number(comments_raw),
            "can_like": True, # Assuming possible
            "like_count": self._parse_human_readable_number(likes_raw),
            "hashtags": re.findall(r"#\w+", text_content) if isinstance(text_content, str) else [],
            "mentions": re.findall(r"@\w+", text_content) if isinstance(text_content, str) else [],
            "caption": text_content, # Using message for caption as no separate field
            "description": "", # Not provided by this actor
            "platform": self.platform,
            "brand_name": self.brand_name,
            "comments": []  # Placeholder to be populated later
        }

    def _transform_comment(self, comment):
        """
        Transform raw comment data from apify~facebook-comments-scraper to standardized format.

        Args:
            comment (dict): Raw comment data

        Returns:
            dict: Transformed comment data or None if input is invalid.
        """
        if not isinstance(comment, dict):
             print(f"❌ Transformation Erreur: L'élément commentaire n'est pas un dictionnaire: {comment}")
             return None

        comment_url = comment.get("commentUrl") or comment.get("url")
        comment_id = None

        # Attempt to extract comment ID from URL if available (common format in comments scraper)
        if comment_url and isinstance(comment_url, str):
            try:
                parsed_url = urlparse(comment_url)
                query_params = parse_qs(parsed_url.query)
                fragment_params = parse_qs(parsed_url.fragment)

                comment_id = query_params.get('comment_id', fragment_params.get('comment_id'))
                if comment_id:
                    comment_id = comment_id[0]

                if not comment_id:
                     comment_id = query_params.get('comment_fbid', fragment_params.get('comment_fbid'))
                     if comment_id:
                         comment_id = comment_id[0]

            except Exception as e:
                 print(f"⚠️ Warning: Could not parse comment ID from URL '{comment_url}': {e}")
                 comment_id = None

        # Fallback to direct IDs from the data if URL parsing failed or URL wasn't present
        comment_id = comment_id or comment.get("commentId") or comment.get("id")
        if comment_id is None:
             # Log issue if ID is still missing after trying multiple sources
             print(f"⚠️ Warning: Could not determine comment ID for comment starting with: {comment.get('text', 'N/A')[:50]}...")


        # Handle different timestamp keys used by actors and parse if necessary
        created_time_raw = comment.get("date") or comment.get("time") or comment.get("created_time")
        created_time_iso = created_time_raw

        if isinstance(created_time_raw, str):
             try:
                 # Apify comments scraper often provides ISO 8601 or "YYYY-MM-DD HH:MM:SS"
                 datetime.fromisoformat(created_time_raw.replace('Z', '+00:00')) # Check if ISO-like
                 # If successful, created_time_iso remains the raw string (which is already ISO)
             except ValueError:
                 # Try parsing "YYYY-MM-DD HH:MM:SS"
                 try:
                      dt_obj = datetime.strptime(created_time_raw, "%Y-%m-%d %H:%M:%S")
                      created_time_iso = dt_obj.replace(tzinfo=timezone.utc).isoformat()
                 except ValueError:
                      print(f"⚠️ Warning: Could not parse comment timestamp string '{created_time_raw}'. Keeping raw string.")
                      created_time_iso = created_time_raw
        elif created_time_raw is not None:
            print(f"⚠️ Warning: Unexpected type for comment timestamp: {type(created_time_raw)}. Keeping raw value.")
            created_time_iso = created_time_raw


        user_data = comment.get("user", {})
        user_id = comment.get("profileId") or comment.get("userId") or user_data.get("id")
        user_name = comment.get("profileName") or comment.get("userName") or user_data.get("name")
        user_url = comment.get("profileUrl") or comment.get("userUrl") or user_data.get("url")

        text = comment.get("text", "")
        likes_count_raw = comment.get("likesCount", 0) or comment.get("likes", 0)
        # Note: `commentsCount` in the comments scraper output usually means replies to this comment
        replies_count_raw = comment.get("commentsCount", 0) or comment.get("replies", 0)


        return {
            "comment_id": comment_id,
            "comment_url": comment_url,
            "user_id": user_id,
            "user_name": user_name,
            "user_url": user_url,
            "created_time": created_time_iso,
            "message": text,
            "like_count": self._parse_human_readable_number(likes_count_raw),
            "reply_count": self._parse_human_readable_number(replies_count_raw),
            "hashtags": re.findall(r"#\w+", text) if isinstance(text, str) else [],
            "mentions": re.findall(r"@\w+", text) if isinstance(text, str) else [],
            "platform": self.platform,
            "brand_name": self.brand_name
        }


    def collect_posts(self):
        """
        Collect posts using the EasyAPI Facebook posts search scraper.

        Returns:
            list: List of transformed posts or empty list on failure/no results.
        """
        print(f"\n--- Début de la collecte des posts ---")
        print(f"📱 Collecte des posts avec la requête: '{self.search_query}'")
        print(f"🔢 Nombre max de posts: {self.max_posts}")

        actor_input = {
            "searchQuery": self.search_query,
            "maxPosts": self.max_posts,
            # Note: postTimeRange parameter is not listed in the easyapi~facebook-posts-search-scraper docs.
            # It might be ignored by this specific actor.
            #"postTimeRange": self.post_time_range,
            "proxyConfig": {"useApifyProxy": True},
        }

        run_data = self._run_apify_actor(self.posts_actor_id, actor_input)
        if not run_data or run_data.get('status') != 'SUCCEEDED':
            print(f"❌ L'acteur de posts a échoué ou n'a pas démarré correctement.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print("❌ Impossible de récupérer l'ID du dataset des posts depuis les données du run.")
            return []

        posts_data = self._get_dataset_items(dataset_id)

        if not posts_data:
            print(f"ℹ️ Aucun post trouvé dans le dataset {dataset_id} pour la requête '{self.search_query}'")
            return []

        posts = []
        print(f"➡️ Transformation de {len(posts_data)} posts bruts...")
        # Debug: print first post structure for inspection
        if posts_data:
            print("\n--- Structure du premier post (debug) ---")
            try:
                print(json.dumps(posts_data[0], indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"Could not serialize first post item for debug print: {e}")
                print(posts_data[0])
            print("---------------------------------------\n")


        for post_item in posts_data:
            try:
                transformed_post = self._transform_post(post_item)
                if transformed_post:
                    posts.append(transformed_post)
                else:
                    print(f"⚠️ Transformation du post {post_item.get('postId', 'Inconnu')} a retourné None.")
            except Exception as e:
                # Catch unexpected errors during transformation
                print(f"❌ Erreur critique lors de la transformation du post {post_item.get('postId', 'Inconnu')}: {type(e).__name__} - {e}")

        print(f"✅ {len(posts)} posts transformés avec succès.")
        return posts

    def collect_comments_for_post(self, post_url):
        """
        Collect comments for a specific post using the standard Apify Facebook comments scraper.

        Args:
            post_url (str): URL of the Facebook post

        Returns:
            list: List of transformed comments or empty list on failure/no results.
        """
        if not post_url or not isinstance(post_url, str):
             print("⚠️ URL du post invalide ou manquante pour la collecte des commentaires.")
             return []

        print(f"\n--- Début de la collecte des commentaires ---")
        print(f"💬 Collecte des commentaires pour : {post_url}")
        print(f"🔢 Nombre max de commentaires par post: {self.max_comments_per_post}")

        # Input format for apify~facebook-comments-scraper
        actor_input = {
            "startUrls": [{"url": post_url}],
            "resultsLimit": self.max_comments_per_post,
            "reactions": True, # Include reaction counts
            # "replies": "enabled", # Note: ResultsLimit usually applies to top-level comments only.
            "proxyConfig": {"useApifyProxy": True},
        }

        run_data = self._run_apify_actor(self.comments_actor_id, actor_input)
        if not run_data or run_data.get('status') != 'SUCCEEDED':
            print(f"❌ L'acteur de commentaires a échoué ou n'a pas démarré correctement pour l'URL {post_url}.")
            return []

        dataset_id = run_data.get('defaultDatasetId')
        if not dataset_id:
            print(f"❌ Impossible de récupérer l'ID du dataset des commentaires depuis les données du run pour l'URL {post_url}.")
            return []

        comments_data = self._get_dataset_items(dataset_id)

        if not comments_data:
            print(f"ℹ️ Aucun commentaire trouvé dans le dataset {dataset_id} pour l'URL {post_url}")
            return []

        comments = []
        print(f"➡️ Transformation de {len(comments_data)} commentaires bruts...")
        # Debug: print first comment structure
        if comments_data:
            print("\n--- Structure du premier commentaire (debug) ---")
            try:
                 print(json.dumps(comments_data[0], indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"Could not serialize first comment item for debug print: {e}")
                print(comments_data[0])
            print("--------------------------------------------\n")


        for comment_item in comments_data:
            try:
                transformed_comment = self._transform_comment(comment_item)
                if transformed_comment:
                    comments.append(transformed_comment)
                else:
                    print(f"⚠️ Transformation du commentaire a retourné None.")

            except Exception as e:
                # Catch unexpected errors during transformation
                print(f"❌ Erreur critique lors de la transformation d'un commentaire : {type(e).__name__} - {e}")

        print(f"✅ {len(comments)} commentaires transformés avec succès pour l'URL {post_url}.")
        return comments

    def collect_all_data(self):
        """
        Collect all posts and their comments.

        Returns:
            list: List of posts with their comments
        """
        print(f"\n=====================================")
        print(f"🔍 Début de la collecte complète pour {self.brand_name}")
        print(f"=====================================")
        print(f"📊 Paramètres: {self.max_posts} posts max, {self.max_comments_per_post} commentaires/post max")
        print(f"🔎 Requête de recherche: '{self.search_query}'")
        print(f"⏰ Période spécifiée: {self.post_time_range} (peut ne pas être supportée par l'acteur de posts)")


        # Collect posts first
        try:
            posts = self.collect_posts()
        except Exception as e:
            print(f"❌ Une erreur inattendue s'est produite lors de la collecte des posts : {type(e).__name__} - {e}")
            return []

        if not posts:
            print("❌ Aucun post collecté ou une erreur s'est produite pendant la collecte des posts.")
            return []

        # Then collect comments for each post
        print(f"\n--- Collecte des commentaires pour {len(posts)} posts ---")
        for i, post in enumerate(posts, 1):
            print(f"\n--- Traitement du post {i}/{len(posts)} (ID: {post.get('post_id', 'Inconnu')}) ---")
            post_url = post.get("permalink")
            if post_url:
                try:
                    comments = self.collect_comments_for_post(post_url)
                    post["comments"] = comments
                except Exception as e:
                    print(f"❌ Une erreur inattendue s'est produite lors de la collecte des commentaires pour l'URL {post_url} : {type(e).__name__} - {e}")
                    post["comments"] = [] # Ensure 'comments' key exists even on error

                # Add a small delay between comment collection runs to be nice to APIs
                if i < len(posts):
                    delay_seconds = 2
                    print(f"⏳ Attente de {delay_seconds} secondes avant le prochain post...")
                    time.sleep(delay_seconds)
            else:
                print("⚠️ Pas de permalink disponible pour ce post. Impossible de collecter les commentaires.")
                post["comments"] = [] # Ensure 'comments' key exists

        print(f"\n=====================================")
        print(f"✅ Collecte complète terminée.")
        print(f"=====================================")

        return posts

