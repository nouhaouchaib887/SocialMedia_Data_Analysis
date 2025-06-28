import re
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs


class FacebookDataTransformer:
    """
    Transformer pour convertir les données brutes de Facebook en format standardisé.
    """
    
    def __init__(self, brand_name, platform="facebook"):
        """
        Initialize the transformer.
        
        Args:
            brand_name (str): Brand name for identification
            platform (str): Platform identifier
        """
        if not brand_name:
            raise ValueError("Brand name cannot be None or empty.")
        
        self.brand_name = brand_name
        self.platform = platform
    
    def _parse_human_readable_number(self, value):
        """
        Converts numbers like '2.4K', '1M', '1,234' to an integer.
        
        Args:
            value: Value to parse (int, float, str, or None)
            
        Returns:
            int: Parsed number or 0 if parsing fails
        """
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
        
        Args:
            link_url (str): Link URL from the post
            thumb_url (str): Thumbnail URL from the post
            
        Returns:
            str: Media type classification
        """
        if not link_url or not isinstance(link_url, str):
            return "IMAGE" if thumb_url else "STATUS"
        
        parsed_url = urlparse(link_url)
        if "facebook.com" not in parsed_url.netloc:
            # External link
            return "LINK_WITH_IMAGE" if thumb_url else "LINK"
        
        path_segments = [segment for segment in parsed_url.path.split('/') if segment]
        
        # Check common Facebook path segments
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
        if "posts" in path_segments and thumb_url:
            return "IMAGE"
        
        # Fallback based on thumbnail
        if thumb_url:
            return "IMAGE"
        
        return "STATUS"
    
    def _parse_fb_timestamp(self, ts_input_raw, time_str_raw):
        """
        Parses Facebook timestamp (prefers Unix timestamp, falls back to time string).
        
        Args:
            ts_input_raw: Raw timestamp (numeric or string)
            time_str_raw: Raw time string
            
        Returns:
            str: ISO 8601 formatted string with UTC timezone or None
        """
        # Try Unix timestamp (number)
        if isinstance(ts_input_raw, (int, float)):
            try:
                return datetime.fromtimestamp(float(ts_input_raw), timezone.utc).isoformat()
            except (ValueError, TypeError) as e:
                print(f"⚠️ Warning: Could not parse numeric timestamp '{ts_input_raw}': {e}")
        
        # Try String format (e.g., "YYYY-MM-DD HH:MM:SS")
        elif isinstance(time_str_raw, str):
            try:
                dt_obj = datetime.strptime(time_str_raw, "%Y-%m-%d %H:%M:%S")
                return dt_obj.replace(tzinfo=timezone.utc).isoformat()
            except ValueError as e:
                print(f"⚠️ Warning: Could not parse string timestamp '{time_str_raw}': {e}")
                return time_str_raw
        
        # If both fail or inputs are None/wrong type
        if ts_input_raw is not None or time_str_raw is not None:
            print(f"⚠️ Warning: Timestamp inputs '{ts_input_raw}', '{time_str_raw}' were unparsable. Returning None.")
        
        return None
    
    def _extract_hashtags_and_mentions(self, text):
        """
        Extract hashtags and mentions from text.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            tuple: (hashtags_list, mentions_list)
        """
        if not isinstance(text, str):
            return [], []
        
        hashtags = re.findall(r"#\w+", text)
        mentions = re.findall(r"@\w+", text)
        
        return hashtags, mentions
    
    def transform_post(self, post_data):
        """
        Transform raw post data to standardized format.
        
        Args:
            post_data (dict): Raw post data from API
            
        Returns:
            dict: Transformed post data or None if invalid
        """
        if not isinstance(post_data, dict):
            print(f"❌ Transformation Erreur: L'élément n'est pas un dictionnaire: {post_data}")
            return None
        
        # Extract basic data
        text_content = post_data.get("text", "")
        link_url = post_data.get("link")
        thumb_url = post_data.get("thumb")
        post_id = post_data.get("postId")
        page_id = post_data.get("pageId")
        page_name = post_data.get("pageName")
        url = post_data.get("url")
        
        # Parse engagement metrics
        shares_raw = post_data.get("shares", 0)
        comments_raw = post_data.get("comments", 0)
        likes_raw = post_data.get("likes", 0)
        
        # Parse timestamps
        timestamp_raw = post_data.get("timestamp")
        time_str_raw = post_data.get("time")
        created_time_iso = self._parse_fb_timestamp(timestamp_raw, time_str_raw)
        
        # Extract hashtags and mentions
        hashtags, mentions = self._extract_hashtags_and_mentions(text_content)
        
        return {
            "post_id": post_id,
            "source_id": page_id,
            "created_time": created_time_iso,
            "updated_time": None,
            "permalink": url,
            "page_id": page_id,
            "page_name": page_name,
            "message": text_content,
            "media_type": self._get_media_type_from_link(link_url, thumb_url),
            "media_url": link_url,
            "thumbnail_url": thumb_url,
            "can_share": True,
            "shares": self._parse_human_readable_number(shares_raw),
            "can_comment": True,
            "comments_count": self._parse_human_readable_number(comments_raw),
            "can_like": True,
            "like_count": self._parse_human_readable_number(likes_raw),
            "hashtags": hashtags,
            "mentions": mentions,
            "caption": text_content,
            "description": "",
            "platform": self.platform,
            "brand_name": self.brand_name,
            "comments": []  # Placeholder to be populated later
        }
    
    def transform_comment(self, comment_data):
        """
        Transform raw comment data to standardized format.
        
        Args:
            comment_data (dict): Raw comment data from API
            
        Returns:
            dict: Transformed comment data or None if invalid
        """
        if not isinstance(comment_data, dict):
            print(f"❌ Transformation Erreur: L'élément commentaire n'est pas un dictionnaire: {comment_data}")
            return None
        
        # Extract comment URL and try to parse ID
        comment_url = comment_data.get("commentUrl") or comment_data.get("url")
        comment_id = self._extract_comment_id(comment_url, comment_data)
        
        # Parse timestamp
        created_time_raw = comment_data.get("date") or comment_data.get("time") or comment_data.get("created_time")
        created_time_iso = self._parse_comment_timestamp(created_time_raw)
        
        # Extract user data
        user_data = comment_data.get("user", {})
        user_id = comment_data.get("profileId") or comment_data.get("userId") or user_data.get("id")
        user_name = comment_data.get("profileName") or comment_data.get("userName") or user_data.get("name")
        user_url = comment_data.get("profileUrl") or comment_data.get("userUrl") or user_data.get("url")
        
        # Extract text and engagement
        text = comment_data.get("text", "")
        likes_count_raw = comment_data.get("likesCount", 0) or comment_data.get("likes", 0)
        replies_count_raw = comment_data.get("commentsCount", 0) or comment_data.get("replies", 0)
        
        # Extract hashtags and mentions
        hashtags, mentions = self._extract_hashtags_and_mentions(text)
        
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
            "hashtags": hashtags,
            "mentions": mentions,
            "platform": self.platform,
            "brand_name": self.brand_name
        }
    
    def _extract_comment_id(self, comment_url, comment_data):
        """
        Extract comment ID from URL or fallback to data fields.
        
        Args:
            comment_url (str): Comment URL
            comment_data (dict): Comment data dictionary
            
        Returns:
            str: Comment ID or None
        """
        comment_id = None
        
        # Try to extract from URL
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
        
        # Fallback to direct IDs from data
        comment_id = comment_id or comment_data.get("commentId") or comment_data.get("id")
        
        if comment_id is None:
            print(f"⚠️ Warning: Could not determine comment ID for comment starting with: {comment_data.get('text', 'N/A')[:50]}...")
        
        return comment_id
    
    def _parse_comment_timestamp(self, created_time_raw):
        """
        Parse comment timestamp from various formats.
        
        Args:
            created_time_raw: Raw timestamp from comment data
            
        Returns:
            str: Parsed timestamp or raw value
        """
        if not isinstance(created_time_raw, str):
            if created_time_raw is not None:
                print(f"⚠️ Warning: Unexpected type for comment timestamp: {type(created_time_raw)}")
            return created_time_raw
        
        try:
            # Check if ISO-like format
            datetime.fromisoformat(created_time_raw.replace('Z', '+00:00'))
            return created_time_raw
        except ValueError:
            # Try parsing "YYYY-MM-DD HH:MM:SS"
            try:
                dt_obj = datetime.strptime(created_time_raw, "%Y-%m-%d %H:%M:%S")
                return dt_obj.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                print(f"⚠️ Warning: Could not parse comment timestamp string '{created_time_raw}'. Keeping raw string.")
                return created_time_raw
    
    def transform_posts_batch(self, posts_data):
        """
        Transform a batch of posts data.
        
        Args:
            posts_data (list): List of raw posts data
            
        Returns:
            list: List of transformed posts
        """
        if not posts_data:
            return []
        
        print(f"➡️ Transformation de {len(posts_data)} posts bruts...")
        
        # Debug: print first post structure
        if posts_data:
            print("\n--- Structure du premier post (debug) ---")
            try:
                print(json.dumps(posts_data[0], indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"Could not serialize first post item for debug print: {e}")
                print(posts_data[0])
            print("---------------------------------------\n")
        
        transformed_posts = []
        for post_item in posts_data:
            try:
                transformed_post = self.transform_post(post_item)
                if transformed_post:
                    transformed_posts.append(transformed_post)
                else:
                    print(f"⚠️ Transformation du post {post_item.get('postId', 'Inconnu')} a retourné None.")
            except Exception as e:
                print(f"❌ Erreur critique lors de la transformation du post {post_item.get('postId', 'Inconnu')}: {type(e).__name__} - {e}")
        
        print(f"✅ {len(transformed_posts)} posts transformés avec succès.")
        return transformed_posts
    
    def transform_comments_batch(self, comments_data, post_url=""):
        """
        Transform a batch of comments data.
        
        Args:
            comments_data (list): List of raw comments data
            post_url (str): URL of the post (for debugging)
            
        Returns:
            list: List of transformed comments
        """
        if not comments_data:
            return []
        
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
        
        transformed_comments = []
        for comment_item in comments_data:
            try:
                transformed_comment = self.transform_comment(comment_item)
                if transformed_comment:
                    transformed_comments.append(transformed_comment)
                else:
                    print(f"⚠️ Transformation du commentaire a retourné None.")
            except Exception as e:
                print(f"❌ Erreur critique lors de la transformation d'un commentaire : {type(e).__name__} - {e}")
        
        print(f"✅ {len(transformed_comments)} commentaires transformés avec succès" + (f" pour l'URL {post_url}" if post_url else "") + ".")
        return transformed_comments