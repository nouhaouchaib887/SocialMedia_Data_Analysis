import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Dict, List, Optional, Any


class FacebookDataTransformer:
    """
    Transforme les données brutes de Facebook en format standardisé.
    """

    def __init__(self, brand_name: str, page_name: str, platform: str = "facebook"):
        """
        Initialise le transformateur de données.

        Args:
            brand_name (str): Nom de la marque
            page_name (str): Nom de la page Facebook
            platform (str): Nom de la plateforme (par défaut "facebook")
        """
        self.brand_name = brand_name
        self.page_name = page_name
        self.platform = platform

    def transform_post(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transforme un post brut en format standardisé.

        Args:
            post (dict): Données brutes du post

        Returns:
            dict: Post transformé ou None si échec
        """
        try:
            if not self._validate_post_data(post):
                return None

            media_info = self._extract_media_info(post.get("media"))
            timestamp = post["timestamp"]
            
            if not isinstance(timestamp, (int, float)):
                print(f"⚠️ Post skipped: Invalid timestamp format {timestamp}")
                return None

            created_time_iso = datetime.utcfromtimestamp(timestamp).isoformat()
            post_id = post["postId"]
            permalink = f"https://www.facebook.com/{self.page_name}/posts/{post_id}"

            transformed_post = {
                "post_id": post_id,
                "source_id": post["user"]["id"],
                "created_time": created_time_iso,
                "permalink": permalink,
                "page_id": post["user"]["id"],
                "page_name": self.page_name,
                "message": post.get("text", ""),
                "media_type": media_info["type"],
                "media_url": media_info["url"],
                "thumbnail_url": media_info["thumbnail"],
                "can_share": True,
                "shares": post.get("shares", 0),
                "can_comment": True,
                "comments_count": post.get("comments", 0),
                "can_like": True,
                "like_count": post.get("likes", 0),
                "hashtags": self._extract_hashtags(post.get("text", "")),
                "mentions": self._extract_mentions(post.get("text", "")),
                "platform": self.platform,
                "brand_name": self.brand_name,
                "comments": []
            }

            return transformed_post

        except Exception as e:
            print(f"❌ Erreur lors de la transformation du post (ID: {post.get('postId', 'N/A')}): {e}")
            return None

    def transform_comment(self, comment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transforme un commentaire brut en format standardisé.

        Args:
            comment (dict): Données brutes du commentaire

        Returns:
            dict: Commentaire transformé ou None si échec
        """
        try:
            if not self._validate_comment_data(comment):
                return None

            comment_url = comment.get("commentUrl")
            comment_id = self._extract_comment_id(comment_url)

            transformed_comment = {
                "comment_id": comment_id,
                "comment_url": comment_url,
                "user_id": comment.get("profileId"),
                "user_name": comment.get("profileName"),
                "user_url": comment.get("profileUrl"),
                "created_time": comment.get("date"),
                "message": comment.get("text", ""),
                "like_count": comment.get("likesCount", 0),
                "reply_count": comment.get("commentsCount", 0),
                "hashtags": self._extract_hashtags(comment.get("text", "")),
                "mentions": self._extract_mentions(comment.get("text", "")),
                "platform": self.platform,
                "brand_name": self.brand_name
            }

            return transformed_comment

        except Exception as e:
            print(f"❌ Erreur lors de la transformation du commentaire (URL: {comment.get('commentUrl', 'N/A')}): {e}")
            return None

    def transform_posts_batch(self, posts_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforme une liste de posts bruts.

        Args:
            posts_data (list): Liste des données brutes de posts

        Returns:
            list: Liste des posts transformés (exclut les échecs)
        """
        transformed_posts = []
        
        for post in posts_data:
            transformed_post = self.transform_post(post)
            if transformed_post:
                transformed_posts.append(transformed_post)

        print(f"✅ {len(posts_data)} posts bruts reçus. {len(transformed_posts)} posts transformés avec succès.")
        return transformed_posts

    def transform_comments_batch(self, comments_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforme une liste de commentaires bruts.

        Args:
            comments_data (list): Liste des données brutes de commentaires

        Returns:
            list: Liste des commentaires transformés (exclut les échecs)
        """
        transformed_comments = []
        
        for comment in comments_data:
            transformed_comment = self.transform_comment(comment)
            if transformed_comment:
                transformed_comments.append(transformed_comment)

        return transformed_comments

    def _validate_post_data(self, post: Dict[str, Any]) -> bool:
        """Valide les données essentielles d'un post."""
        required_keys = ['postId', 'timestamp', 'user']
        
        if not isinstance(post, dict):
            print(f"⚠️ Post skipped: Not a dictionary")
            return False
            
        for key in required_keys:
            if key not in post:
                print(f"⚠️ Post skipped: Missing key '{key}'")
                return False
                
        if 'id' not in post.get('user', {}):
            print(f"⚠️ Post skipped: Missing user id")
            return False
            
        return True

    def _validate_comment_data(self, comment: Dict[str, Any]) -> bool:
        """Valide les données essentielles d'un commentaire."""
        required_keys = ['commentUrl', 'profileId']
        
        if not isinstance(comment, dict):
            print(f"⚠️ Comment skipped: Not a dictionary")
            return False
            
        for key in required_keys:
            if key not in comment:
                print(f"⚠️ Comment skipped: Missing key '{key}'")
                return False
                
        return True

    def _extract_media_info(self, media: Any) -> Dict[str, str]:
        """Extrait les informations média d'un post."""
        media_info = {"type": "", "url": "", "thumbnail": ""}
        
        if isinstance(media, dict):
            media_info["type"] = media.get("__typename", "")
            media_info["url"] = media.get("url", "")
            media_info["thumbnail"] = media.get("thumbnail", "")
        elif isinstance(media, list) and media:
            first_media = media[0]
            if isinstance(first_media, dict):
                media_info["type"] = first_media.get("__typename", "")
                media_info["url"] = first_media.get("url", "")
                media_info["thumbnail"] = first_media.get("thumbnail", "")
                
        return media_info

    def _extract_comment_id(self, comment_url: str) -> Optional[str]:
        """Extrait l'ID du commentaire depuis son URL."""
        if not comment_url:
            return None
            
        try:
            parsed_url = urlparse(comment_url)
            query_params = parse_qs(parsed_url.query)
            return query_params.get('comment_id', [None])[0]
        except Exception as e:
            print(f"⚠️ Could not parse comment_id from URL {comment_url}: {e}")
            return None

    def _extract_hashtags(self, text: str) -> List[str]:
        """Extrait les hashtags d'un texte."""
        if not text:
            return []
        return re.findall(r"#(\w+)", text)

    def _extract_mentions(self, text: str) -> List[str]:
        """Extrait les mentions d'un texte."""
        if not text:
            return []
        return re.findall(r"@(\w+)", text)