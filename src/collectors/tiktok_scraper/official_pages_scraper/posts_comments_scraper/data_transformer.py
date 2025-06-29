import re
import json


class TikTokDataTransformer:
    """
    Classe pour transformer les données brutes de TikTok en format standardisé.
    """

    def __init__(self, platform="tiktok", brand_name=""):
        """
        Initialize the data transformer.

        Args:
            platform (str): Platform name (default: "tiktok")
            brand_name (str): Brand name associated with the data
        """
        self.platform = platform
        self.brand_name = brand_name

    def transform_post(self, post):
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
                "source_id": author_meta.get("id"),
                "created_time": post.get("createTimeISO"),
                "permalink": post.get("webVideoUrl"),
                "page_id": author_meta.get("id"),
                "page_name": author_meta.get("name"),
                "message": post.get("text", ""),
                "media_type": "photo" if video_meta.get("duration", 0) == 0 else "video",
                "media_url": post.get("webVideoUrl"),
                "thumbnail_url": video_meta.get("coverUrl"),
                "can_share": True,
                "shares": post.get("shareCount", 0),
                "can_comment": True,
                "comments_count": post.get("commentCount", 0),
                "can_like": True,
                "like_count": post.get("diggCount", 0),
                "playCount": post.get("playCount"),
                "hashtags": post.get("hashtags", []),
                "mentions": post.get("mentions", []),
                "caption": post.get("text", ""),
                "description": post.get("desc", ""),
                "platform": self.platform,
                "brand_name": self.brand_name,
                "comments": []
            }

            # Basic validation for essential fields
            if not transformed.get("post_id"):
                print(f"⚠️ Transformation post échouée: ID manquant pour l'item raw: {json.dumps(post)[:200]}...")
                return None
            if not transformed.get("permalink"):
                print(f"⚠️ Transformation post: permalink manquant pour post {transformed['post_id']}.")
                return None
            if not transformed.get("created_time"):
                print(f"⚠️ Transformation post: created_time manquant pour post {transformed['post_id']}.")

            return transformed

        except Exception as e:
            post_id_safe = post.get('id', 'Inconnu')
            print(f"❌ Erreur inattendue lors de la transformation du post (ID raw: {post_id_safe}): {e}")
            return None

    def transform_comment(self, comment):
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
            user_info = comment.get("user", {})
            user_unique_id = user_info.get("uniqueId")

            transformed = {
                "comment_id": comment.get("cid"),
                "comment_url": comment.get("commentUrl", ""),
                "user_id": user_info.get("uid"),
                "user_name": user_unique_id,
                "user_url": f"https://www.tiktok.com/@{user_unique_id}/" if user_unique_id else None,
                "created_time": comment.get("createTimeISO"),
                "message": comment.get("text", ""),
                "like_count": comment.get("diggCount", 0),
                "reply_count": comment.get("replyCommentTotal", 0),
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
            comment_id_safe = comment.get('cid', 'Inconnu')
            print(f"❌ Erreur inattendue lors de la transformation du commentaire (ID raw: {comment_id_safe}): {e}")
            return None

    def transform_posts_batch(self, posts_data):
        """
        Transform a batch of raw posts data.

        Args:
            posts_data (list): List of raw posts data

        Returns:
            list: List of transformed posts (excluding failed transformations)
        """
        if not isinstance(posts_data, list):
            print("⚠️ Les données de posts doivent être une liste.")
            return []

        transformed_posts = [self.transform_post(post) for post in posts_data]
        posts = [post for post in transformed_posts if post is not None]

        print(f"✅ {len(posts)} posts transformés avec succès (sur {len(posts_data)} items bruts).")
        if len(posts) == 0 and len(posts_data) > 0:
            print("⚠️ Aucun post n'a pu être transformé. Vérifiez la structure des données brutes.")

        return posts

    def transform_comments_batch(self, comments_data):
        """
        Transform a batch of raw comments data.

        Args:
            comments_data (list): List of raw comments data

        Returns:
            list: List of transformed comments (excluding failed transformations)
        """
        if not isinstance(comments_data, list):
            print("⚠️ Les données de commentaires doivent être une liste.")
            return []

        transformed_comments = [self.transform_comment(comment) for comment in comments_data]
        comments = [comment for comment in transformed_comments if comment is not None]

        print(f"✅ {len(comments)} commentaires transformés avec succès (sur {len(comments_data)} items bruts).")
        if len(comments) == 0 and len(comments_data) > 0:
            print("⚠️ Aucun commentaire n'a pu être transformé. Vérifiez la structure des données brutes.")

        return comments