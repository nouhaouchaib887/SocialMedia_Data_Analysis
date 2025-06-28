import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional


from motor.motor_asyncio import AsyncIOMotorDatabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostCommentRepository:
    """Gère les interactions avec MongoDB pour les données Facebook"""

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.posts_collection = self.db['posts']
        self.comments_collection = self.db['comments']
        logger.info("FacebookRepository initialized")

    async def ensure_indexes(self):
        """Assure la création des index nécessaires dans MongoDB"""
        try:
            await self.posts_collection.create_index("post_id", unique=True)
            await self.comments_collection.create_index("comment_id", unique=True)
            await self.posts_collection.create_index("updated_time")
            logger.info("MongoDB indexes ensured")
        except Exception as e:
            logger.error(f"Erreur lors de la création des index: {e}")
            raise # Re-raise the exception

    async def find_post_by_id(self, post_id: str) -> Optional[Dict[str, Any]]:
        """Trouve un post par son ID"""
        try:
            return await self.posts_collection.find_one({"post_id": post_id})
        except Exception as e:
            logger.error(f"Erreur lors de la recherche du post {post_id}: {e}")
            return None # Or raise, depending on desired error handling

    async def insert_post(self, post_document: Dict[str, Any]):
        """Insère un nouveau post dans la base"""
        try:
            result = await self.posts_collection.insert_one(post_document)
            logger.info(f"Post inséré avec _id: {result.inserted_id}")
        except Exception as e:
            logger.error(f"Erreur lors de l'insertion du post {post_document.get('post_id')}: {e}")
            # Consider handling DuplicateKeyError specifically if needed

    async def update_post_metrics(self, post_id: str, metrics: Dict[str, Any]):
        """Met à jour les métriques d'un post existant"""
        try:
            update_doc = {
                "$set": {
                    "shares": metrics.get("shares", 0),
                    "comments_count": metrics.get("comments_count", 0),
                    "like_count": metrics.get("like_count", 0),
                    "updated_time": datetime.utcnow().isoformat() # Use UTC
                }
            }
            result = await self.posts_collection.update_one(
                {"post_id": post_id},
                update_doc
            )
            if result.modified_count > 0:
                logger.info(f"Métriques mises à jour pour le post {post_id}")
            else:
                logger.warning(f"Aucune mise à jour effectuée pour le post {post_id}. Le post pourrait ne pas exister ou les métriques étaient identiques.")

        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour des métriques du post {post_id}: {e}")

    async def find_comment_by_id(self, comment_id: str) -> Optional[Dict[str, Any]]:
        """Trouve un commentaire par son ID"""
        try:
            return await self.comments_collection.find_one({"comment_id": comment_id})
        except Exception as e:
            logger.error(f"Erreur lors de la recherche du commentaire {comment_id}: {e}")
            return None

    async def insert_comment(self, comment_document: Dict[str, Any]):
        """Insère un nouveau commentaire dans la base"""
        try:
            result = await self.comments_collection.insert_one(comment_document)
            logger.info(f"Commentaire inséré avec _id: {result.inserted_id}")
        except Exception as e:
            logger.error(f"Erreur lors de l'insertion du commentaire {comment_document.get('comment_id')}: {e}")