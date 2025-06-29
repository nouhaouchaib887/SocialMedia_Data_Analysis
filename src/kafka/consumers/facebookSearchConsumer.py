import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient
from src.kafka.consumers.postCommentRepository import PostCommentRepository
from intelligentAnalysis.externalContentAnalyzer import ExternalContentAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExternalContentConsumer:
    """Consumer final, qui sauvegarde les données enrichies en respectant le schéma."""

    def __init__(self, kafka_config: Dict[str, Any], mongodb_config: Dict[str, Any], google_api_key: str):
        # ... (constructeur inchangé)
        self.kafka_config = kafka_config
        self.mongodb_config = mongodb_config
        self.google_api_key = google_api_key
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.repository: Optional[PostCommentRepository] = None
        self.analyzer: Optional[ExternalContentAnalyzer] = None
        self._is_running = False
        self.OFFICIAL_PAGE_IDS = {
            "100064944541507", "100050531557552", "100064662374992", 
            "100069357321773", "100070238800377c", "100064413743218"
        }
        logger.info("ExternalContentConsumer initialisé.")

    async def initialize(self):
        # ... (méthode initialize inchangée)
        logger.info("Initialisation des connexions et dépendances...")
        if not self.google_api_key:
            raise ValueError("La clé API Google est manquante.")
        self.analyzer = ExternalContentAnalyzer(
            google_api_key=self.google_api_key,
            topics_file="config/themes/general_themes.json",
            intents_post_file="config/themes/posts_intents.json",
            intents_comments_file="config/themes/comments_intents.json",
            prompts_file="config/prompts/search_prompts.yaml"
        )
        self.mongo_client = AsyncIOMotorClient(self.mongodb_config['connection_string'])
        db = self.mongo_client[self.mongodb_config['database']]
        self.repository = PostCommentRepository(db)
        await self.repository.ensure_indexes()
        self.consumer = AIOKafkaConsumer(
            self.kafka_config['topic'],
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            group_id=self.kafka_config['group_id'],
            auto_offset_reset='earliest'
        )
        logger.info("Connexions et dépendances initialisées.")

    async def _process_message(self, msg: Any):
        # ... (méthode _process_message inchangée)
        try:
            message_data = json.loads(msg.value.decode('utf-8'))
        except json.JSONDecodeError:
            logger.error(f"Impossible de décoder le message JSON: {msg.value}")
            return

        post_id = message_data.get("post_id")
        if not post_id:
            logger.warning("Message ignoré : 'post_id' manquant.")
            return
            
        logger.info(f"--- Début du traitement pour le post ID: {post_id} ---")
        
        page_id = message_data.get("page_id")
        if page_id and str(page_id) in self.OFFICIAL_PAGE_IDS:
            logger.info(f"Post {post_id} ignoré car il provient d'une page officielle (Page ID: {page_id}).")
            return
            
        existing_post = await self.repository.find_post_by_id(post_id)
        if existing_post:
            await self._handle_existing_post(post_id, message_data, existing_post)
        else:
            await self._handle_new_post(post_id, message_data)
            
        logger.info(f"--- Traitement terminé pour le post ID: {post_id} ---")

    async def _analyze_and_save_comment(self, comment_data: dict, post_id: str, post_content: str, post_analysis: dict):
        """Analyse un seul commentaire, le formate et le sauvegarde."""
        comment_content = comment_data.get('message')
        if not comment_content:
            return

        comment_analysis = await self.analyzer.analyze_content("comment", comment_content, post_content, post_analysis)
        
        original_comment_time_str = comment_data.get('created_time')
        comment_created_time_obj = datetime.now(timezone.utc)
        if original_comment_time_str:
            try:
                comment_created_time_obj = datetime.fromisoformat(original_comment_time_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                logger.warning(f"Format de date invalide pour le commentaire {comment_data.get('comment_id')}: '{original_comment_time_str}'. Utilisation de now().")
        
        comment_document_to_insert = {
            "comment_id": comment_data.get("comment_id"), "comment_url": comment_data.get("comment_url"),
            "user_id": comment_data.get("user_id"), "user_name": comment_data.get("user_name"),
            "user_url": comment_data.get("user_url"), "created_time": comment_created_time_obj,
            "message": comment_content, "like_count": comment_data.get("like_count", 0),
            "reply_count": comment_data.get("reply_count", 0), "hashtags": comment_data.get("hashtags", []),
            "mentions": comment_data.get("mentions", []), "platform": comment_data.get("platform"),
            "brand_name": comment_data.get("brand_name"), "parent_post_id": post_id,
            "comment_analysis": comment_analysis
        }
        
        await self.repository.insert_comment(comment_document_to_insert)
        logger.info(f"Nouveau commentaire {comment_data.get('comment_id')} analysé et sauvegardé pour le post {post_id}.")

    async def _handle_new_post(self, post_id: str, post_data: dict):
        logger.info(f"Nouveau post détecté (ID: {post_id}). Analyse complète.")
        post_content = post_data.get("message") or post_data.get("caption")
        if not post_content:
            logger.warning(f"Post {post_id} ignoré : aucun contenu textuel.")
            return

        post_analysis = await self.analyzer.analyze_content("post", post_content)
        if not post_analysis.get("relevance", {}).get("general_relevance"):
            logger.info(f"Post {post_id} jugé non pertinent. Sauvegarde du post et de ses commentaires annulée.")
            return
        
        logger.info(f"Post {post_id} pertinent. Préparation pour la sauvegarde.")
        
        original_created_time_str = post_data.get('created_time')
        created_time_obj = datetime.now(timezone.utc)
        if original_created_time_str:
            try:
                created_time_obj = datetime.fromisoformat(original_created_time_str.replace('Z', '+00:00').replace('+0000', '+00:00'))
            except (ValueError, TypeError):
                logger.warning(f"Impossible de parser la date du post '{original_created_time_str}'. Utilisation de la date actuelle.")

        post_document_to_insert = {k: v for k, v in post_data.items() if k not in ['comments', 'kafka_metadata', 'collection_params']}
        post_document_to_insert['post_analysis'] = post_analysis
        post_document_to_insert['created_time'] = created_time_obj
        post_document_to_insert['updated_time'] = None
        await self.repository.insert_post(post_document_to_insert)

        for comment_data in post_data.get("comments", []):
            await self._analyze_and_save_comment(comment_data, post_id, post_content, post_analysis)

    async def _handle_existing_post(self, post_id: str, new_data: dict, existing_post: dict):
        logger.info(f"Post existant (ID: {post_id}). Mise à jour des métriques et vérification des nouveaux commentaires.")
        
        metric_keys = ['likes_count', 'shares_count', 'comments_count', 'reactions', 'views_count']
        fields_to_update = {}
        for key in metric_keys:
            if key in new_data:
                fields_to_update[key] = new_data[key]
        
        if len(fields_to_update) > 0:
            fields_to_update['updated_time'] = datetime.now(timezone.utc)
            await self.repository.posts_collection.update_one(
                {"post_id": post_id},
                {"$set": fields_to_update}
            )
            logger.info(f"Métriques du post {post_id} mises à jour.")
        else:
            logger.info(f"Aucune nouvelle métrique à mettre à jour pour le post {post_id}.")

        incoming_comments = new_data.get("comments", [])
        if not incoming_comments:
            return

        post_content = existing_post.get("message") or existing_post.get("caption")
        post_analysis = existing_post.get("post_analysis")

        if not post_content or not post_analysis:
            logger.warning(f"Impossible de traiter les commentaires pour le post {post_id} car le contenu/analyse du post parent est manquant.")
            return

        incoming_comment_ids = [c.get("comment_id") for c in incoming_comments if c.get("comment_id")]
        if not incoming_comment_ids:
            return

        cursor = self.repository.comments_collection.find(
            {"comment_id": {"$in": incoming_comment_ids}},
            {"comment_id": 1}
        )
        existing_docs = await cursor.to_list(length=None)
        existing_comment_ids_set = {doc['comment_id'] for doc in existing_docs}
        
        logger.info(f"{len(existing_comment_ids_set)} commentaire(s) déjà existant(s) sur {len(incoming_comment_ids)} reçu(s).")

        new_comments_found = 0
        for comment_data in incoming_comments:
            comment_id = comment_data.get("comment_id")
            if comment_id and comment_id not in existing_comment_ids_set:
                new_comments_found += 1
                await self._analyze_and_save_comment(comment_data, post_id, post_content, post_analysis)

        if new_comments_found == 0:
            logger.info(f"Aucun nouveau commentaire à ajouter pour le post {post_id}.")
            
    # ... le reste du code (run, stop, main) est inchangé ...
    async def run(self):
        await self.initialize()
        await self.consumer.start()
        self._is_running = True
        logger.info(f"Consumer démarré. Écoute sur le topic '{self.kafka_config['topic']}'.")
        try:
            async for msg in self.consumer:
                await self._process_message(msg)
        finally:
            await self.stop()

    async def stop(self):
        if not self._is_running: return
        logger.info("Arrêt du consumer...")
        self._is_running = False
        if self.consumer: await self.consumer.stop()
        if self.mongo_client: self.mongo_client.close()
        logger.info("Consumer arrêté.")

async def main():
    """Point d'entrée principal utilisant VOTRE configuration."""
    
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'facebook-data-consumer-group-3',
        'topic': 'facebook-search-data-test',
        'auto_offset_reset': 'earliest',
    }
    mongodb_config = {
        'connection_string': 'mongodb://localhost:27017/',
        'database': 'social_media_db_test'
    }
    
    # Remplacez par votre clé API ou chargez-la depuis les variables d'environnement
    google_api_key = "AIzaSyCSF8QXA4bIxQvdCUlFWZqzGOxYRM6fJKM" 
    
    if not google_api_key or "VOTRE_CLE_API" in google_api_key:
        logger.critical("GOOGLE_API_KEY non définie ou invalide. Arrêt.")
        return

    consumer = ExternalContentConsumer(kafka_config, mongodb_config, google_api_key)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    try:
        await consumer.run()
    except Exception as e:
        logger.critical(f"Erreur au démarrage: {e}", exc_info=True)
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())