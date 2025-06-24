import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Assume these imports exist and ContentAnalyzer is defined elsewhere
from kafka import KafkaConsumer
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from PostCommentRepository import PostCommentRepository
from IntelligentAnalysis.ContentAnalyzer import ContentAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FacebookDataConsumer:
    """Consumer Kafka pour traiter les données Facebook"""

    def __init__(self, kafka_config: Dict[str, Any], mongodb_config: Dict[str, Any],
                 google_api_key: str):
        self.kafka_config = kafka_config
        self.mongodb_config = mongodb_config
        self.google_api_key = google_api_key

        # Initialisation des clients/dependencies
        self.consumer: Optional[KafkaConsumer] = None
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.repository: Optional[PostCommentRepository] = None
        # ContentAnalyzer will be instantiated when needed or potentially managed differently

        logger.info("FacebookDataConsumer initialized with configs")

    async def initialize(self):
        """Initialise les connexions Kafka et MongoDB et les dépendances"""
        try:
            # Initialisation du consumer Kafka
            self.consumer = KafkaConsumer(
                self.kafka_config.get('topic', 'facebook-data'), # Use get with default
                bootstrap_servers=self.kafka_config.get('bootstrap_servers'), # Use get
                auto_offset_reset=self.kafka_config.get('auto_offset_reset', 'earliest'), # Use get
                enable_auto_commit=self.kafka_config.get('enable_auto_commit', True), # Use get
                group_id=self.kafka_config.get('group_id', 'facebook-data-consumer'), # Use get
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # consumer_timeout_ms=1000 # poll() handles timeouts
            )
            logger.info("Kafka Consumer initialized")

            # Initialisation du client MongoDB
            self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
                self.mongodb_config['connection_string'] # connection_string is essential
            )
            db = self.mongo_client[self.mongodb_config.get('database', 'facebook_analytics')] # Use get

            # Initialisation du repository
            self.repository =   PostCommentRepository(db)
            await self.repository.ensure_indexes()

            logger.info("Connexions et repository initialisés avec succès")

        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation: {e}")
            # Ensure cleanup if initialization fails partially? Or let run() handle it.
            raise

    async def should_update_post_metrics(self, existing_post: Dict[str, Any]) -> bool:
        """
        Vérifie si les métriques du post doivent être mises à jour
        (Business logic - can stay or be moved depending on complexity)
        """
        if not existing_post.get('updated_time'):
            return True # Always update if no update time recorded

        try:
            # Parse updated_time, handling potential timezone issues or lack of 'Z'
            # Using fromisoformat and handling Z is a good approach
            updated_time_str = existing_post['updated_time']
            if updated_time_str.endswith('Z'):
                updated_time_str = updated_time_str[:-1] + '+00:00'
            updated_time = datetime.fromisoformat(updated_time_str)

            # Get current time with timezone info if available, or assume UTC
            # Best practice is to work in UTC
            current_time = datetime.utcnow()

            # Ensure both datetimes are timezone-aware or naive for comparison
            # If updated_time is fromisoformat without Z/+00:00, it's naive.
            # Let's standardize to UTC.
            if updated_time.tzinfo is None:
                 # Assume stored time is UTC if no timezone info
                updated_time = updated_time.replace(tzinfo=datetime.timezone.utc)
            current_time = current_time.replace(tzinfo=datetime.timezone.utc)


            time_diff = current_time - updated_time

            # Define update frequency (e.g., weekly)
            update_frequency = timedelta(days=7)

            return time_diff >= update_frequency

        except Exception as e:
            # Log and default to True to ensure updates happen on parse errors
            logger.error(f"Erreur lors du calcul de la différence de temps pour post {existing_post.get('post_id')}: {e}")
            return True # Default to update if calculation fails

    async def process_post(self, post_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Traite un post: vérifie s'il existe, l'analyse si nécessaire
        Utilise le repository pour la persistence et ContentAnalyzer pour l'analyse.
        """
        post_id = post_data.get("post_id")
        if not post_id:
            logger.error("Post sans post_id, ignoré")
            return None

        if not self.repository:
             logger.error("Repository not initialized.")
             return None # Should not happen if initialize is called before run

        try:
            # Vérifier si le post existe déjà (via repository)
            existing_post = await self.repository.find_post_by_id(post_id)

            if existing_post:
                # Post existe, vérifier s'il faut mettre à jour les métriques
                if await self.should_update_post_metrics(existing_post):
                    metrics = {
                        "shares": post_data.get("shares", 0),
                        "comments_count": post_data.get("comments_count", 0),
                        "like_count": post_data.get("like_count", 0)
                    }
                    await self.repository.update_post_metrics(post_id, metrics) # Use repository

                # Retourner l'analyse existante pour le traitement des commentaires
                return existing_post.get("post_analysis")

            else:
                # Nouveau post, procéder à l'analyse
                post_content = post_data.get("message", "")
                brand_name = post_data.get("brand_name", "")

                # Instantiate Analyzer (could potentially be injected or managed differently)
                analyzer = ContentAnalyzer(
                    google_api_key=self.google_api_key,
                    brand_name=brand_name # Analyzer configuration depends on message data
                )

                post_analysis = await analyzer.analyze_content("post", post_content)

                logger.info(f"Analyse du post {post_id}:")
                logger.info(json.dumps(post_analysis, ensure_ascii=False, indent=2))

                # Préparer les données du post pour la sauvegarde
                post_document = {
                    key: value for key, value in post_data.items()
                    if key != "comments"  # Exclure les commentaires imbriqués
                }
                post_document["post_analysis"] = post_analysis
                post_document["processed_at"] = datetime.utcnow().isoformat() # Use UTC

                # Sauvegarder le post (via repository)
                await self.repository.insert_post(post_document)
                logger.info(f"Nouveau post {post_id} sauvegardé avec analyse")

                return post_analysis # Return the new analysis

        except Exception as e:
            logger.error(f"Erreur lors du traitement du post {post_id}: {e}")
            return None

    async def process_comment(self, comment_data: Dict[str, Any],
                              post_content: str, post_analysis: Dict[str, Any]) -> bool:
        """
        Traite un commentaire: vérifie s'il existe, l'analyse si nécessaire
        Utilise le repository pour la persistence et ContentAnalyzer pour l'analyse.
        """
        comment_id = comment_data.get("comment_id")
        if not comment_id:
            logger.error("Commentaire sans comment_id, ignoré")
            return False

        if not self.repository:
             logger.error("Repository not initialized.")
             return False # Should not happen if initialize is called before run

        try:
            # Vérifier si le commentaire existe déjà (via repository)
            existing_comment = await self.repository.find_comment_by_id(comment_id)

            if existing_comment:
                logger.info(f"Commentaire {comment_id} existe déjà, ignoré")
                return True # Successfully processed (by finding it)

            # Nouveau commentaire, procéder à l'analyse
            comment_text = comment_data.get("message", "")
            brand_name = comment_data.get("brand_name", "") # Assuming brand_name is in comment data too, or passed from post

            # Instantiate Analyzer (could potentially be injected or managed differently)
            analyzer = ContentAnalyzer(
                google_api_key=self.google_api_key,
                brand_name=brand_name
            )

            comment_analysis = await analyzer.analyze_content(
                content_type="comment",
                text=comment_text,
                post_text=post_content,
                post_analysis=post_analysis # Pass post analysis for context
            )

            logger.info(f"Analyse du commentaire {comment_id}:")
            logger.info(json.dumps(comment_analysis, ensure_ascii=False, indent=2))

            # Enrichir les données du commentaire
            comment_document = comment_data.copy()
            comment_document["comment_analysis"] = comment_analysis
            comment_document["processed_at"] = datetime.utcnow().isoformat() # Use UTC

            # Sauvegarder le commentaire (via repository)
            await self.repository.insert_comment(comment_document)
            logger.info(f"Nouveau commentaire {comment_id} sauvegardé avec analyse")

            return True # Successfully processed and saved

        except Exception as e:
            logger.error(f"Erreur lors du traitement du commentaire {comment_id}: {e}")
            return False # Processing failed

    async def process_message(self, message_data: Dict[str, Any]):
        """
        Traite un message complet (post + commentaires)
        Orchestre l'appel à process_post et process_comment.
        """
        post_id = message_data.get('post_id', 'N/A')
        logger.info(f"Traitement du message pour le post {post_id}")

        try:
            # Traiter le post - returns analysis if successful
            post_analysis = await self.process_post(message_data)

            # If the post was successfully processed (either new or existing found)
            # and the message includes comments
            comments = message_data.get("comments", [])
            # Process comments only if we have a post analysis to link them to (either new or existing)
            if post_analysis is not None and comments:
                post_content = message_data.get("message", "") # Get post content for comment analysis context

                # Process each comment
                # Use asyncio.gather for potential parallel processing if needed,
                # but sequential is fine and simpler for now.
                for comment in comments:
                    # Pass post content and analysis to process_comment
                    await self.process_comment(comment, post_content, post_analysis)

            logger.info(f"Message traité avec succès pour le post {post_id}")

        except Exception as e:
            # Catching potential errors *within* message processing
            # Note: Errors in process_post/comment are already logged there.
            # This catch is for unexpected errors in the orchestration logic itself.
            logger.error(f"Erreur lors de l'orchestration du traitement du message pour le post {post_id}: {e}")
            # Decide if you want to re-raise or just log and continue to the next message

    async def run(self):
        """Lance le consumer et traite les messages en continu"""
        # Initialize connections and dependencies first
        await self.initialize()

        logger.info("Démarrage du consumer Facebook Data...")

        # Ensure consumer is initialized before polling
        if not self.consumer:
             logger.error("Kafka Consumer non initialisé. Impossible de démarrer.")
             await self.cleanup() # Attempt cleanup
             return

        try:
            # Use poll with a timeout to allow for clean shutdown
            while True:
                # poll returns {TopicPartition: [Messages]} or None
                message_batch = self.consumer.poll(timeout_ms=1000) # Poll for 1 second

                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            # Log offset and partition for debugging
                            logger.debug(f"Received message from {topic_partition.topic}:{topic_partition.partition} at offset {message.offset}")
                            try:
                                # Value is already deserialized by KafkaConsumer config
                                message_data = message.value
                                await self.process_message(message_data)
                                # Auto-commit is enabled, so no manual commit needed unless you change that config
                            except Exception as e:
                                logger.error(f"Erreur non gérée lors du traitement du message du topic {topic_partition.topic}, partition {topic_partition.partition}, offset {message.offset}: {e}", exc_info=True)
                                # Depending on error handling strategy:
                                # - Log and continue (with auto-commit, message is committed)
                                # - Implement dead letter queue
                                # - Manual commit control

                # A short sleep might still be beneficial even with poll timeout
                # but 0.1s might be too frequent. Adjust based on expected message rate.
                # If poll(1000) doesn't block, it returns immediately. No extra sleep needed.
                # If it times out, the loop continues after 1 second.
                # Let's remove the extra sleep as poll timeout handles this.
                # await asyncio.sleep(0.1) # Removed

        except KeyboardInterrupt:
            logger.info("Arrêt du consumer demandé (KeyboardInterrupt)")
        except Exception as e:
            logger.error(f"Erreur fatale dans le consumer: {e}", exc_info=True)
        finally:
            logger.info("Exiting run loop. Initiating cleanup.")
            await self.cleanup()

    async def cleanup(self):
        """Nettoie les ressources (Kafka consumer, MongoDB client)"""
        logger.info("Nettoyage des ressources...")
        try:
            if self.consumer:
                logger.info("Fermeture du consumer Kafka...")
                self.consumer.close()
                logger.info("Consumer Kafka fermé.")
            if self.mongo_client:
                logger.info("Fermeture du client MongoDB...")
                self.mongo_client.close()
                logger.info("Client MongoDB fermé.")
            logger.info("Ressources nettoyées")
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage: {e}", exc_info=True)

    # Configuration et point d'entrée
async def main():
    """Fonction principale"""

    # Configuration Kafka
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],  # Adjust as needed
        'group_id': 'facebook-data-consumer-group',
        'topic': 'facebook-data',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True
    }

    # Configuration MongoDB
    mongodb_config = {
        'connection_string': 'mongodb://localhost:27017/',  # Adjust as needed
        'database': 'social_media_db_test'
    }

    # Clé API Google (à configurer) - Consider using environment variables
    google_api_key = "AIzaSyDroS___71S2NH_Qz08fuZBkJeX0s21dCY"  # Replace with your API key

    # Create the consumer instance
    consumer = FacebookDataConsumer(kafka_config, mongodb_config, google_api_key)

    try:
        # The run method handles initialize internally now
        await consumer.run()
    except Exception as e:
        # This catch block will mostly catch errors during consumer.run() itself
        # if it doesn't handle them internally. Fatal errors in the loop are caught
        # within run and trigger cleanup.
        logger.error(f"Erreur non gérée dans main: {e}", exc_info=True)
        # Cleanup is already called in run's finally block, but maybe double call is harmless?
        # await consumer.cleanup() # Redundant if run has a finally block

if __name__ == "__main__":
    # Execute the async main function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt de l'application demandé.")
    except Exception as e:
        logger.error(f"Erreur fatale lors de l'exécution de l'application: {e}", exc_info=True)