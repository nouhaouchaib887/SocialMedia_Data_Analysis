import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List
from kafka import KafkaProducer
import logging
import pathlib

class FacebookSearchDataKafkaPublisher:
    """
    Une classe pour lire des données Facebook depuis des fichiers JSON,
    les transformer et les publier dans un topic Kafka.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        """
        Initialise le producteur Kafka avec la configuration donnée.
        
        Args:
            kafka_config: Dictionnaire contenant la configuration Kafka.
        """
        self.producer = KafkaProducer(**kafka_config)
        self.topic = "facebook-search-data" 
        self.logger = logging.getLogger(__name__)
        
    def _transform_comment_to_kafka_format(self, comment: Dict[str, Any], parent_post_id: str, brand_name: str) -> Dict[str, Any]:
        """Transforme les données d'un commentaire pour le format du message Kafka."""
        if not comment.get('comment_id'):
            return None
            
        like_count = comment.get('like_count', 0)
        reply_count = comment.get('reply_count', 0)
        engagement_score = like_count + reply_count
        
        message = comment.get('message', '')
        comment_length = len(message)
        
        has_hashtags = len(comment.get('hashtags', [])) > 0
        has_mentions = len(comment.get('mentions', [])) > 0
        
        return {
            "comment_id": comment.get('comment_id'),
            "comment_url": comment.get('comment_url'),
            "user_id": comment.get('user_id'),
            "user_name": comment.get('user_name'),
            "user_url": comment.get('user_url'),
            "created_time": comment.get('created_time'),
            "message": message,
            "like_count": like_count,
            "reply_count": reply_count,
            "hashtags": comment.get('hashtags', []),
            "mentions": comment.get('mentions', []),
            "platform": "facebook",
            "brand_name": brand_name,
            "comment_metadata": {
                "parent_post_id": parent_post_id,
                "comment_engagement_score": engagement_score,
                "comment_length": comment_length,
                "has_hashtags": has_hashtags,
                "has_mentions": has_mentions
            }
        }
    
    def _transform_post_to_kafka_format(self, post: Dict[str, Any], search_query: str, brand_name: str) -> Dict[str, Any]:
        """Transforme les données d'un post pour le format du message Kafka."""
        transformed_comments = [
            transformed_comment for comment in post.get('comments', [])
            if (transformed_comment := self._transform_comment_to_kafka_format(comment, post['post_id'], brand_name)) is not None
        ]
        
        current_timestamp = int(time.time() * 1000)
        
        kafka_message = {
            "post_id": post.get('post_id'),
            "source_id": post.get('source_id'),
            "created_time": post.get('created_time'),
            "updated_time": post.get('updated_time'),
            "permalink": post.get('permalink'),
            "page_id": post.get('page_id'),
            "page_name": post.get('page_name'),
            "message": post.get('message', ''),
            "media_type": post.get('media_type', 'STATUS'),
            "media_url": post.get('media_url'),
            "thumbnail_url": post.get('thumbnail_url'),
            "can_share": post.get('can_share', True),
            "shares": post.get('shares', 0),
            "can_comment": post.get('can_comment', True),
            "comments_count": post.get('comments_count', 0),
            "can_like": post.get('can_like', True),
            "like_count": post.get('like_count', 0),
            "hashtags": post.get('hashtags', []),
            "mentions": post.get('mentions', []),
            "caption": post.get('caption', post.get('message', '')),
            "description": post.get('description', ''),
            "platform": "facebook",
            "brand_name": brand_name,
            "comments": transformed_comments,
            "kafka_metadata": {
                "topic": self.topic,
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "producer_timestamp": current_timestamp,
                "message_type": "facebook_search_post_with_comments",
                "version": "1.0"
            },
            "collection_params": {
                "search_query": search_query,
                "max_posts": None,
                "max_comments_per_post": None,
                "post_time_range": "30d"
            }
        }
        return kafka_message
    
    def publish_from_json_file(self, file_path: str) -> Dict[str, Any]:
        """
        Lit un fichier JSON et publie ses données dans le topic Kafka.
        
        Args:
            file_path: Chemin vers le fichier JSON contenant les données Facebook.
            
        Returns:
            Un dictionnaire avec le résultat de la publication.
        """
        try:
            # Lit le fichier JSON en s'assurant que l'encodage est utf-8
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            metadata = data.get('metadata', {})
            brand_name = metadata.get('brand_name', 'unknown')
            search_query = metadata.get('search_query', '')
            
            published_count = 0
            failed_count = 0
            
            for post in data.get('data', []):
                try:
                    kafka_message = self._transform_post_to_kafka_format(post, search_query, brand_name)
                    key = post.get('post_id')
                    
                    future = self.producer.send(topic=self.topic, key=key, value=kafka_message)
                    record_metadata = future.get(timeout=10)
                    
                    self.logger.info(f"Post publié {key} -> topic '{record_metadata.topic}' [partition {record_metadata.partition}]")
                    published_count += 1
                except Exception as e:
                    self.logger.error(f"Échec de la publication du post {post.get('post_id', 'unknown')}: {e}")
                    failed_count += 1
            
            self.producer.flush()
            
            return {
                'status': 'completed', 'published_count': published_count, 'failed_count': failed_count,
                'total_posts': len(data.get('data', [])), 'brand_name': brand_name
            }
        except Exception as e:
            self.logger.error(f"Erreur lors du traitement du fichier {file_path}: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def close(self):
        """Ferme le producteur Kafka proprement."""
        if self.producer:
            self.producer.close()
            self.logger.info("Producteur Kafka fermé.")

# ==============================================================================
# POINT D'ENTRÉE DU SCRIPT
# ==============================================================================
if __name__ == "__main__":
    # Configuration du logging pour des messages clairs
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Configuration de Kafka
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        
        # ### CORRECTION IMPORTANTE ###
        # Ajout de `ensure_ascii=False` pour que les caractères arabes 
        # soient conservés tels quels dans le JSON avant d'être encodés en UTF-8.
        'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        
        'key_serializer': lambda k: k.encode('utf-8') if k else None,
        'acks': 'all',
        'retries': 3,
        'batch_size': 16384,
        'linger_ms': 10,
    }
    
    # Définir le répertoire à scanner.
    # Assurez-vous que ce chemin est correct.
    root_directory = pathlib.Path('/home/doha/Desktop/SocialMedia_Data_Analysis/data/external')
    
    # Initialiser le publisher
    publisher = FacebookSearchDataKafkaPublisher(kafka_config)
    
    logging.info(f"Démarrage de la publication pour le répertoire : {root_directory.resolve()}")
    
    try:
        if not root_directory.is_dir():
            logging.error(f"Le répertoire spécifié n'existe pas : {root_directory.resolve()}")
        else:
            # Trouve tous les fichiers se terminant par .json, même dans les sous-dossiers
            json_files = list(root_directory.rglob('*.json'))
            
            if not json_files:
                logging.warning("Aucun fichier .json trouvé dans le répertoire.")
            else:
                logging.info(f"{len(json_files)} fichier(s) .json trouvé(s). Début de la publication...")
                
                total_success = 0
                total_failures = 0

                for i, json_file in enumerate(json_files, 1):
                    logging.info(f"--- [{i}/{len(json_files)}] Publication du fichier : {json_file.name} ---")
                    try:
                        result = publisher.publish_from_json_file(str(json_file))
                        logging.info(f"-> Résultat : {result}")
                        if result.get('status') == 'completed':
                            total_success += result.get('published_count', 0)
                            total_failures += result.get('failed_count', 0)
                    except Exception as e:
                        logging.error(f"!!! Erreur critique lors de la publication du fichier {json_file.name}: {e}")

                logging.info("========== RÉSUMÉ FINAL ==========")
                logging.info(f"Total des posts publiés avec succès : {total_success}")
                logging.info(f"Total des échecs de publication : {total_failures}")
                logging.info("==================================")
        
    finally:
        # Toujours fermer le producteur à la fin, même en cas d'erreur.
        publisher.close()