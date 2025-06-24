import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List
from kafka import KafkaProducer
import logging
<<<<<<< HEAD
import pathlib
=======
import unicodedata
import re
>>>>>>> f9866a5 (updating files)

class FacebookSearchDataKafkaPublisher:
    """
    Une classe pour lire des données Facebook depuis des fichiers JSON,
    les transformer et les publier dans un topic Kafka.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        """
        Initialise le producteur Kafka avec la configuration donnée.
        
        Args:
<<<<<<< HEAD
            kafka_config: Dictionnaire contenant la configuration Kafka.
        """
        self.producer = KafkaProducer(**kafka_config)
        self.topic = "facebook-search-data" 
=======
            kafka_config: Dictionary containing Kafka configuration
            Example:
            {
                'bootstrap_servers': ['localhost:9092'],
                'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None
            }
        """
        self.producer = KafkaProducer(**kafka_config)
        self.topic = "facebook-search-data"  # Topic for facebook data
>>>>>>> f9866a5 (updating files)
        self.logger = logging.getLogger(__name__)
        
    def _clean_arabic_text(self, text: str) -> str:
        """
        Clean and normalize Arabic text
        
        Args:
            text: Raw text that may contain Arabic characters
            
        Returns:
            Cleaned and normalized text
        """
        if not text:
            return ""
            
        # Remove extra whitespace and normalize
        text = text.strip()
        
        # Normalize Arabic text (remove diacritics if needed, normalize forms)
        text = unicodedata.normalize('NFKC', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def _extract_hashtags_from_arabic_text(self, text: str) -> List[str]:
        """
        Extract hashtags from Arabic text
        
        Args:
            text: Text that may contain Arabic hashtags
            
        Returns:
            List of hashtags found in the text
        """
        if not text:
            return []
            
        # Pattern for hashtags (# followed by Arabic, English, or numbers)
        hashtag_pattern = r'#[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF\w]+'
        hashtags = re.findall(hashtag_pattern, text)
        
        # Clean hashtags (remove the # symbol)
        cleaned_hashtags = [tag[1:] for tag in hashtags if len(tag) > 1]
        
        return cleaned_hashtags
    
    def _extract_mentions_from_text(self, text: str) -> List[str]:
        """
        Extract mentions from text (Arabic or English)
        
        Args:
            text: Text that may contain mentions
            
        Returns:
            List of mentions found in the text
        """
        if not text:
            return []
            
        # Pattern for mentions (@ followed by username characters)
        mention_pattern = r'@[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF\w.]+'
        mentions = re.findall(mention_pattern, text)
        
        # Clean mentions (remove the @ symbol)
        cleaned_mentions = [mention[1:] for mention in mentions if len(mention) > 1]
        
        return cleaned_mentions
        
    def _transform_comment_to_kafka_format(self, comment: Dict[str, Any], parent_post_id: str, brand_name: str) -> Dict[str, Any]:
        """Transforme les données d'un commentaire pour le format du message Kafka."""
        if not comment.get('comment_id'):
            return None
            
<<<<<<< HEAD
=======
        # Clean and normalize Arabic text
        message = self._clean_arabic_text(comment.get('message', ''))
        
        # Extract hashtags and mentions from the message if not provided
        hashtags = comment.get('hashtags', [])
        mentions = comment.get('mentions', [])
        
        # If hashtags/mentions are empty, try to extract from message
        if not hashtags:
            hashtags = self._extract_hashtags_from_arabic_text(message)
        if not mentions:
            mentions = self._extract_mentions_from_text(message)
        
        # Calculate engagement score
>>>>>>> f9866a5 (updating files)
        like_count = comment.get('like_count', 0)
        reply_count = comment.get('reply_count', 0)
        engagement_score = like_count + reply_count
        
<<<<<<< HEAD
        message = comment.get('message', '')
        comment_length = len(message)
        
        has_hashtags = len(comment.get('hashtags', [])) > 0
        has_mentions = len(comment.get('mentions', [])) > 0
=======
        # Calculate comment length (use proper Unicode length for Arabic)
        comment_length = len(message)
        
        # Check for hashtags and mentions
        has_hashtags = len(hashtags) > 0
        has_mentions = len(mentions) > 0
>>>>>>> f9866a5 (updating files)
        
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
            "hashtags": hashtags,
            "mentions": mentions,
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
<<<<<<< HEAD
        """Transforme les données d'un post pour le format du message Kafka."""
        transformed_comments = [
            transformed_comment for comment in post.get('comments', [])
            if (transformed_comment := self._transform_comment_to_kafka_format(comment, post['post_id'], brand_name)) is not None
        ]
=======
        """
        Transform post data to match Kafka message format
        """
        # Clean and normalize Arabic text fields
        message = self._clean_arabic_text(post.get('message', ''))
        caption = self._clean_arabic_text(post.get('caption', message))
        description = self._clean_arabic_text(post.get('description', ''))
        page_name = self._clean_arabic_text(post.get('page_name', ''))
        
        # Extract hashtags and mentions if not provided
        hashtags = post.get('hashtags', [])
        mentions = post.get('mentions', [])
        
        # If hashtags/mentions are empty, try to extract from message
        if not hashtags and message:
            hashtags = self._extract_hashtags_from_arabic_text(message)
        if not mentions and message:
            mentions = self._extract_mentions_from_text(message)
        
        # Transform comments, filtering out null ones
        transformed_comments = []
        for comment in post.get('comments', []):
            transformed_comment = self._transform_comment_to_kafka_format(
                comment, post['post_id'], brand_name
            )
            if transformed_comment:  # Only add non-null comments
                transformed_comments.append(transformed_comment)
>>>>>>> f9866a5 (updating files)
        
        current_timestamp = int(time.time() * 1000)
        
        kafka_message = {
            "post_id": post.get('post_id'),
            "source_id": post.get('source_id'),
            "created_time": post.get('created_time'),
            "updated_time": post.get('updated_time'),
            "permalink": post.get('permalink'),
            "page_id": post.get('page_id'),
            "page_name": page_name,
            "message": message,
            "media_type": post.get('media_type', 'STATUS'),
            "media_url": post.get('media_url'),
            "thumbnail_url": post.get('thumbnail_url'),
            "can_share": post.get('can_share', True),
            "shares": post.get('shares', 0),
            "can_comment": post.get('can_comment', True),
            "comments_count": post.get('comments_count', 0),
            "can_like": post.get('can_like', True),
            "like_count": post.get('like_count', 0),
            "hashtags": hashtags,
            "mentions": mentions,
            "caption": caption,
            "description": description,
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
<<<<<<< HEAD
            # Lit le fichier JSON en s'assurant que l'encodage est utf-8
=======
            # Read JSON file with proper UTF-8 encoding for Arabic text
>>>>>>> f9866a5 (updating files)
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
    
<<<<<<< HEAD
    # Configuration de Kafka
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        
        # ### CORRECTION IMPORTANTE ###
        # Ajout de `ensure_ascii=False` pour que les caractères arabes 
        # soient conservés tels quels dans le JSON avant d'être encodés en UTF-8.
        'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        
=======
    # Kafka configuration with proper Arabic text handling
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
>>>>>>> f9866a5 (updating files)
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
<<<<<<< HEAD
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
=======
        # Publish from JSON file
        result = publisher.publish_from_json_file('/home/nouha/Desktop/SocialMediaTracker/data/external/facebook_data_backup/orangemaroc_أورنج_facebook_data_20250605_192127.json')
        print(f"Publishing result: {result}")
>>>>>>> f9866a5 (updating files)
        
    finally:
        # Toujours fermer le producteur à la fin, même en cas d'erreur.
        publisher.close()