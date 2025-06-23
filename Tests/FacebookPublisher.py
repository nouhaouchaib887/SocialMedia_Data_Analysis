import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from typing import Dict, Any
import logging
import pathlib

class FacebookDataKafkaPublisher:
    """
    Une classe pour lire des données de pages Facebook officielles depuis des fichiers JSON,
    les transformer et les publier dans un topic Kafka.
    """
    def __init__(self, kafka_config: Dict[str, Any]):
        self.producer = KafkaProducer(**kafka_config)
        self.topic = "facebook-data"
        self.logger = logging.getLogger(__name__)

    def transform_post_to_kafka_message(self, post_data: Dict[str, Any], collection_params: Dict[str, Any]) -> Dict[str, Any]:
        """Transforme un post unique au format du message Kafka."""
        current_timestamp = int(time.time() * 1000)
        kafka_message = {
            "post_id": post_data.get("post_id"),
            "source_id": post_data.get("source_id"),
            "created_time": post_data.get("created_time"),
            "permalink": post_data.get("permalink"),
            "page_id": post_data.get("page_id"),
            "page_name": post_data.get("page_name"),
            "message": post_data.get("message"),
            "media_type": post_data.get("media_type"),
            "media_url": post_data.get("media_url"),
            "thumbnail_url": post_data.get("thumbnail_url"),
            "can_share": post_data.get("can_share"),
            "shares": post_data.get("shares"),
            "can_comment": post_data.get("can_comment"),
            "comments_count": post_data.get("comments_count"),
            "can_like": post_data.get("can_like"),
            "like_count": post_data.get("like_count"),
            "hashtags": post_data.get("hashtags", []),
            "mentions": post_data.get("mentions", []),
            "platform": post_data.get("platform"),
            "brand_name": post_data.get("brand_name"),
            "comments": post_data.get("comments", []),
            "kafka_metadata": {
                "topic": self.topic,
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "producer_timestamp": current_timestamp,
                "message_type": "facebook_post_with_comments",
                "version": "1.0"
            },
            "collection_params": collection_params
        }
        return kafka_message
    
    # ### CORRECTION IMPORTANTE CI-DESSOUS ###
    def publish_from_json_file(self, json_file_path: str) -> Dict[str, Any]:
        """
        Lit un fichier JSON et publie tous ses posts dans Kafka.
        Gère les fichiers dont la racine est un dictionnaire (avec/sans metadata) ou une liste.
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            posts = []
            collection_params = {}

            # NOUVELLE LOGIQUE POUR GÉRER TOUTES LES STRUCTURES
            if isinstance(data, dict) and 'data' in data and 'metadata' in data:
                # Structure 1: Dictionnaire avec 'data' et 'metadata'
                self.logger.debug(f"Structure de type 'dict avec metadata' détectée pour {json_file_path}")
                posts = data.get('data', [])
                metadata = data.get('metadata', {})
                collection_params = metadata.get('collection_params', {})
            elif isinstance(data, dict) and 'post_id' in data:
                # Structure 2: Dictionnaire représentant un post unique
                self.logger.debug(f"Structure de type 'post unique' détectée pour {json_file_path}")
                posts = [data] # On le met dans une liste pour que la boucle fonctionne
                collection_params = {} # Pas de metadata
            elif isinstance(data, list):
                # Structure 3: Liste de posts directement
                self.logger.debug(f"Structure de type 'list' détectée pour {json_file_path}")
                posts = data
                collection_params = {} # Pas de metadata
            else:
                self.logger.warning(f"Structure JSON non supportée dans {json_file_path}. Le fichier n'est ni un dict avec 'data', ni un post unique, ni une list.")
                return {'status': 'unsupported_format', 'published_count': 0, 'failed_count': 0}

            if not posts:
                self.logger.warning(f"Aucun post trouvé à publier dans {json_file_path}.")
                return {'status': 'empty', 'published_count': 0, 'failed_count': 0}

            published_count = 0
            failed_count = 0
            for post in posts:
                try:
                    kafka_message = self.transform_post_to_kafka_message(post, collection_params)
                    key = post.get('post_id')
                    future = self.producer.send(topic=self.topic, key=key, value=kafka_message)
                    future.get(timeout=10)
                    published_count += 1
                except Exception as e:
                    self.logger.error(f"Échec de publication du post {post.get('post_id', 'unknown')} dans le fichier {json_file_path}: {e}")
                    failed_count += 1
            
            self.producer.flush()
            return {'status': 'completed', 'published_count': published_count, 'failed_count': failed_count}
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Erreur de décodage JSON dans le fichier {json_file_path}: {e}")
            return {'status': 'json_error', 'error': str(e)}
        except Exception as e:
            self.logger.error(f"Erreur critique lors du traitement du fichier {json_file_path}: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}

    def close(self):
        """Ferme le producteur Kafka."""
        if self.producer:
            self.producer.close()
            self.logger.info("Producteur Kafka fermé.")

def main():
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8') if k else None,
        'acks': 'all',
        'retries': 3
    }
    
    root_directory = pathlib.Path('/home/doha/Desktop/SocialMedia_Data_Analysis/data/official_pages/facebook_data_backup')
    
    publisher = FacebookDataKafkaPublisher(kafka_config)
    
    logging.info(f"Démarrage de la publication pour le répertoire : {root_directory.resolve()}")
    
    try:
        if not root_directory.is_dir():
            logging.error(f"Le répertoire spécifié n'existe pas : {root_directory.resolve()}")
            return

        json_files = list(root_directory.rglob('*.json'))
        
        if not json_files:
            logging.warning("Aucun fichier .json trouvé dans le répertoire.")
            return

        logging.info(f"{len(json_files)} fichier(s) .json trouvé(s). Début de la publication...")
        
        files_success = 0
        files_failed = 0
        total_posts_published = 0
        total_posts_failed = 0

        for i, json_file in enumerate(json_files, 1):
            logging.info(f"--- [{i}/{len(json_files)}] Publication du fichier : {json_file.name} ---")
            result = publisher.publish_from_json_file(str(json_file))
            logging.info(f"-> Résultat : {result}")

            if result.get('status') in ['completed', 'empty']:
                files_success += 1
                total_posts_published += result.get('published_count', 0)
                total_posts_failed += result.get('failed_count', 0)
            else:
                files_failed += 1

        logging.info("========== RÉSUMÉ FINAL ==========")
        logging.info(f"Fichiers traités avec succès : {files_success}/{len(json_files)}")
        logging.info(f"Fichiers en échec : {files_failed}/{len(json_files)}")
        logging.info("----------------------------------")
        logging.info(f"Total des posts publiés avec succès : {total_posts_published}")
        logging.info(f"Total des échecs de publication (par post) : {total_posts_failed}")
        logging.info("==================================")

    except Exception as e:
        logging.critical(f"Une erreur imprévue est survenue: {e}", exc_info=True)
    finally:
        publisher.close()

if __name__ == "__main__":
    # Changez le level en logging.DEBUG pour voir les messages "Structure de type..."
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()