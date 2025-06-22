import json
import time
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any

class InstagramDataKafkaPublisher:
    def __init__(self, kafka_config: Dict[str, Any]):
        """
        Initialize Kafka producer with configuration
        
        Args:
            kafka_config: Dictionary containing Kafka configuration
                         Example: {
                             'bootstrap_servers': ['localhost:9092'],
                             'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                             'key_serializer': lambda k: k.encode('utf-8') if k else None
                         }
        """
        self.producer = KafkaProducer(**kafka_config)
        self.topic = "instagram-data"  # Topic for Instagram data
    
    def transform_post_to_kafka_message(self, post_data: Dict[str, Any], collection_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Transform a single post from your JSON format to Kafka message format
        
        Args:
            post_data: Single post data from your JSON file
            collection_params: Collection parameters (optional)
            
        Returns:
            Dictionary in Kafka message format matching your example structure
        """
        # Current timestamp for Kafka metadata
        current_timestamp = int(time.time() * 1000)  # milliseconds
        
        # Create the Kafka message structure matching your Facebook example but for Instagram
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
            "platform": "instagram",  # Changed to instagram
            "brand_name": post_data.get("brand_name"),
            "comments": post_data.get("comments", []),
            "kafka_metadata": {
                "topic": self.topic,
                "produced_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "producer_timestamp": current_timestamp,
                "message_type": "instagram_post_with_comments",  # Changed to instagram
                "version": "1.0"
            },
            "collection_params": collection_params or {
                "max_posts": 2,
                "max_comments_per_post": 2,
                "days_back": 15,
                "from_date": "2025-06-05T00:00:00.000Z"
            }
        }
        
        return kafka_message
    
    def publish_from_json_file(self, json_file_path: str, collection_params: Dict[str, Any] = None):
        """
        Read JSON file and publish all posts to Kafka
        
        Args:
            json_file_path: Path to your JSON file
            collection_params: Optional collection parameters
        """
        try:
            # Read the JSON file
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Extract posts from the data array
            posts = data.get('data', [])
            metadata = data.get('metadata', {})
            
            print(f"Found {len(posts)} Instagram posts to publish")
            
            # Default collection params if not provided
            if collection_params is None:
                collection_params = {
                    "max_posts": len(posts),
                    "max_comments_per_post": 10,
                    "days_back": 30,
                    "from_date": "2025-05-01T00:00:00.000Z"
                }
            
            # Publish each post
            published_count = 0
            for i, post in enumerate(posts):
                try:
                    # Transform post to Kafka message format
                    kafka_message = self.transform_post_to_kafka_message(post, collection_params)
                    
                    # Use post_id as key for partitioning
                    key = post.get('post_id')
                    
                    # Send to Kafka
                    future = self.producer.send(
                        topic=self.topic,
                        key=key,
                        value=kafka_message
                    )
                    
                    # Wait for confirmation
                    record_metadata = future.get(timeout=10)
                    published_count += 1
                    
                    print(f"✅ Published Instagram post {i+1}/{len(posts)}: {post.get('post_id')}")
                    print(f"   Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                    
                except Exception as e:
                    print(f"❌ Error publishing Instagram post {post.get('post_id', 'unknown')}: {str(e)}")
                    continue
            
            # Ensure all messages are sent
            self.producer.flush()
            print(f"\n🎉 Successfully published {published_count}/{len(posts)} Instagram posts to Kafka topic '{self.topic}'")
            
        except FileNotFoundError:
            print(f"❌ Error: JSON file '{json_file_path}' not found")
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON format in file '{json_file_path}': {str(e)}")
        except Exception as e:
            print(f"❌ Error reading JSON file or publishing to Kafka: {str(e)}")
    
    def publish_single_post(self, post_data: Dict[str, Any], collection_params: Dict[str, Any] = None):
        """
        Publish a single Instagram post to Kafka
        
        Args:
            post_data: Single post data
            collection_params: Optional collection parameters
        """
        try:
            kafka_message = self.transform_post_to_kafka_message(post_data, collection_params)
            key = post_data.get('post_id')
            
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=kafka_message
            )
            
            record_metadata = future.get(timeout=10)
            print(f"✅ Published Instagram post: {post_data.get('post_id')}")
            print(f"   Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
        except Exception as e:
            print(f"❌ Error publishing Instagram post: {str(e)}")
    
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()
        print("🔌 Kafka producer closed")


def main():
    """
    Example usage of the InstagramDataKafkaPublisher
    """
    # Configuration Kafka
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],  # Mettre à jour avec vos serveurs Kafka
        'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8') if k else None,
        'acks': 'all',  # Attendre la confirmation de tous les réplicas
        'retries': 3,   # Réessayer les envois échoués
        'max_in_flight_requests_per_connection': 1  # Assurer l'ordre
    }
    
    # Paramètres de collection (personnalisables)
    collection_params = {
        "max_posts": 100,
        "max_comments_per_post": 500,
        "days_back": 30,
        "from_date": "2025-05-01T00:00:00.000Z"
    }
    
    # Initialiser le publisher
    publisher = InstagramDataKafkaPublisher(kafka_config)
    
    try:
        # Publier depuis votre fichier JSON
        json_file_path = "../data/official_pages/instagram_data_backup/maroctelecom_Instagram_posts_with_comments_20250605_114601.json"  # Mettre à jour avec le chemin de votre fichier
        publisher.publish_from_json_file(json_file_path, collection_params)
        
    except KeyboardInterrupt:
        print("\n⏹️ Publication interrompue par l'utilisateur")
    except Exception as e:
        print(f"❌ Erreur dans main: {str(e)}")
    finally:
        # Toujours fermer le producer
        publisher.close()


if __name__ == "__main__":
    main()