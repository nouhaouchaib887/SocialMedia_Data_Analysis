import json
import time
from typing import Dict, Any, List
from kafka import KafkaProducer
from datetime import datetime


class TikTokDataKafkaPublisher:
    def __init__(self, kafka_config: Dict[str, Any]):
        """
        Initialize Kafka producer with configuration
        
        Args:
            kafka_config: Dictionary containing Kafka configuration
            Example:
            {
                'bootstrap_servers': ['localhost:9092'],
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None
            }
        """
        self.producer = KafkaProducer(**kafka_config)
        self.topic = "tiktok-data"  # Topic for tiktok data
    
    def transform_json_to_kafka_message(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform JSON data structure to match Kafka message format
        
        Args:
            json_data: The JSON data from your file
            
        Returns:
            List of Kafka messages ready to be published
        """
        kafka_messages = []
        
        # Extract metadata
        metadata = json_data.get("metadata", {})
        brand_name = metadata.get("brand_name", "")
        
        # Process each post in the data array
        for post_data in json_data.get("data", []):
            # Prepare comments with metadata
            comments_with_metadata = []
            for comment in post_data.get("comments", []):
                comment_with_metadata = {
                    "comment_id": comment.get("comment_id", ""),
                    "comment_url": comment.get("comment_url", ""),
                    "user_id": comment.get("user_id", ""),
                    "user_name": comment.get("user_name", ""),
                    "user_url": comment.get("user_url", ""),
                    "created_time": comment.get("created_time", ""),
                    "message": comment.get("message", ""),
                    "like_count": comment.get("like_count", 0),
                    "reply_count": comment.get("reply_count", 0),
                    "hashtags": comment.get("hashtags", []),
                    "mentions": comment.get("mentions", []),
                    "platform": comment.get("platform", "tiktok"),
                    "brand_name": brand_name,
                    "comment_metadata": {
                        "parent_post_id": post_data.get("post_id", ""),
                        "comment_engagement_score": comment.get("like_count", 0) + comment.get("reply_count", 0),
                        "comment_length": len(comment.get("message", "")),
                        "has_hashtags": len(comment.get("hashtags", [])) > 0,
                        "has_mentions": len(comment.get("mentions", [])) > 0
                    }
                }
                comments_with_metadata.append(comment_with_metadata)
            
            # Create Kafka message structure
            kafka_message = {
                "post_id": post_data.get("post_id", ""),
                "source_id": post_data.get("source_id", ""),
                "created_time": post_data.get("created_time", ""),
                "permalink": post_data.get("permalink", ""),
                "page_id": post_data.get("page_id", ""),
                "page_name": post_data.get("page_name", ""),
                "message": post_data.get("message", ""),
                "media_type": post_data.get("media_type", "video"),
                "media_url": post_data.get("media_url", ""),
                "thumbnail_url": post_data.get("thumbnail_url", ""),
                "can_share": post_data.get("can_share", True),
                "shares": post_data.get("shares", 0),
                "can_comment": post_data.get("can_comment", True),
                "comments_count": post_data.get("comments_count", 0),
                "can_like": post_data.get("can_like", True),
                "like_count": post_data.get("like_count", 0),
                "playCount": post_data.get("playCount", 0),
                "hashtags": post_data.get("hashtags", []),
                "mentions": post_data.get("mentions", []),
                "caption": post_data.get("caption", ""),
                "description": post_data.get("description", ""),
                "platform": post_data.get("platform", "tiktok"),
                "brand_name": brand_name,
                "comments": comments_with_metadata,
                "kafka_metadata": {
                    "topic": self.topic,
                    "produced_at": datetime.utcnow().isoformat() + "Z",
                    "producer_timestamp": int(time.time() * 1000),
                    "message_type": "tiktok_post_with_comments",
                    "version": "1.0"
                },
                "collection_params": {
                    "max_posts": metadata.get("total_records", 100),
                    "max_comments_per_post": 2,  # Adjust based on your needs
                    "days_back": 100,  # Adjust based on your needs
                    "from_date": self._calculate_from_date(metadata.get("collection_time", ""))
                }
            }
            
            kafka_messages.append(kafka_message)
        
        return kafka_messages
    
    def _calculate_from_date(self, collection_time: str) -> str:
        """
        Calculate from_date based on collection_time
        
        Args:
            collection_time: Collection time in format "YYYYMMDD_HHMMSS"
            
        Returns:
            ISO formatted date string
        """
        try:
            if collection_time:
                # Parse collection_time format "20250608_155026"
                date_part = collection_time.split('_')[0]
                year = int(date_part[:4])
                month = int(date_part[4:6])
                day = int(date_part[6:8])
                
                # Calculate 100 days back (adjust as needed)
                from datetime import timedelta
                collection_date = datetime(year, month, day)
                from_date = collection_date - timedelta(days=100)
                return from_date.isoformat() + ".000Z"
            else:
                # Default fallback
                return "2025-03-12T00:00:00.000Z"
        except:
            return "2025-03-12T00:00:00.000Z"
    
    def publish_from_json_file(self, json_file_path: str) -> bool:
        """
        Read JSON file and publish all posts to Kafka
        
        Args:
            json_file_path: Path to the JSON file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            
            kafka_messages = self.transform_json_to_kafka_message(json_data)
            
            for message in kafka_messages:
                self.publish_message(message)
            
            print(f"Successfully published {len(kafka_messages)} messages to Kafka topic '{self.topic}'")
            return True
            
        except Exception as e:
            print(f"Error publishing messages: {str(e)}")
            return False
    
    def publish_message(self, message: Dict[str, Any]) -> bool:
        """
        Publish a single message to Kafka
        
        Args:
            message: The message to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use post_id as the key for partitioning
            key = message.get("post_id", "")
            
            future = self.producer.send(self.topic, key=key, value=message)
            record_metadata = future.get(timeout=10)
            
            print(f"Message published successfully:")
            print(f"  Topic: {record_metadata.topic}")
            print(f"  Partition: {record_metadata.partition}")
            print(f"  Offset: {record_metadata.offset}")
            print(f"  Post ID: {message.get('post_id', 'N/A')}")
            
            return True
            
        except Exception as e:
            print(f"Error publishing message: {str(e)}")
            return False
    
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()


# Example usage
if __name__ == "__main__":
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8') if k else None,
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,   # Retry failed sends
        'max_in_flight_requests_per_connection': 1  # Ensure ordering
    }
    
    # Initialize publisher
    publisher = TikTokDataKafkaPublisher(kafka_config)
    
    try:
        # Publish from JSON file
        success = publisher.publish_from_json_file("../data/official_pages/tiktok_data_backup/orangemaroc_Tiktok_posts_with_comments_20250607_121800.json")
        
        if success:
            print("All messages published successfully!")
        else:
            print("Failed to publish messages.")
            
    finally:
        # Always close the producer
        publisher.close()