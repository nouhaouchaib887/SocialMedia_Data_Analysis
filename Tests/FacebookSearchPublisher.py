import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from kafka import KafkaProducer
import logging

class FacebookSearchDataKafkaPublisher:
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
        self.topic = "facebook-search-data-test"  # Topic for facebook data
        self.logger = logging.getLogger(__name__)
        
    def _transform_comment_to_kafka_format(self, comment: Dict[str, Any], parent_post_id: str, brand_name: str) -> Dict[str, Any]:
        """
        Transform comment data to match Kafka message format
        """
        # Handle null comments (empty comments from your JSON)
        if not comment.get('comment_id'):
            return None
            
        # Calculate engagement score
        like_count = comment.get('like_count', 0)
        reply_count = comment.get('reply_count', 0)
        engagement_score = like_count + reply_count
        
        # Calculate comment length
        message = comment.get('message', '')
        comment_length = len(message)
        
        # Check for hashtags and mentions
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
        """
        Transform post data to match Kafka message format
        """
        # Transform comments, filtering out null ones
        transformed_comments = []
        for comment in post.get('comments', []):
            transformed_comment = self._transform_comment_to_kafka_format(
                comment, post['post_id'], brand_name
            )
            if transformed_comment:  # Only add non-null comments
                transformed_comments.append(transformed_comment)
        
        # Create current timestamp for kafka metadata
        current_timestamp = int(time.time() * 1000)  # milliseconds
        
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
                "max_posts": None,  # You can set this based on your needs
                "max_comments_per_post": None,  # You can set this based on your needs
                "post_time_range": "30d"  # Default value, adjust as needed
            }
        }
        
        return kafka_message
    
    def publish_from_json_file(self, file_path: str) -> Dict[str, Any]:
        """
        Read JSON file and publish data to Kafka topic
        
        Args:
            file_path: Path to the JSON file containing Facebook data
            
        Returns:
            Dictionary with publishing results
        """
        try:
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Extract metadata
            metadata = data.get('metadata', {})
            brand_name = metadata.get('brand_name', 'unknown')
            search_query = metadata.get('search_query', '')
            
            # Process each post
            published_count = 0
            failed_count = 0
            
            for post in data.get('data', []):
                try:
                    # Transform post to Kafka format
                    kafka_message = self._transform_post_to_kafka_format(
                        post, search_query, brand_name
                    )
                    
                    # Use post_id as key for partitioning
                    key = post.get('post_id')
                    
                    # Publish to Kafka
                    future = self.producer.send(
                        topic=self.topic,
                        key=key,
                        value=kafka_message
                    )
                    
                    # Wait for the message to be sent
                    record_metadata = future.get(timeout=10)
                    
                    self.logger.info(f"Published post {key} to topic {record_metadata.topic} "
                                   f"partition {record_metadata.partition} "
                                   f"offset {record_metadata.offset}")
                    
                    published_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to publish post {post.get('post_id', 'unknown')}: {str(e)}")
                    failed_count += 1
            
            # Ensure all messages are sent
            self.producer.flush()
            
            return {
                'status': 'completed',
                'published_count': published_count,
                'failed_count': failed_count,
                'total_posts': len(data.get('data', [])),
                'brand_name': brand_name,
                'search_query': search_query
            }
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'published_count': 0,
                'failed_count': 0
            }
    
    def publish_single_post(self, post_data: Dict[str, Any], search_query: str, brand_name: str) -> bool:
        """
        Publish a single post to Kafka
        
        Args:
            post_data: Single post data dictionary
            search_query: Search query used to find the post
            brand_name: Brand name for the post
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Transform post to Kafka format
            kafka_message = self._transform_post_to_kafka_format(
                post_data, search_query, brand_name
            )
            
            # Use post_id as key
            key = post_data.get('post_id')
            
            # Publish to Kafka
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=kafka_message
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            self.logger.info(f"Published post {key} to topic {record_metadata.topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish post: {str(e)}")
            return False
    
    def close(self):
        """
        Close the Kafka producer
        """
        if self.producer:
            self.producer.close()
            self.logger.info("Kafka producer closed")

# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8') if k else None,
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,   # Retry failed sends
        'batch_size': 16384,  # Batch size in bytes
        'linger_ms': 10,  # Wait time to batch messages
    }
    
    # Initialize publisher
    publisher = FacebookSearchDataKafkaPublisher(kafka_config)
    
    try:
        # Publish from JSON file
        result = publisher.publish_from_json_file('../data/external/facebook_data_backup/inwi_إنوي_facebook_data_20250605_195731.json')
        print(f"Publishing result: {result}")
        
    finally:
        # Always close the producer
        publisher.close()