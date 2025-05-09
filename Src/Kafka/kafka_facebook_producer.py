"""
Kafka Facebook Producer module.
Handles publishing Facebook data events to Kafka topics.
"""
import json
from kafka import KafkaProducer
from typing import Dict, List, Any


class KafkaFacebookProducer:
    """
    A producer that publishes Facebook data events to Kafka.
    """
    
    def __init__(self, bootstrap_servers: List[str], topic: str):
        """Initialize the Kafka Facebook producer with Kafka configuration.
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic to publish events to
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        self.topic = topic
        print(f"Initialized Kafka producer with topic: {topic}")
    
    def publish_event(self, event_data: Dict[str, Any]):
        """Publish a single event to Kafka topic.
        
        Args:
            event_data: Event data to publish
        """
        self.producer.send(self.topic, event_data)
        print(f"Published event with ID: {event_data.get('post_id', event_data.get('comment_id', 'unknown'))}")
    
    def publish_events(self, events_data: Dict[str, Any]):
        """Publish post and comments events to Kafka topic.
        
        Args:
            events_data: Dictionary containing post and comments data
        """
        # Publish post event
        if "post" in events_data and events_data["post"]:
            self.publish_event(events_data["post"])
        
        # Publish comment events
        if "comments" in events_data and events_data["comments"]:
            for comment in events_data["comments"]:
                self.publish_event(comment)
        
        # Ensure messages are sent
        self.producer.flush()
    
    def close(self):
        """Close the Kafka producer."""
        self.producer.close()
        print("Kafka producer closed")