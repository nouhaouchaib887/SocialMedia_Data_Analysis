"""
Facebook Processor module.
Orchestrates the collection, transformation, and publishing of Facebook data.
"""
from typing import List

from collectors.Facebook.FacebookCollector import FacebookCollector
from collectors.Facebook.facebook_data_transformer import FacebookDataTransformer
from Kafka.kafka_facebook_producer import KafkaFacebookProducer


class FacebookProcessor:
    """
    Orchestrates the collection, transformation, and publishing of Facebook data.
    """
    
    def __init__(self, 
                 file_path: str, 
                 column_name: str,
                 bootstrap_servers: List[str], 
                 topic: str):
        """Initialize the Facebook processor.
        
        Args:
            file_path: Path to the Excel file containing data
            column_name: Name of the column containing structured text data
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic to publish events to
        """
        self.collector = FacebookCollector(file_path, column_name)
        self.transformer = FacebookDataTransformer()
        self.producer = KafkaFacebookProducer(bootstrap_servers, topic)
    
    def process_data_item(self, xml_data: str):
        """Process a single data item.
        
        Args:
            xml_data: XML data string to process
        """
        try:
            # Transform the XML data
            events_data = self.transformer.process_xml_data(xml_data)
            print(events_data)
            # Publish events to Kafka
            self.producer.publish_events(events_data)
        except Exception as e:
            print(f"Error processing data item: {e}")
    
    def start_processing(self, 
                        interval_min: float = 0.5, 
                        interval_max: float = 2.0, 
                        jitter: bool = True):
        """Start the data processing.
        
        Args:
            interval_min: Minimum interval between messages in seconds
            interval_max: Maximum interval between messages in seconds
            jitter: Whether to add random time jitter between messages
        """
        try:
            # Start the data streaming simulation
            self.collector.simulate_streaming(
                data_handler=self.process_data_item,
                interval_min=interval_min,
                interval_max=interval_max,
                jitter=jitter
            )
        finally:
            # Close the Kafka producer
            self.producer.close()