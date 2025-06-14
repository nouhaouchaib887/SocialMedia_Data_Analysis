from collectors.Trustpilot.TrustpilotCollector import TrustpilotCollector
from Kafka.BaseKafkaProducer import BaseKafkaProducer
import time
from datetime import datetime
from confluent_kafka import Producer
class TrustpilotKafkaProducer:
    """
    A class that combines TrustpilotCollector and BaseKafkaProducer to collect
    Trustpilot reviews and publish them to Kafka.
    """
    
    def __init__(self, kafka_config, brand_name, topic=None, delay_range=(1, 3)):
        """
        Initialize the TrustpilotKafkaProducer.
        
        Args:
            kafka_config (dict): Kafka producer configuration
            brand_name (str): Name of the brand to scrape on Trustpilot
            topics (dict): Kafka topics configuration with keys 'reviews' and 'statistics'
            delay_range (tuple): Range of seconds to delay between requests (min, max)
        """
        # Default topics if not provided
        self.topic= topic or {
            'reviews': f'trustpilot.content.reviews',
            'statistics':'trustpilot.statistics'
        }
        
        # Initialize collector and producer
        self.collector = TrustpilotCollector(brand_name, delay_range=delay_range)
        self.producer = BaseKafkaProducer(kafka_config, self.topic['reviews'])
        self.logger = self.producer.logger
        
        self.logger.info(f"Initialized TrustpilotKafkaProducer for brand: {brand_name}")
        
    def publish_brand_statistics(self):
        """
        Collect and publish brand statistics to Kafka.
        
        Returns:
            dict: The statistics data that was published
        """
        self.logger.info(f"Collecting statistics for {self.collector.brand_name}")
        statistics = self.collector.get_brand_statistics()
        
        if "error" in statistics:
            self.logger.error(f"Failed to collect statistics: {statistics['error']}")
            return statistics
        
        # Add metadata
        statistics['timestamp'] = datetime.now().isoformat()
        
        # Publish to Kafka
        key = f"{self.collector.brand_name}_stats"
        self.producer.produce_message(
            self.topic['statistics'], 
            key, 
            statistics,
            headers=[
                ('content_type', 'application/json'),
                ('event_type', 'brand_statistics'),
                ('timestamp', datetime.now().isoformat().encode('utf-8'))
            ]
        )
        
        self.logger.info(f"Published brand statistics for {self.collector.brand_name}")
        return statistics
    
    def publish_reviews(self, pages=1, min_date=None, batch_size=10):
        """
        Collect and publish reviews to Kafka, optionally in batches.
        
        Args:
            pages (int): Number of pages to scrape
            min_date (str|datetime): Minimum date for reviews (format: 'YYYY-MM-DD' or datetime object)
            batch_size (int): Number of reviews to publish in a single Kafka message (0 for individual messages)
            
        Returns:
            int: Number of reviews published
        """
        self.logger.info(f"Collecting reviews for {self.collector.brand_name}, pages: {pages}")
        reviews = self.collector.get_reviews(pages, min_date)
        
        if not reviews:
            self.logger.warning(f"No reviews found for {self.collector.brand_name}")
            return 0
            
        total_published = 0
        
        # Publish in batches or individually
        if batch_size > 0:
            batches = [reviews[i:i + batch_size] for i in range(0, len(reviews), batch_size)]
            
            for i, batch in enumerate(batches):
                # Create a batch message
                batch_message = {
                    'brand': self.collector.brand_name,
                    'batch_number': i + 1,
                    'total_batches': len(batches),
                    'count': len(batch),
                    'timestamp': datetime.now().isoformat(),
                    'reviews': batch
                }
                
                # Publish batch
                key = f"{self.collector.brand_name}_batch_{i+1}"
                self.producer.produce_message(
                    self.topics['reviews'],
                    key,
                    batch_message,
                    headers=[
                        ('content_type', 'application/json'),
                        ('event_type', 'reviews_batch'),
                        ('batch_number', str(i+1).encode('utf-8')),
                        ('count', str(len(batch)).encode('utf-8')),
                        ('timestamp', datetime.now().isoformat().encode('utf-8'))
                    ]
                )
                
                total_published += len(batch)
                self.logger.info(f"Published batch {i+1}/{len(batches)} with {len(batch)} reviews")
        else:
            # Publish individual reviews
            for i, review in enumerate(reviews):
                # Add metadata
                review['timestamp'] = datetime.now().isoformat()
                
                # Generate a unique key for the review
                key = f"{self.collector.brand_name}_review_{i+1}"
                
                self.producer.produce_message(
                    self.topics['reviews'],
                    key,
                    review,
                    headers=[
                        ('content_type', 'application/json'),
                        ('event_type', 'single_review'),
                        ('timestamp', datetime.now().isoformat().encode('utf-8'))
                    ]
                )
                
                total_published += 1
                
                # Log progress periodically
                if (i + 1) % 10 == 0:
                    self.logger.info(f"Published {i+1}/{len(reviews)} reviews")
        
        # Ensure all messages are delivered
        self.producer.flush()
        self.logger.info(f"Total published reviews: {total_published}")
        
        return total_published
    
    def run_scheduled_collection(self, interval_hours=24, pages=5, min_date=None):
        """
        Run continuous scheduled collection and publishing of Trustpilot data.
        
        Args:
            interval_hours (int): Hours between each collection
            pages (int): Number of pages to scrape each time
            min_date (str|datetime): Minimum date for reviews to consider
            
        This will run until interrupted with Ctrl+C
        """
        self.logger.info(f"Starting scheduled collection every {interval_hours} hours")
        
        try:
            while True:
                # Collect and publish statistics
                self.publish_brand_statistics()
                
                # Collect and publish reviews
                self.publish_reviews(pages=pages, min_date=min_date)
                
                # Wait for next interval
                self.logger.info(f"Waiting {interval_hours} hours until next collection")
                time.sleep(interval_hours * 3600)
                
        except KeyboardInterrupt:
            self.logger.info("Scheduled collection interrupted by user")
        except Exception as e:
            self.logger.error(f"Error in scheduled collection: {e}")
        finally:
            self.producer.flush()
            self.logger.info("Scheduled collection ended")
