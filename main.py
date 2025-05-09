from Src.Kafka.TrustPilotKafkaProducer import TrustpilotKafkaProducer
from Src.collectors.Trustpilot.TrustpilotCollector import TrustpilotCollector 
if __name__ == "__main__":
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'trustpilot-producer',
        'acks': 'all'
    }
    
    # Create TrustpilotKafkaProducer
    trustpilot_producer = TrustpilotKafkaProducer(
        kafka_config=kafka_config,
        brand_name='inwi' # Replace with actual brand name
    
    )
    
    # Example 1: Publish brand statistics
    statistics = trustpilot_producer.publish_brand_statistics()
    print(f"Published statistics: {statistics}")
    
    # Example 2: Publish reviews from the last 30 days
    from datetime import datetime, timedelta
    min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    published_count = trustpilot_producer.publish_reviews(pages=25,batch_size=5)
    print(f"Published {published_count} reviews")
    min_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    collector = TrustpilotCollector("inwi")
    reviews = collector.get_reviews(1)

    print(reviews)