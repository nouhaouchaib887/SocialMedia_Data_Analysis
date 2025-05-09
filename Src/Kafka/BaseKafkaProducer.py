import json
import logging
from datetime import datetime
from confluent_kafka import Producer

class BaseKafkaProducer:
    def __init__(self, kafka_config, default_topic):
        self.producer = Producer(kafka_config)
        self.default_topic = default_topic
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Standard logger configuration"""
        logger = logging.getLogger(f"kafka_producer_{self.__class__.__name__}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
        
    def _delivery_report(self, err, msg):
        """Handles message delivery reports"""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def _serialize_date(self, obj):
        """Serializes date objects for JSON"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Non-serializable type: {type(obj)}")
        
    def produce_message(self, topic, key, value, headers=None):
        """Produces a message to Kafka"""
        try:
            if isinstance(key, str):
                key = key.encode('utf-8')
            
            message = json.dumps(value, default=self._serialize_date, ensure_ascii=False)
            self.producer.produce(
                topic or self.default_topic,
                key=key,
                value=message.encode('utf-8'),
                headers=headers,
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")
            raise
            
    def flush(self):
        """Flushes any pending messages"""
        self.producer.flush()