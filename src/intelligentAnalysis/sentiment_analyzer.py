import json
from typing import Dict, Any
from base_analyzer import BaseAnalyzer


class SentimentAnalyzer(BaseAnalyzer):
    """Handles sentiment analysis for content"""
    
    def setup_analyzers(self):
        """Setup sentiment analyzer"""
        self.sentiment_classifier = self._create_classifier_from_yaml('sentiment')
    
    async def analyze_sentiment(self, text: str, result: Dict[str, Any],
                              post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Analyze sentiment expressed in content"""
        try:
            # Sentiment classification
            sentiment_result = await self.sentiment_classifier["chain"].ainvoke({
                "brand_name": self.brand_name,
                "post_text": post_text,
                "post_analysis": json.dumps(post_analysis, indent=2, ensure_ascii=False),
                "text": text
            })
            
            # Add sentiment to result
            result["sentiment"] = {
                "sentiment": sentiment_result["sentiment"],
                "emotion": sentiment_result["emotion"],
                "polarity_score": sentiment_result["polarity_score"]
            }
            
            # Update global confidence
            result["confidence"] = min(result["confidence"], sentiment_result["confidence"])
            
        except Exception as e:
            print(f"Erreur lors de l'analyse de sentiment: {e}")
            result["sentiment"] = {
                "sentiment": "unknown",
                "emotion": "unknown",
                "polarity_score": 0.0,
                "confidence": 0.0
            }
        
        return result