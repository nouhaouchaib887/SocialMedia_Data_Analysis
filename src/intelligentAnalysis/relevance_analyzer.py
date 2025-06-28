import json
from typing import Dict, Any
from base_analyzer import BaseAnalyzer


class RelevanceAnalyzer(BaseAnalyzer):
    """Handles relevance analysis for comments"""
    
    def setup_analyzers(self):
        """Setup relevance analyzer"""
        self.relevance_analyzer = self._create_classifier_from_yaml("relevance_analyzer")
    
    async def analyze_relevance(self, text: str, post_text: str, 
                              post_analysis: str = "") -> Dict[str, Any]:
        """Analyze relevance of content relative to post and general context"""
        try:
            # Relevance analysis
            relevance_result = await self.relevance_analyzer["chain"].ainvoke({
                "brand_name": self.brand_name,
                "post_text": post_text,
                "post_analysis": json.dumps(post_analysis, indent=2, ensure_ascii=False),
                "text": text
            })
            
            # Structure result
            result = {
                "relevance": {
                    "relevance_post": relevance_result["relevance_post"],
                    "general_relevance": relevance_result["general_relevance"]
                },
                "confidence": relevance_result["confidence"]
            }
            
        except Exception as e:
            print(f"Erreur lors de l'analyse de pertinence: {e}")
            result = {
                "relevance": {
                    "relevance_post": "unknown",
                    "general_relevance": "unknown"
                },
                "confidence": 0.0
            }
        
        return result