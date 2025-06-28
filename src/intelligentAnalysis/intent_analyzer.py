import json
from typing import Dict, Any
from base_analyzer import BaseAnalyzer


class IntentAnalyzer(BaseAnalyzer):
    """Handles intent classification based on themes"""
    
    def setup_analyzers(self):
        """Setup intent analyzer"""
        self.intent_classifier = self._create_classifier_from_yaml("intent")
    
    async def classify_intent(self, content_type: str, text: str, 
                            result: Dict[str, Any], theme_id: str,
                            post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """Classify intent based on identified theme"""
        try:
            # Get available intents for this theme
            available_intents = self.get_intents_for_theme(theme_id, content_type)
            
            if not available_intents:
                print(f"Aucune intention trouvée pour le thème: {theme_id}")
                return result
            
            # Input data for intent classification
            input_data = {
                "brand_name": self.brand_name,
                "content_type": content_type,
                "post_text": post_text,
                "post_analysis": json.dumps(post_analysis, indent=2, ensure_ascii=False),
                "text": text,
                "theme_name": result["theme"]["name"],
                "available_intents": ", ".join(available_intents)
            }
            
            # Classify intent
            intent_result = await self.intent_classifier["chain"].ainvoke(input_data)
            
            # Add intent to result
            result["intent"] = {
                "name": intent_result["intent"]
            }
            
            # Update global confidence
            result["confidence"] = min(result["confidence"], intent_result["confidence"])
            
        except Exception as e:
            print(f"Erreur lors de la classification d'intention: {e}")
            result["intent"] = {
                "name": "unknown",
                "confidence": 0.0
            }
        
        return result