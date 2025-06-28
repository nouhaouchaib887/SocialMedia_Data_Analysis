import asyncio
import json
from typing import Dict, Any
from thematic_analyzer import ThematicAnalyzer
from intent_analyzer import IntentAnalyzer
from sentiment_analyzer import SentimentAnalyzer
from relevance_analyzer import RelevanceAnalyzer


class ContentAnalyzer:
    """Main content analyzer that orchestrates all analysis modules"""
    
    def __init__(self, google_api_key: str, brand_name: str, 
                 intents_file_posts: str = "../config/themes/posts_intents.json",
                 intents_file_comments: str = "../config/themes/comments_intents.json", 
                 prompts_file: str = "../config/prompts/prompts_o.yaml"):
        """
        Initialize the main content analyzer
        
        Args:
            google_api_key: Google Gemini API key
            brand_name: Brand name
            intents_file_posts: Path to posts intents JSON config
            intents_file_comments: Path to comments intents JSON config
            prompts_file: Path to YAML prompts file
        """
        self.google_api_key = google_api_key
        self.brand_name = brand_name
        
        # Initialize all analyzer modules
        self.thematic_analyzer = ThematicAnalyzer(
            google_api_key, brand_name, intents_file_posts, 
            intents_file_comments, prompts_file
        )
        
        self.intent_analyzer = IntentAnalyzer(
            google_api_key, brand_name, intents_file_posts, 
            intents_file_comments, prompts_file
        )
        
        self.sentiment_analyzer = SentimentAnalyzer(
            google_api_key, brand_name, intents_file_posts, 
            intents_file_comments, prompts_file
        )
        
        self.relevance_analyzer = RelevanceAnalyzer(
            google_api_key, brand_name, intents_file_posts, 
            intents_file_comments, prompts_file
        )
    
    async def analyze_content(self, content_type: str, text: str,
                            post_text: str = "", post_analysis: str = "") -> Dict[str, Any]:
        """
        Analyze content (comment or post) and return complete classification
        
        The analysis includes several successive steps:
        - Main theme detection
        - Hierarchical classification specific to theme (offers, initiatives, communication...)
        - Intent detection
        - Sentiment analysis
        - Relevance verification (for comments)
        
        Args:
            content_type: Type of content to analyze ("comment" or "post")
            text: Main text to analyze
            post_text: Original post text (for comment context)
            post_analysis: Previous post analysis results
            
        Returns:
            Complete content classification dictionary
        """
        
        if content_type =="comment":
            print("")
            result = await self.relevance_analyzer.analyze_relevance(text,post_text, post_analysis)
            print("")
        else:
            result = { "theme":{
                "id":"",
                "name":""
            },
            "confidence": ""} 
                              
        if (content_type == "comment" and  result.get("relevance", {}).get("general_relevance") == "true" ) or content_type !="comment":
            
            # 1. Classification du thème principal avec le prompt YAML
            result = await self.thematic_analyzer.classify_theme(result,content_type, text,post_text, post_analysis)
            theme_id = result["theme"]["id"]
            # 2. Classification spécifique selon le thème - HIÉRARCHIQUE
            if theme_id  == "offre":
                result = await self.thematic_analyzer.classify_offre_hierarchical(content_type, text, result,post_text, post_analysis)
            
            elif theme_id  == "initiative":
                result = await self.thematic_analyzer.classify_initiative_hierarchical(content_type,text, result,post_text, post_analysis)
            
            elif theme_id  == "communication_interaction":
                result = await self.thematic_analyzer.classify_communication_hierarchical(content_type,text, result,post_text, post_analysis)
        
            # 3. Classification des intentions basée sur le thème
            if theme_id  != "none":
                result = await self.intent_analyzer.classify_intent(content_type,text, result, theme_id ,post_text, post_analysis)
            if content_type == "comment":

                # 3. Classification des intentions basée sur le thème
                if theme_id  != "none":
                    result = await self.sentiment_analyzer.analyze_sentiment(text, result, post_text, post_analysis)
        
        return result
    
async def main():
    # REMPLACEZ PAR VOTRE VRAIE CLÉ API GOOGLE GEMINI
    GOOGLE_API_KEY = "AIzaSyBkwR3k6Hu_WybdYjGzm1sDq4evhSal0nk"
    
    # Vérification de la clé API
    if GOOGLE_API_KEY == "VOTRE_CLE_API_GOOGLE_ICI":
        print("⚠️  ATTENTION: Veuillez remplacer GOOGLE_API_KEY par votre vraie clé API Google Gemini")
        return
    
    try:
        # Initialiser l'analyseur avec la clé API
        analyzer = ContentAnalyzer(
            google_api_key=GOOGLE_API_KEY,
            brand_name="orangemaroc"
        )
        
        # Exemple de post avec commentaires
        post_content = '''
🚨 تفرجو على راحتكم مع عرض *5 😎

استافد من 2,5Go على Youtube بالإضافة ل TikTok مع *5, صالحة سيمانة و غييير ب 10 دراهم 🧡'''
        
        # Commentaires associés au post
        comments = [
            "دخلتها على اساس كونكسيو الواتساب صدقات ليا فتيكتوك",
            "amazon chnou fiha"
        ]
        
        print("=== Analyse d'un Post avec ses Commentaires ===\n")
        
        # 1. Analyser le post principal
        print("📝 ANALYSE DU POST PRINCIPAL:")
        print(f"Contenu: {post_content}")
        print("-" * 50)
        
        try:
            post_analysis = await analyzer.analyze_content("post", post_content)
            print(f"Résultat Post: {json.dumps(post_analysis, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"Erreur lors de l'analyse du post: {e}")
            post_analysis = None
        
        print("\n" + "="*70 + "\n")
        
        # 2. Analyser chaque commentaire
        for i, comment in enumerate(comments, 1):
            print(f"💬 ANALYSE DU COMMENTAIRE {i}:")
            print(f"Contenu: {comment}")
            print("-" * 50)
            
            try:
                comment_analysis = await analyzer.analyze_content(
                    content_type="comment",
                    text=comment,
                    post_text=post_content,
                    post_analysis=post_analysis
                )
                print(f"Résultat Commentaire {i}: {json.dumps(comment_analysis, ensure_ascii=False, indent=2)}")
            except Exception as e:
                print(f"Erreur lors de l'analyse du commentaire {i}: {e}")
            
            print("\n" + "="*50 + "\n")
        
        # Tests supplémentaires avec d'autres posts
        print("=== Tests Supplémentaires ===\n")
        additional_tests = [
            '''النجمة 6 غادي تغادرنا ولكن ماتخافوش راها مامطولاش فالتسافيرة ديالها 😎
تسناوها نهار 23 يونيو 🔥''',
            "كيفاش دايز عندكوم العيد ؟"
        ]
        
        for i, test_post in enumerate(additional_tests, 1):
            print(f"Test {i}: {test_post}")
            print("-" * 50)
            
            try:
                result = await analyzer.analyze_content("post", test_post)
                print(f"Résultat: {json.dumps(result, ensure_ascii=False, indent=2)}")
            except Exception as e:
                print(f"Erreur lors de l'analyse: {e}")
            
            print("\n" + "="*70 + "\n")
            
    except Exception as e:
        print(f"Erreur d'initialisation: {e}")

if __name__ == "__main__":
    # Pour tester la classe
    asyncio.run(main())