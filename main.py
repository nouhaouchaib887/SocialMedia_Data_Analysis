import json
import os
from typing import Dict, Any, List, Optional
from pathlib import Path
import litellm
from litellm import completion

# --- Configuration de la clé API (recommandé via .env) ---
# Pour tester facilement, vous pouvez décommenter la ligne suivante
# et y mettre votre clé, mais la méthode .env est plus sûre.
from dotenv import load_dotenv
load_dotenv()
if "GOOGLE_API_KEY" not in os.environ:
    os.environ["GOOGLE_API_KEY"] = "AIzaSyDxbT0TMHBW8g3plvqFHafKmNipdWyWGhc"

# --- Classes inchangées (ThemeManager et FacebookPostAnalyzer) ---

class ThemeManager:
    """Gestionnaire des thèmes pour différents opérateurs"""
    
    def __init__(self, themes_directory: str = "themes"):
        self.themes_directory = Path(themes_directory)
        self.themes_cache = {}
        self._load_all_themes()
    
    def _load_all_themes(self):
        """Charge tous les fichiers de thèmes disponibles"""
        if not self.themes_directory.exists():
            print(f"AVERTISSEMENT: Le dossier de thèmes '{self.themes_directory}' n'existe pas. Création du dossier.")
            self.themes_directory.mkdir(parents=True, exist_ok=True)
            return
        
        for theme_file in self.themes_directory.glob("*.json"):
            brand_name = theme_file.stem.lower()
            try:
                with open(theme_file, 'r', encoding='utf-8') as f:
                    self.themes_cache[brand_name] = json.load(f)
                print(f"Thème chargé pour {brand_name}")
            except Exception as e:
                print(f"Erreur lors du chargement de {theme_file}: {e}")
    
    def get_theme_for_brand(self, brand: str) -> Dict[str, Any]:
        """Récupère le thème pour un opérateur spécifique"""
        brand_key = brand.lower().replace(" ", "_")
        
        if brand_key in self.themes_cache:
            return self.themes_cache[brand_key]
        
        brand_variations = {
            "orange": ["orange", "orange_maroc"],
            "inwi": ["inwi"],
            "maroc_telecom": ["maroc_telecom", "iam", "maroctelecom"],
        }
        
        for canonical_name, variations in brand_variations.items():
            if brand_key in variations and canonical_name in self.themes_cache:
                return self.themes_cache[canonical_name]
        
        # Fallback au cas où aucun thème n'est trouvé pour éviter un crash
        print(f"AVERTISSEMENT: Aucun thème trouvé pour la marque: {brand}. Utilisation d'un thème vide.")
        return {}
    
    def get_available_brands(self) -> List[str]:
        """Retourne la liste des marques disponibles"""
        return list(self.themes_cache.keys())
    
    def extract_category_data(self, brand: str, category: str) -> Dict[str, Any]:
        """Extrait les données d'une catégorie spécifique pour une marque"""
        theme = self.get_theme_for_brand(brand)
        if not theme: return {}
        
        if "themes" in theme:
            for theme_item in theme["themes"]:
                if theme_item.get("id") == category:
                    return theme_item
        
        category_key = f"category_{category}"
        if category_key in theme:
            return {
                "id": category,
                "name": category.title(),
                "data": theme[category_key]
            }
        return {}


class FacebookPostAnalyzer:
    """Analyseur de posts Facebook avec gestion multi-marques"""
    
    def __init__(self, themes_directory: str = "themes", model: str = "gemini/gemini-1.5-flash"):
        self.theme_manager = ThemeManager(themes_directory)
        self.model = model
    
    def _get_all_keywords_for_classification(self, brand: str) -> Dict[str, List[str]]:
        """Extrait tous les mots-clés par catégorie principale pour un opérateur"""
        theme = self.theme_manager.get_theme_for_brand(brand)
        keywords_by_category = {"offre": [], "initiative": [], "communication_interaction": []}
        if not theme: return keywords_by_category

        if "themes" in theme:
            for theme_item in theme["themes"]:
                category_id = theme_item.get("id", "")
                
                if category_id == "offre" and "category_offre" in theme_item:
                    for offer in theme_item["category_offre"]:
                        keywords_by_category["offre"].extend(offer.get("keywords", []))
                        for subcategory in offer.get("subcategories", []):
                            keywords_by_category["offre"].extend(subcategory.get("keywords", []))
                
                elif category_id == "initiative" and "category_initiative" in theme_item:
                    for initiative in theme_item["category_initiative"]:
                        keywords_by_category["initiative"].extend(initiative.get("keywords", []))
                        for event in initiative.get("events", []):
                            keywords_by_category["initiative"].extend(event.get("keywords", []))
                
                elif category_id == "communication_interaction" and "category_communication" in theme_item:
                    for comm in theme_item["category_communication"]:
                        keywords_by_category["communication_interaction"].extend(comm.get("keywords", []))
                        for subtype in comm.get("subtypes", []):
                            keywords_by_category["communication_interaction"].extend(subtype.get("keywords", []))
        
        for category in keywords_by_category:
            keywords_by_category[category] = list(set(keywords_by_category[category]))
        
        return keywords_by_category
    
    def _classify_main_category(self, post_text: str, brand: str) -> Dict[str, Any]:
        """Classification principale du post selon l'opérateur"""
        keywords_by_category = self._get_all_keywords_for_classification(brand)
        
        system_prompt = f"Tu es un expert en analyse de contenu pour {brand.upper()}. Classifie les posts dans une de ces catégories: 'offre', 'initiative', 'communication_interaction'. Réponds UNIQUEMENT avec un JSON valide: {{'category': '...', 'confidence': 0.0, 'reasoning': '...'}}"
        
        user_prompt = f"Analyse ce post de {brand.upper()} et classe-le. MOTS-CLÉS pour {brand.upper()}: offre: {', '.join(keywords_by_category['offre'][:15])}..., initiative: {', '.join(keywords_by_category['initiative'][:15])}..., communication: {', '.join(keywords_by_category['communication_interaction'][:15])}... POST: {post_text}"
        
        try:
            response = completion(model=self.model, messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], temperature=0.1, response_format={"type": "json_object"})
            result_str = response.choices[0].message.content
            # Gérer les cas où le modèle renvoie du texte avant/après le JSON
            result = json.loads(result_str[result_str.find('{'):result_str.rfind('}')+1])
            return result
        except Exception as e:
            print(f"Erreur lors de la classification principale: {e}")
            return {"category": "communication_interaction", "confidence": 0.0, "reasoning": "Erreur de classification"}
    
    def _classify_specific_category(self, post_text: str, brand: str, main_category: str) -> Dict[str, Any]:
        """Classification spécifique selon la catégorie principale identifiée"""
        category_data = self.theme_manager.extract_category_data(brand, main_category)
        if not category_data:
            return {"specific_id": None, "subcategory_id": None, "confidence": 0.0, "reasoning": "Pas de données de thème pour cette catégorie"}
        
        system_prompt = f"Tu es un expert des {main_category}s de {brand.upper()}. Identifie l'élément spécifique et sa sous-catégorie. Réponds UNIQUEMENT avec un JSON valide: {{'specific_id': '...', 'subcategory_id': '...', 'confidence': 0.0, 'reasoning': '...'}}"
        
        category_key = f"category_{main_category}"
        category_items = category_data.get(category_key, category_data.get("data", []))
        
        user_prompt = f"ÉLÉMENTS DISPONIBLES pour {main_category.upper()}: {json.dumps(category_items, ensure_ascii=False, indent=2)}\n\nPOST À ANALYSER: {post_text}"
        
        try:
            response = completion(model=self.model, messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], temperature=0.1, response_format={"type": "json_object"})
            result_str = response.choices[0].message.content
            result = json.loads(result_str[result_str.find('{'):result_str.rfind('}')+1])
            return result
        except Exception as e:
            print(f"Erreur lors de la classification spécifique: {e}")
            return {"specific_id": None, "subcategory_id": None, "confidence": 0.0, "reasoning": "Erreur"}
    
    def analyze_post(self, post_text: str, brand: str) -> Dict[str, Any]:
        """Analyse complète d'un post Facebook pour un opérateur spécifique"""
        brand_clean = brand.lower().split('.')[0]
        
        main_classification = self._classify_main_category(post_text, brand_clean)
        
        result = {
            "brand": brand_clean,
            "main_category": main_classification.get("category"),
            "main_confidence": main_classification.get("confidence"),
            "main_reasoning": main_classification.get("reasoning"),
            "specific_id": None, "subcategory_id": None, "specific_confidence": None, "specific_reasoning": None,
            "theme_used": f"themes/{brand_clean}.json"
        }
        
        if main_classification.get("confidence", 0) > 0.3:
            specific_result = self._classify_specific_category(post_text, brand_clean, main_classification["category"])
            result.update({
                "specific_id": specific_result.get("specific_id"),
                "subcategory_id": specific_result.get("subcategory_id"),
                "specific_confidence": specific_result.get("confidence"),
                "specific_reasoning": specific_result.get("reasoning"),
            })
        
        return result
    
    ### MODIFIÉ ###
    def analyze_post_from_data(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyse un post à partir d'un dictionnaire (venant de votre JSON).
        """
        # Cherche le texte dans 'message', sinon 'text', sinon 'content'
        post_text = post_data.get("message", post_data.get("text", post_data.get("content", "")))
        
        # Cherche la marque dans 'brand_name', sinon 'brand', sinon 'operator'
        brand = post_data.get("brand_name", post_data.get("brand", post_data.get("operator", "")))
        
        if not post_text:
            return {"error": "Aucun texte de post ('message') fourni", "original_post_data": post_data}
        if not brand:
            return {"error": "Aucune marque ('brand_name') spécifiée", "original_post_data": post_data}
        
        # Lance l'analyse principale
        analysis_result = self.analyze_post(post_text, brand)
        
        # Ajoute les données du post original pour le contexte, sans écraser les clés de l'analyse
        analysis_result["original_post_data"] = {
            "post_id": post_data.get("post_id"),
            "created_time": post_data.get("created_time"),
            "permalink": post_data.get("permalink"),
            "shares": post_data.get("shares"),
            "comments_count": post_data.get("comments_count"),
            "like_count": post_data.get("like_count")
        }
        
        return analysis_result

### NOUVEAU ###
def process_facebook_data_file(analyzer: FacebookPostAnalyzer, file_path: str) -> List[Dict[str, Any]]:
    """
    Charge un fichier JSON de posts, les analyse et retourne les résultats.
    """
    print(f"\n--- Traitement du fichier : {file_path} ---")
    results = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
    except FileNotFoundError:
        print(f"ERREUR: Le fichier '{file_path}' n'a pas été trouvé.")
        return []
    except json.JSONDecodeError:
        print(f"ERREUR: Le fichier '{file_path}' n'est pas un JSON valide.")
        return []

    # Récupérer la liste des posts
    posts = content.get("data", [])
    if not posts:
        print("AVERTISSEMENT: Aucune donnée de post trouvée dans le fichier (clé 'data' vide ou absente).")
        return []

    print(f"Trouvé {len(posts)} posts à analyser.")
    
    # Analyser chaque post
    for i, post in enumerate(posts):
        print(f"  -> Analyse du post {i+1}/{len(posts)} (ID: {post.get('post_id', 'N/A')})...")
        
        # Le post contient déjà toutes les informations nécessaires ('message', 'brand_name')
        # que la méthode `analyze_post_from_data` sait gérer.
        analysis = analyzer.analyze_post_from_data(post)
        results.append(analysis)

    return results

### MODIFIÉ ###
def main():
    """Fonction principale pour lancer l'analyse."""
    
    # --- Configuration ---
    # Mettez la clé API ici si vous n'utilisez pas les variables d'environnement
    # os.environ["GOOGLE_API_KEY"] = "VOTRE_CLÉ_API_GEMINI_ICI"

    # Initialisation de l'analyseur
    analyzer = FacebookPostAnalyzer(
        themes_directory="themes",
        model="gemini/gemini-1.5-flash"
    )
    
    # --- Chemin vers votre fichier de données ---
    # Assurez-vous que le chemin est correct par rapport à l'endroit où vous exécutez le script
    file_to_process = "data/official_pages/facebook_data_backup/inwi.ma_facebook_posts_with_comments_20250601_035335.json"
    
    # Lancer le traitement du fichier
    analysis_results = process_facebook_data_file(analyzer, file_to_process)
    
    # Afficher les résultats
    if analysis_results:
        print(f"\n--- {len(analysis_results)} RÉSULTATS D'ANALYSE ---")
        for result in analysis_results:
            if "error" in result:
                print(json.dumps(result, indent=2, ensure_ascii=False))
            else:
                # Affichage résumé
                print(
                    f"Post ID: {result['original_post_data'].get('post_id')} | "
                    f"Brand: {result['brand']} | "
                    f"Catégorie: {result['main_category']} (Conf: {result['main_confidence']:.2f}) | "
                    f"Spécifique: {result['specific_id']} (Conf: {result['specific_confidence'] if result['specific_confidence'] is not None else 0:.2f})"
                )
        
        # Optionnel : Sauvegarder les résultats dans un nouveau fichier JSON
        output_filename = f"analysis_results_{Path(file_to_process).stem}.json"
        with open(output_filename, 'w', encoding='utf-8') as f_out:
            json.dump(analysis_results, f_out, indent=2, ensure_ascii=False)
        print(f"\nRésultats complets sauvegardés dans : {output_filename}")


if __name__ == "__main__":
    main()