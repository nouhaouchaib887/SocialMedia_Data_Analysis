"""
Module principal TikTok Scraper - Orchestrateur des modules API et Transformer
"""

from typing import List, Dict, Optional, Union
from api_client import ApifyApiClient, TikTokScraperError
from data_transformer import TikTokDataTransformer


class TikTokProfileScraper:
    """
    Classe principale pour collecter et transformer les informations des profils TikTok
    Utilise ApifyApiClient pour récupérer les données et TikTokDataTransformer pour les traiter
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le scraper avec le token Apify
        
        Args:
            apify_token (str): Token d'authentification Apify
            
        Raises:
            TikTokScraperError: Si le token n'est pas fourni
        """
        self.api_client = ApifyApiClient(apify_token)
        self.transformer = TikTokDataTransformer()
    
    def scrape_profiles(self, 
                       profiles: Union[str, List[str]], 
                       results_per_page: int = 1,
                       timeout: int = 300, 
                       save_to_file: Optional[str] = None,
                       min_followers: Optional[int] = None,
                       verified_only: bool = False) -> List[Dict]:
        """
        Scrape les profils TikTok spécifiés et retourne les données transformées
        
        Args:
            profiles (Union[str, List[str]]): Nom d'utilisateur ou liste de noms d'utilisateur
            results_per_page (int): Nombre de résultats par page
            timeout (int): Timeout en secondes pour l'exécution
            save_to_file (Optional[str]): Chemin du fichier de sauvegarde (optionnel)
            min_followers (Optional[int]): Filtre par nombre minimum de followers
            verified_only (bool): Ne garder que les profils vérifiés
            
        Returns:
            List[Dict]: Liste des profils scrapés, transformés et filtrés
        """
        # Convertir en liste si c'est une chaîne
        if isinstance(profiles, str):
            profiles = [profiles]
        
        try:
            # 1. Récupérer les données brutes via l'API
            print(f"🎯 Début du scraping pour {len(profiles)} profil(s)...")
            raw_data = self.api_client.scrape_raw_data(
                profiles=profiles,
                results_per_page=results_per_page,
                timeout=timeout
            )
            
            # 2. Transformer les données brutes
            transformed_profiles = self.transformer.transform_profiles_batch(raw_data)
            
            # 3. Appliquer les filtres si spécifiés
            if min_followers is not None or verified_only:
                print(f"🔍 Application des filtres...")
                transformed_profiles = self.transformer.filter_profiles_by_criteria(
                    transformed_profiles, 
                    min_followers=min_followers,
                    verified_only=verified_only
                )
                print(f"✅ {len(transformed_profiles)} profil(s) après filtrage.")
            
            # 4. Sauvegarder si demandé
            if save_to_file and transformed_profiles:
                self.transformer.save_profiles(transformed_profiles, save_to_file)
                
            return transformed_profiles
            
        except TikTokScraperError as e:
            print(f"❌ Erreur pendant le scraping: {e}")
            return []
        except Exception as e:
            print(f"❌ Erreur inattendue: {e}")
            return []
    
    def scrape_telecom_profiles(self, 
                              save_to_file: Optional[str] = None,
                              timeout: int = 300,
                              min_followers: Optional[int] = None,
                              verified_only: bool = False) -> List[Dict]:
        """
        Méthode de convenance pour scraper les profils TikTok des opérateurs télécoms marocains
        
        Args:
            save_to_file (Optional[str]): Fichier de sauvegarde
            timeout (int): Timeout en secondes pour l'exécution
            min_followers (Optional[int]): Filtre par nombre minimum de followers
            verified_only (bool): Ne garder que les profils vérifiés
            
        Returns:
            List[Dict]: Profils des opérateurs télécoms transformés
        """
        telecom_profiles = [
            "orangemaroc",
            "inwi.maroc", 
            "maroctelecom"
        ]
        
        print("\n🤖 Lancement du scraping pour les profils télécoms marocains...")
        
        return self.scrape_profiles(
            profiles=telecom_profiles,
            results_per_page=1,
            timeout=timeout,
            save_to_file=save_to_file,
            min_followers=min_followers,
            verified_only=verified_only
        )
    
    def get_profiles_summary(self, profiles: List[Dict]) -> None:
        """
        Affiche un résumé des profils TikTok
        
        Args:
            profiles (List[Dict]): Liste des profils
        """
        self.transformer.get_profiles_summary(profiles)
    
    def save_profiles(self, profiles: List[Dict], filename: str) -> None:
        """
        Sauvegarde les profils dans un fichier JSON
        
        Args:
            profiles (List[Dict]): Liste des profils à sauvegarder
            filename (str): Nom du fichier de sauvegarde
        """
        self.transformer.save_profiles(profiles, filename)


