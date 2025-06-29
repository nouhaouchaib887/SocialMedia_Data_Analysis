from typing import List, Dict, Optional, Union
from api_client import ApifyInstagramClient
from data_transformer import InstagramDataTransformer


class InstagramProfileScraper:
    """
    Classe principale pour orchestrer le scraping et la transformation des profils Instagram
    """
    
    def __init__(self, apify_token: str):
        """
        Initialise le scraper avec le token Apify
        
        Args:
            apify_token (str): Token d'authentification Apify
        """
        self.client = ApifyInstagramClient(apify_token)
        self.transformer = InstagramDataTransformer()
        
    def scrape_profiles(self, 
                       usernames: Union[str, List[str]], 
                       timeout: int = 300, 
                       save_to_file: Optional[str] = None,
                       show_summary: bool = True) -> List[Dict]:
        """
        Scrape et transforme les profils Instagram spécifiés
        
        Args:
            usernames (Union[str, List[str]]): Nom d'utilisateur ou liste de noms d'utilisateur
            timeout (int): Timeout en secondes pour l'exécution
            save_to_file (Optional[str]): Chemin du fichier de sauvegarde (optionnel)
            show_summary (bool): Afficher le résumé des profils
            
        Returns:
            List[Dict]: Liste des profils scrapés et transformés
            
        Raises:
            Exception: Si le scraping échoue
        """
        # Convertir en liste si c'est une chaîne
        if isinstance(usernames, str):
            usernames = [usernames]
            
        try:
            # 1. Récupérer les données brutes via l'API
            raw_profiles = self.client.get_raw_profiles(usernames, timeout)
            
            # 2. Transformer les données
            print("🔄 Transformation des données...")
            transformed_profiles = self.transformer.transform_profiles_batch(raw_profiles)
            
            # 3. Sauvegarder si demandé
            if save_to_file:
                self.transformer.save_profiles(transformed_profiles, save_to_file)
            
            # 4. Afficher le résumé si demandé
            if show_summary:
                self.transformer.get_profile_summary(transformed_profiles)
                
            return transformed_profiles
            
        except Exception as e:
            print(f"❌ Erreur lors du scraping: {e}")
            raise
    
    def load_and_analyze_profiles(self, filename: str) -> List[Dict]:
        """
        Charge les profils depuis un fichier et affiche les analyses
        
        Args:
            filename (str): Nom du fichier à charger
            
        Returns:
            List[Dict]: Profils chargés
        """
        profiles = self.transformer.load_profiles(filename)
        
        if profiles:
            # Afficher le résumé
            self.transformer.get_profile_summary(profiles)
            
            # Afficher les statistiques
            stats = self.transformer.get_metrics_summary(profiles)
            self.display_statistics(stats)
            
        return profiles
    
    def filter_profiles(self, profiles: List[Dict], **filters) -> List[Dict]:
        """
        Filtre les profils selon des critères
        
        Args:
            profiles (List[Dict]): Profils à filtrer
            **filters: Critères de filtrage (min_followers, is_verified, etc.)
            
        Returns:
            List[Dict]: Profils filtrés
        """
        return self.transformer.filter_profiles(profiles, **filters)
    
    def display_statistics(self, stats: Dict) -> None:
        """
        Affiche les statistiques des profils
        
        Args:
            stats (Dict): Statistiques calculées
        """
        if not stats:
            return
            
        print("\n📈 STATISTIQUES GÉNÉRALES:")
        print("=" * 50)
        print(f"📊 Total de profils: {stats['total_profiles']}")
        print(f"✅ Comptes vérifiés: {stats['verified_accounts']}")
        print(f"🏢 Comptes business: {stats['business_accounts']}")
        print(f"🔒 Comptes privés: {stats['private_accounts']}")
        
        print("\n👥 STATISTIQUES FOLLOWERS:")
        print("-" * 30)
        print(f"Total: {stats['followers']['total']:,}")
        print(f"Moyenne: {stats['followers']['average']:,.0f}")
        print(f"Min: {stats['followers']['min']:,}")
        print(f"Max: {stats['followers']['max']:,}")
        
        print("\n📸 STATISTIQUES POSTS:")
        print("-" * 30)
        print(f"Total: {stats['posts']['total']:,}")
        print(f"Moyenne: {stats['posts']['average']:,.0f}")
        print(f"Min: {stats['posts']['min']:,}")
        print(f"Max: {stats['posts']['max']:,}")
        print("=" * 50)
    
    def export_filtered_data(self, profiles: List[Dict], 
                           filename: str, 
                           **filters) -> List[Dict]:
        """
        Filtre et exporte les données
        
        Args:
            profiles (List[Dict]): Profils à traiter
            filename (str): Nom du fichier d'export
            **filters: Critères de filtrage
            
        Returns:
            List[Dict]: Profils filtrés et exportés
        """
        filtered_profiles = self.filter_profiles(profiles, **filters)
        
        if filtered_profiles:
            self.transformer.save_profiles(filtered_profiles, filename)
            print(f"📤 {len(filtered_profiles)} profil(s) filtré(s) exporté(s) vers '{filename}'")
        else:
            print("❌ Aucun profil ne correspond aux critères de filtrage")
            
        return filtered_profiles


