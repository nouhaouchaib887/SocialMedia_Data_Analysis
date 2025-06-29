"""
Module pour la transformation des données brutes TikTok en format standardisé
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Optional
from api_client import TikTokScraperError


class TikTokDataTransformer:
    """
    Transformateur pour les données TikTok brutes
    """
    
    def __init__(self):
        """Initialise le transformateur"""
        pass
        
    def safe_get_nested(self, data: Dict, *keys, default=None):
        """
        Récupère une valeur imbriquée de manière sécurisée
        
        Args:
            data (Dict): Dictionnaire source
            *keys: Clés imbriquées
            default: Valeur par défaut
            
        Returns:
            Valeur trouvée ou valeur par défaut
        """
        if not isinstance(data, dict):
            return default
        
        try:
            value = data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key, default)
                elif isinstance(value, list) and isinstance(key, int) and len(value) > key:
                    value = value[key]
                else:
                    return default
                
                if value is default and key != keys[-1]:
                    return default
                     
            return value
        except (TypeError, IndexError, AttributeError):
            return default

    def transform_profile_data(self, raw_profile: Dict) -> Optional[Dict]:
        """
        Transforme les données brutes du profil en format standardisé
        
        Args:
            raw_profile (Dict): Données brutes du profil
            
        Returns:
            Optional[Dict]: Profil formaté ou None si les données sont insuffisantes
        """
        if not isinstance(raw_profile, dict) or not raw_profile:
            print(f"⚠️ Skipping invalid raw profile data: {raw_profile}")
            return None
             
        author_meta = raw_profile.get("authorMeta", {})
        if not author_meta:
            print(f"⚠️ Skipping profile data with missing 'authorMeta': {raw_profile}")
            return None

        # Check for essential fields
        profile_id = self.safe_get_nested(author_meta, "id")
        profile_name = self.safe_get_nested(author_meta, "nickName")
        if not profile_id or not profile_name:
            print(f"⚠️ Skipping profile with missing ID or Nickname: {raw_profile}")
            return None

        transformed_data = {
            "platform": "tiktok",
            "profile_id": profile_id,
            "profile_name": profile_name,
            "page_name": self.safe_get_nested(author_meta, "name", ""),
            "url": self.safe_get_nested(author_meta, "profileUrl", ""),
            "biography": self.safe_get_nested(author_meta, "signature", ""),
            "metrics": {
                "likes": self.safe_get_nested(author_meta, "heart", 0),
                "followers": self.safe_get_nested(author_meta, "fans", 0),
                "video_count": self.safe_get_nested(author_meta, "video", 0),
            },
            "is_verified": self.safe_get_nested(author_meta, "verified", False),
            "scraped_at": datetime.now().isoformat()
        }
        
        return transformed_data
    
    def transform_profiles_batch(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transforme une liste de profils bruts
        
        Args:
            raw_data (List[Dict]): Liste des données brutes
            
        Returns:
            List[Dict]: Liste des profils transformés
        """
        transformed_profiles = []
        
        print("✨ Transformation des données des profils...")
        
        for i, raw_profile in enumerate(raw_data):
            try:
                transformed_profile = self.transform_profile_data(raw_profile)
                if transformed_profile:
                    transformed_profiles.append(transformed_profile)
            except Exception as e:
                print(f"⚠️ Erreur lors de la transformation du profil {i+1}: {e}. Skipping this profile.")
                continue
                    
        print(f"✅ {len(transformed_profiles)} profil(s) TikTok traité(s) avec succès sur {len(raw_data)} éléments bruts.")
        return transformed_profiles
    
    def save_profiles(self, profiles: List[Dict], filename: str) -> None:
        """
        Sauvegarde les profils dans un fichier JSON
        
        Args:
            profiles (List[Dict]): Liste des profils à sauvegarder
            filename (str): Nom du fichier de sauvegarde
            
        Raises:
            TikTokScraperError: Si la sauvegarde échoue
        """
        if not filename:
            raise TikTokScraperError("Filename for saving cannot be empty.")
             
        try:
            # Ensure directory exists if filename contains a path
            dirname = os.path.dirname(filename)
            if dirname and not os.path.exists(dirname):
                os.makedirs(dirname)
                print(f"📂 Création du répertoire: {dirname}")
                 
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(profiles, f, ensure_ascii=False, indent=2)
            print(f"💾 Profils TikTok sauvegardés dans '{filename}'")
            
        except (IOError, OSError) as e:
            raise TikTokScraperError(f"Erreur lors de la sauvegarde du fichier '{filename}': {e}") from e
        except TypeError as e:
            raise TikTokScraperError(f"Erreur de type lors de la sérialisation JSON du fichier '{filename}': {e}") from e
        except Exception as e:
            raise TikTokScraperError(f"Une erreur inattendue s'est produite lors de la sauvegarde du fichier '{filename}': {e}") from e

    def get_profiles_summary(self, profiles: List[Dict]) -> None:
        """
        Affiche un résumé des profils TikTok transformés
        
        Args:
            profiles (List[Dict]): Liste des profils
        """
        if not profiles:
            print("\nPas de profils à afficher.")
            return

        print("\n📊 RÉSUMÉ DES PROFILS TIKTOK:")
        print("=" * 60)
        
        for profile in profiles:
            profile_name = self.safe_get_nested(profile, 'profile_name', 'N/A')
            profile_id = self.safe_get_nested(profile, 'profile_id', 'N/A')
            url = self.safe_get_nested(profile, 'url', 'N/A')
            followers = self.safe_get_nested(profile, 'metrics', 'followers', 0)
            likes = self.safe_get_nested(profile, 'metrics', 'likes', 0)
            is_verified = self.safe_get_nested(profile, 'is_verified', False)
            biography = self.safe_get_nested(profile, 'biography', '')

            if profile_name != 'N/A':
                print(f"   📝 Nom d'affichage: {profile_name}")
            if profile_id != 'N/A':
                print(f"   🆔 ID: {profile_id}")
            if url != 'N/A':
                print(f"   🔗 URL: {url}")
            print(f"   👥 Followers: {followers:,}")
            print(f"   💖 Likes totaux: {likes:,}")
            print(f"   ✅ Vérifié: {'Oui' if is_verified else 'Non'}")
            if biography:
                bio_preview = biography[:80] + "..." if len(biography) > 80 else biography
                print(f"   📄 Bio: {bio_preview}")
            print("-" * 60)
    
    def validate_profile_data(self, profile: Dict) -> bool:
        """
        Valide la structure d'un profil transformé
        
        Args:
            profile (Dict): Profil à valider
            
        Returns:
            bool: True si le profil est valide
        """
        required_fields = [
            'platform', 'profile_id', 'profile_name', 
            'url', 'biography', 'metrics', 'is_verified', 'scraped_at'
        ]
        
        for field in required_fields:
            if field not in profile:
                return False
                
        # Validate metrics structure
        if not isinstance(profile.get('metrics'), dict):
            return False
            
        required_metrics = ['likes', 'followers', 'video_count']
        for metric in required_metrics:
            if metric not in profile['metrics']:
                return False
                
        return True
    
    def filter_profiles_by_criteria(self, profiles: List[Dict], 
                                  min_followers: Optional[int] = None,
                                  verified_only: bool = False) -> List[Dict]:
        """
        Filtre les profils selon des critères spécifiques
        
        Args:
            profiles (List[Dict]): Liste des profils
            min_followers (Optional[int]): Nombre minimum de followers
            verified_only (bool): Ne garder que les profils vérifiés
            
        Returns:
            List[Dict]: Profils filtrés
        """
        filtered_profiles = []
        
        for profile in profiles:
            if not self.validate_profile_data(profile):
                continue
                
            # Filter by followers
            if min_followers is not None:
                followers = self.safe_get_nested(profile, 'metrics', 'followers', 0)
                if followers < min_followers:
                    continue
                    
            # Filter by verification status
            if verified_only:
                is_verified = self.safe_get_nested(profile, 'is_verified', False)
                if not is_verified:
                    continue
                    
            filtered_profiles.append(profile)
            
        return filtered_profiles