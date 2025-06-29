import json
from datetime import datetime
from typing import List, Dict, Optional
import sys


class FacebookDataTransformer:
    """
    Transformateur pour les données des pages Facebook
    """

    @staticmethod
    def _safe_get_nested(data: Dict, *keys, default=None):
        """
        Récupère une valeur imbriquée de manière sécurisée
        Handles non-dict inputs and intermediate non-dict values.
        """
        current_data = data # Use a different variable name to avoid shadowing
        try:
            for key in keys:
                # Ensure the current data is a dictionary before attempting access
                if not isinstance(current_data, dict):
                    return default
                # Check if key exists before accessing (more robust than bare [])
                if key not in current_data:
                    return default
                current_data = current_data[key]
            return current_data
        except (KeyError, TypeError, AttributeError):
            # This catch is a fallback, the checks above should prevent most errors
            return default

    @staticmethod
    def transform_page_data(raw_page: Dict) -> Dict:
        """
        Transforme les données brutes de la page en format standardisé.
        Handles non-dict input gracefully by returning a default structure.
        """
        # Define a standard default output structure for invalid/missing data
        # Capture datetime.now() BEFORE the input check, so the timestamp is always generated
        # even if input is invalid. This also makes testing the timestamp easier.
        scraped_timestamp = datetime.now().isoformat()

        # Check if input is a dictionary
        if not isinstance(raw_page, dict):
             # Print a warning to standard error stream
             print(f"⚠️ Attention: transform_page_data reçu des données non-dict: {type(raw_page).__name__} - {raw_page}", file=sys.stderr)
             # Return the default structure, but use the scraped_timestamp captured earlier
             return {
                 "platform": "facebook",
                 "profile_id": None,
                 "profile_name": None,
                 "page_name": None,
                 "url": None,
                 "creation_date": None,
                 "biography": None,
                 "metrics": {"likes": 0, "followers": 0},
                 "is_business_account": False,
                 "scraped_at": scraped_timestamp
             }


        # Proceed with transformation if input is a dict
        page_ad_library = raw_page.get('pageAdLibrary', {})
        # is_business_page_active being non-boolean is now handled by _safe_get_nested
        is_business_active = FacebookDataTransformer._safe_get_nested(
            page_ad_library, 'is_business_page_active', default=False
        )

        # Ensure is_business_account is explicitly boolean in the output
        # because _safe_get_nested might return something other than bool if input is weird
        is_business_account_output = bool(is_business_active)


        return {
            "platform": "facebook",
            "profile_id": raw_page.get("pageId"),
            "profile_name": raw_page.get("title"),
            "page_name": raw_page.get("pageName"),
            "url": raw_page.get("pageUrl"),
            "creation_date": raw_page.get("creation_date"),
            "biography": raw_page.get("intro"),
            "metrics": {
                "likes": raw_page.get("likes", 0), # .get handles missing keys, default 0
                "followers": raw_page.get("followers", 0), # .get handles missing keys, default 0
                # Note: .get(key, default) doesn't check the *type* of the value found if key exists.
                # If raw_page['likes'] was "1000", metrics['likes'] would be "1000".
                # Further type validation might be needed depending on requirements (e.g., convert to int).
                # For now, stick to original logic of just getting the value.
            },
            "is_business_account": is_business_account_output, # Use the boolean version
            "scraped_at": scraped_timestamp # Use the timestamp captured earlier
        }

    @staticmethod
    def transform_pages_batch(raw_pages: List[Dict]) -> List[Dict]:
        """
        Transforme une liste de pages brutes.
        Handles non-list input gracefully.
        Processes each item, appending a default structure for invalid items
        or skipping items that cause unexpected exceptions.
        """
        pages = []
        # Ensure input is a list
        if not isinstance(raw_pages, list):
             print(f"⚠️ Attention: transform_pages_batch reçu des données non-liste: {type(raw_pages).__name__} - {raw_pages}", file=sys.stderr)
             return pages # Return empty list for non-list input

        for i, raw_page in enumerate(raw_pages):
            try:
                # transform_page_data is now robust to non-dict inputs and returns a default dict
                transformed_page = FacebookDataTransformer.transform_page_data(raw_page)
                # Append the result of transform_page_data (either transformed page or default error dict)
                pages.append(transformed_page)

            except Exception as e:
                # This catch block is for truly unexpected errors *during* transformation,
                # which should be less frequent now that input types are handled.
                # Make the error reporting robust even if raw_page is weird.
                page_identifier = f"Index {i}"
                if isinstance(raw_page, dict):
                   page_identifier = raw_page.get('pageUrl') or raw_page.get('pageId', 'N/A')
                # For non-dict, page_identifier is already set

                # Safely get a string representation of the raw_page item
                raw_page_repr = raw_page
                try:
                     raw_page_repr = json.dumps(raw_page, ensure_ascii=False, indent=2)
                except TypeError: # Handle items that can't be JSON serialized
                     raw_page_repr = str(raw_page) # Fallback to string representation

                # Print error to standard error stream
                print(f"⚠️ Erreur inattendue lors de la transformation d'une page ({page_identifier}): {e}. Page brute: {raw_page_repr}", file=sys.stderr)
                # As per original logic, skip this page on unexpected exception
                continue # Do not append anything if an unexpected error occurred

        return pages

    @staticmethod
    def save_pages(pages: List[Dict], filename: str) -> None:
        """
        Sauvegarde les pages dans un fichier JSON.
        Handles non-list input by saving an empty list and printing a warning.
        """
        pages_to_save = pages # Assume input is valid by default

        # Check if input is a list. If not, print warning and save an empty list.
        if not isinstance(pages, list):
             print(f"⚠️ Attention: save_pages reçu des données non-liste: {type(pages).__name__} - {pages}. Sauvegarde d'une liste vide.", file=sys.stderr)
             pages_to_save = [] # Prepare to save an empty list

        try:
            # Attempt to open and dump the data (either the original list or the empty list)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(pages_to_save, f, ensure_ascii=False, indent=2)
            print(f"💾 Pages sauvegardées dans '{filename}'") # Success message to standard output
        except IOError as e:
            print(f"❌ Erreur lors de la sauvegarde du fichier '{filename}': {e}", file=sys.stderr) # Error message to standard error
        except Exception as e:
            print(f"❌ Une erreur inattendue est survenue lors de la sauvegarde de '{filename}': {e}", file=sys.stderr) # Error message to standard error


    @staticmethod
    def get_pages_summary(pages: List[Dict]) -> None:
        """
        Affiche un résumé des pages scrapées.
        Handles non-list input gracefully and skips non-dict items within the list.
        """
        # Ensure input is a list
        if not isinstance(pages, list):
             print(f"⚠️ Attention: get_pages_summary reçu des données non-liste: {type(pages).__name__} - {pages}", file=sys.stderr)
             print("\n📊 Aucun résumé disponible (input non valide).")
             return # Exit if input is not a list

        if not pages:
            print("\n📊 Aucun résumé disponible (aucune page scrapée).")
            return

        print("\n📊 RÉSUMÉ DES PAGES FACEBOOK:")
        print("=" * 60)

        for page in pages:
            # Ensure each item in the list is a dictionary before processing it
            if not isinstance(page, dict):
                 print(f"⚠️ Attention: skipping non-dict item in summary: {type(page).__name__} - {page}", file=sys.stderr)
                 continue # Skip this item and go to the next one

            # Safely get fields using .get()
            name = page.get('profile_name') or page.get('page_name') or 'Page sans nom'
            print(f"📄 {name}")
            print(f"   🆔 ID: {page.get('profile_id', 'N/A')}")
            print(f"   🔗 URL: {page.get('url', 'N/A')}")

            metrics = page.get('metrics', {}) # Default to empty dict if metrics is missing
            # Ensure metrics field is a dictionary before accessing keys within it
            if not isinstance(metrics, dict):
                print(f"⚠️ Attention: 'metrics' field for page '{name}' is not a dictionary (type: {type(metrics).__name__}). Skipping metrics display.", file=sys.stderr)
                metrics = {} # Treat as empty dict for safety

            # Using .get() with default 0 for safety even if metrics is empty or missing keys
            # Also ensure the output is treated as a number for formatting
            followers = metrics.get('followers', 0)
            likes = metrics.get('likes', 0)
            try:
                print(f"   👥 Followers: {int(followers):,}")
            except (ValueError, TypeError):
                 print(f"   👥 Followers: N/A (Non-numeric value '{followers}')", file=sys.stderr)
            try:
                print(f"   👍 Likes: {int(likes):,}")
            except (ValueError, TypeError):
                 print(f"   👍 Likes: N/A (Non-numeric value '{likes}')", file=sys.stderr)


            # Safely get 'is_business_account', defaulting to False if missing
            is_business = page.get('is_business_account', False) # .get handles missing keys, default False
            # Ensure it's treated as boolean for the print (e.g., handles cases where it's None or other types)
            is_business_display = bool(is_business)
            print(f"   🏢 Compte business: {'Oui' if is_business_display else 'Non'}")

            biography = page.get('biography')
            # Check if biography is not None AND is a string before processing
            if biography is not None and isinstance(biography, str):
                bio_preview = biography[:100] + "..." if len(biography) > 100 else biography
                print(f"   📝 Description: {bio_preview}")
            elif biography is not None: # It exists but isn't a string
                 print(f"⚠️ Attention: 'biography' field for page '{name}' is not a string (type: {type(biography).__name__}). Skipping description display.", file=sys.stderr)

            print("-" * 60)