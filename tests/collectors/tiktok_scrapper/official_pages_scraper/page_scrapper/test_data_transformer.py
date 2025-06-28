import unittest
from unittest.mock import patch, Mock, mock_open, MagicMock
import json
import os
import sys
from io import StringIO
from datetime import datetime
from typing import List, Dict, Optional
from src.collectors.tiktok_scrapper.official_pages_scraper.page_scrapper.data_transformer import *
from src.collectors.tiktok_scrapper.official_pages_scraper.page_scrapper.data_transformer import TikTokDataTransformer

class TikTokScraperError(Exception):
    """Exception personnalisée pour les erreurs liées au scraper TikTok."""
    pass

class TestTikTokDataTransformer(unittest.TestCase):

    def setUp(self):
        """Configuration avant chaque test"""
        self.transformer = TikTokDataTransformer()

        # Mock datetime.now for predictable scraped_at times
        self.mock_datetime_now = MagicMock(spec=datetime)
        self.fixed_now = datetime(2023, 10, 27, 10, 0, 0)
        self.mock_datetime_now.return_value = self.fixed_now
        self.patcher_datetime = patch('data_transformer.datetime') # Assumes class is in tiktok_transformer.py
        # If class is in this file, patch __main__.datetime
        if TikTokDataTransformer.__module__ == '__main__':
             self.patcher_datetime = patch('__main__.datetime')
        self.mock_datetime = self.patcher_datetime.start()
        self.mock_datetime.now = self.mock_datetime_now # Only mock the 'now' class method


        # Setup stdout capture for tests that check print statements
        self._original_stdout = sys.stdout
        self._captured_output = StringIO()
        sys.stdout = self._captured_output
        self.addCleanup(lambda: sys.stdout.__setattr__('buffer', self._original_stdout.buffer)) # Use __setattr__ for compatibility
        self.addCleanup(lambda: sys.stdout.__setattr__('encoding', self._original_stdout.encoding)) # Restore encoding
        self.addCleanup(lambda: sys.stdout.__setattr__('errors', self._original_stdout.errors)) # Restore errors
        self.addCleanup(lambda: sys.stdout.__setattr__('closed', self._original_stdout.closed)) # Restore closed state
        # A simpler way for modern Python >= 3.7:
        # self._original_stdout = sys.stdout
        # sys.stdout = self._captured_output
        # self.addCleanup(lambda: sys.stdout.__setattr__('buffer', self._original_stdout.buffer)) # Use __setattr__ for compatibility
        self.addCleanup(lambda: sys.stdout.__setattr__('write', self._original_stdout.write)) # Restore write method

    def tearDown(self):
        """Nettoyage après chaque test"""
        self.patcher_datetime.stop()
        # Restore stdout is handled by addCleanup in setUp


    # --- Tests for safe_get_nested ---
    def test_safe_get_nested_success(self):
        """Test safe_get_nested avec des clés valides"""
        data = {"a": {"b": {"c": 123}}}
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", "c"), 123)

    def test_safe_get_nested_missing_intermediate_key(self):
        """Test safe_get_nested avec une clé intermédiaire manquante"""
        data = {"a": {}}
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "b", "c"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", "c", default="missing"), "missing")

    def test_safe_get_nested_missing_final_key(self):
        """Test safe_get_nested avec une clé finale manquante"""
        data = {"a": {"b": {"d": 456}}}
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "b", "c"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", "c", default="missing"), "missing")

    def test_safe_get_nested_invalid_intermediate_type(self):
        """Test safe_get_nested avec une valeur intermédiaire du mauvais type"""
        data = {"a": "not_a_dict"}
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "b", "c"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", "c", default="missing"), "missing")

    def test_safe_get_nested_list_indexing(self):
        """Test safe_get_nested avec indexation de liste"""
        data = {"a": [{"b": 10}, {"b": 20}]}
        self.assertEqual(self.transformer.safe_get_nested(data, "a", 1, "b"), 20)

    def test_safe_get_nested_out_of_bounds_index(self):
        """Test safe_get_nested avec index de liste hors limites"""
        data = {"a": [{"b": 10}]}
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", 1, "b"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", 1, "b", default="missing"), "missing")

    def test_safe_get_nested_invalid_index_type(self):
        """Test safe_get_nested avec un type d'index invalide"""
        data = {"a": [{"b": 10}]}
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "wrong_key", "b"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "wrong_key", "b", default="missing"), "missing")

    def test_safe_get_nested_non_dict_input(self):
        """Test safe_get_nested avec une entrée qui n'est pas un dictionnaire"""
        data = "not a dict"
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "b"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", default="missing"), "missing")

    def test_safe_get_nested_empty_dict_input(self):
        """Test safe_get_nested avec un dictionnaire vide"""
        data = {}
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "b"))
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", default="missing"), "missing")

    def test_safe_get_nested_no_keys(self):
        """Test safe_get_nested sans clés fournies"""
        data = {"a": 1}
        self.assertEqual(self.transformer.safe_get_nested(data), data)
        self.assertEqual(self.transformer.safe_get_nested(data, default="missing"), data)

    def test_safe_get_nested_default_value(self):
        """Test safe_get_nested avec une valeur par défaut spécifiée"""
        data = {"a": {}}
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "b", default="default_val"), "default_val")

    def test_safe_get_nested_nested_default_value_is_returned(self):
        """Test safe_get_nested retourne la valeur par défaut si une clé intermédiaire renvoie la valeur par défaut"""
        data = {"a": {"b": None}} # If default is None, this should return None
        self.assertIsNone(self.transformer.safe_get_nested(data, "a", "c")) # c is missing, returns None (default)
        self.assertEqual(self.transformer.safe_get_nested(data, "a", "c", default="missing"), "missing")

        data_with_custom_default = {"a": {"b": "exists"}}
        # If get("c", "default") is called, it returns "default".
        # The safe_get_nested should then stop and return "default".
        self.assertEqual(self.transformer.safe_get_nested(data_with_custom_default, "a", "c", "d", default="custom_default"), "custom_default")


    # --- Tests for transform_profile_data ---
    def test_transform_profile_data_success(self):
        """Test transform_profile_data avec des données valides"""
        raw_profile = {
            "authorMeta": {
                "id": "789",
                "nickName": "TestUser",
                "name": "Full Test Name",
                "profileUrl": "https://www.tiktok.com/@testuser",
                "signature": "Just a test account",
                "heart": 12345,
                "fans": 6789,
                "video": 50,
                "verified": True
            }
        }
        expected_transformed = {
            "platform": "tiktok",
            "profile_id": "789",
            "profile_name": "TestUser",
            "page_name": "Full Test Name",
            "url": "https://www.tiktok.com/@testuser",
            "biography": "Just a test account",
            "metrics": {
                "likes": 12345,
                "followers": 6789,
                "video_count": 50,
            },
            "is_verified": True,
            "scraped_at": self.fixed_now.isoformat() # Use the mocked time
        }
        transformed = self.transformer.transform_profile_data(raw_profile)
        self.assertEqual(transformed, expected_transformed)

    def test_transform_profile_data_missing_optional_fields(self):
        """Test transform_profile_data avec des champs optionnels manquants"""
        raw_profile = {
            "authorMeta": {
                "id": "abc",
                "nickName": "UserMinimal"
                # Optional fields are missing
            }
        }
        expected_transformed = {
            "platform": "tiktok",
            "profile_id": "abc",
            "profile_name": "UserMinimal",
            "page_name": "", # Default empty string
            "url": "", # Default empty string
            "biography": "", # Default empty string
            "metrics": {
                "likes": 0, # Default 0
                "followers": 0, # Default 0
                "video_count": 0, # Default 0
            },
            "is_verified": False, # Default False
            "scraped_at": self.fixed_now.isoformat() # Use the mocked time
        }
        transformed = self.transformer.transform_profile_data(raw_profile)
        self.assertEqual(transformed, expected_transformed)


    def test_transform_profile_data_invalid_input_type(self):
        """Test transform_profile_data avec une entrée qui n'est pas un dictionnaire"""
        self.assertIsNone(self.transformer.transform_profile_data(None))
        self.assertIsNone(self.transformer.transform_profile_data([]))
        self.assertIsNone(self.transformer.transform_profile_data("string"))

    def test_transform_profile_data_empty_dict(self):
        """Test transform_profile_data avec un dictionnaire vide"""
        self.assertIsNone(self.transformer.transform_profile_data({}))

    def test_transform_profile_data_missing_author_meta(self):
        """Test transform_profile_data avec 'authorMeta' manquant"""
        raw_profile = {"some_other_key": "value"}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile))

    def test_transform_profile_data_invalid_author_meta_type(self):
        """Test transform_profile_data avec 'authorMeta' d'un type invalide"""
        raw_profile = {"authorMeta": "not a dict"}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile))
        raw_profile = {"authorMeta": None}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile))


    def test_transform_profile_data_missing_required_id(self):
        """Test transform_profile_data avec 'id' manquant dans 'authorMeta'"""
        raw_profile = {"authorMeta": {"nickName": "User"}}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile))

    def test_transform_profile_data_missing_required_nickname(self):
        """Test transform_profile_data avec 'nickName' manquant dans 'authorMeta'"""
        raw_profile = {"authorMeta": {"id": "123"}}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile))

    def test_transform_profile_data_empty_required_fields(self):
        """Test transform_profile_data avec ID ou Nickname vides"""
        raw_profile_empty_id = {"authorMeta": {"id": "", "nickName": "User"}}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile_empty_id))
        raw_profile_empty_nickname = {"authorMeta": {"id": "123", "nickName": ""}}
        self.assertIsNone(self.transformer.transform_profile_data(raw_profile_empty_nickname))


    def test_transform_profile_data_metrics_wrong_type(self):
        """Test transform_profile_data convertit les métriques en int"""
        raw_profile = {
            "authorMeta": {
                "id": "789",
                "nickName": "TestUser",
                "heart": "12345", # string instead of int
                "fans": 6789.5, # float instead of int
                "video": None, # None instead of int
                "verified": "true" # string instead of bool
            }
        }
        expected_transformed = {
            "platform": "tiktok",
            "profile_id": "789",
            "profile_name": "TestUser",
            "page_name": "",
            "url": "",
            "biography": "",
            "metrics": {
                "likes": 12345, # Should be converted
                "followers": 6789, # Should be converted (truncated float)
                "video_count": 0, # None should default to 0 and be int
            },
            "is_verified": True, # Should be converted
            "scraped_at": self.fixed_now.isoformat()
        }
        transformed = self.transformer.transform_profile_data(raw_profile)
        self.assertEqual(transformed, expected_transformed)

    # --- Tests for transform_profiles_batch ---
    @patch.object(TikTokDataTransformer, 'transform_profile_data')
    def test_transform_profiles_batch_empty_list(self, mock_transform_single):
        """Test transform_profiles_batch avec une liste vide"""
        raw_data = []
        transformed = self.transformer.transform_profiles_batch(raw_data)
        self.assertEqual(transformed, [])
        mock_transform_single.assert_not_called()

    @patch.object(TikTokDataTransformer, 'transform_profile_data', side_effect=[
        {"id": "1", "name": "User1"},
        {"id": "2", "name": "User2"}
    ])
    def test_transform_profiles_batch_success(self, mock_transform_single):
        """Test transform_profiles_batch avec des profils valides"""
        raw_data = [{"raw1": 1}, {"raw2": 2}] # Raw data structure doesn't matter here due to mock
        expected = [{"id": "1", "name": "User1"}, {"id": "2", "name": "User2"}]
        transformed = self.transformer.transform_profiles_batch(raw_data)
        self.assertEqual(transformed, expected)
        self.assertEqual(mock_transform_single.call_count, 2)
        mock_transform_single.assert_any_call({"raw1": 1})
        mock_transform_single.assert_any_call({"raw2": 2})


    @patch.object(TikTokDataTransformer, 'transform_profile_data', side_effect=[
        None, # Simulate invalid profile
        {"id": "2", "name": "User2"}, # Simulate valid profile
        None, # Simulate another invalid profile
        {"id": "4", "name": "User4"}  # Simulate another valid profile
    ])
    def test_transform_profiles_batch_mixed_valid_invalid(self, mock_transform_single):
        """Test transform_profiles_batch avec un mélange de profils valides et invalides"""
        raw_data = [{"raw1": 1}, {"raw2": 2}, {"raw3": 3}, {"raw4": 4}]
        expected = [{"id": "2", "name": "User2"}, {"id": "4", "name": "User4"}]
        transformed = self.transformer.transform_profiles_batch(raw_data)
        self.assertEqual(transformed, expected)
        self.assertEqual(mock_transform_single.call_count, 4) # transform_profile_data is called for each item

    @patch.object(TikTokDataTransformer, 'transform_profile_data', side_effect=[
        {"id": "1", "name": "User1"},
        Exception("Simulated transformation error"), # Simulate error
        {"id": "3", "name": "User3"} # This one should still be processed
    ])
    def test_transform_profiles_batch_with_errors(self, mock_transform_single):
        """Test transform_profiles_batch gère les erreurs de transformation individuelles"""
        raw_data = [{"raw1": 1}, {"raw2": 2}, {"raw3": 3}]
        expected = [{"id": "1", "name": "User1"}, {"id": "3", "name": "User3"}] # The profile causing error is skipped
        transformed = self.transformer.transform_profiles_batch(raw_data)
        self.assertEqual(transformed, expected)
        self.assertEqual(mock_transform_single.call_count, 3) # All items are attempted
        # We could also check that a warning message was printed if we mocked print

    def test_transform_profiles_batch_invalid_input_type(self):
        """Test transform_profiles_batch avec une entrée qui n'est pas une liste"""
        self.assertEqual(self.transformer.transform_profiles_batch(None), [])
        self.assertEqual(self.transformer.transform_profiles_batch({}), [])
        self.assertEqual(self.transformer.transform_profiles_batch("string"), [])


    # --- Tests for save_profiles ---
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('os.path.exists', return_value=True) # Assume path exists by default
    @patch('os.makedirs') # Mock makedirs
    def test_save_profiles_success(self, mock_makedirs, mock_json_dump, mock_file):
        """Test save_profiles réussit la sauvegarde"""
        profiles_data = [{"id": "1", "name": "User1"}]
        filename = "output/profiles.json"

        self.transformer.save_profiles(profiles_data, filename)

        # Check if open was called correctly
        mock_file.assert_called_once_with(filename, 'w', encoding='utf-8')
        # Get the file handle mock returned by open
        mock_handle = mock_file()
        # Check if json.dump was called with the correct data and handle
        mock_json_dump.assert_called_once_with(profiles_data, mock_handle, ensure_ascii=False, indent=2)
        # Check if makedirs was NOT called if path exists
        mock_makedirs.assert_not_called()

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('os.path.exists', return_value=False) # Simulate path doesn't exist
    @patch('os.makedirs') # Mock makedirs
    def test_save_profiles_create_directory(self, mock_makedirs, mock_json_dump, mock_file, mock_exists):
        """Test save_profiles crée le répertoire si nécessaire"""
        profiles_data = [{"id": "1", "name": "User1"}]
        filename = "new_dir/profiles.json"

        self.transformer.save_profiles(profiles_data, filename)

        mock_file.assert_called_once_with(filename, 'w', encoding='utf-8')
        mock_makedirs.assert_called_once_with("new_dir") # Check makedirs was called

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('os.path.exists', return_value=True)
    @patch('os.makedirs')
    def test_save_profiles_empty_list(self, mock_makedirs, mock_json_dump, mock_file):
        """Test save_profiles avec une liste de profils vide"""
        profiles_data = []
        filename = "profiles.json"

        self.transformer.save_profiles(profiles_data, filename)

        mock_file.assert_called_once_with(filename, 'w', encoding='utf-8')
        mock_handle = mock_file()
        mock_json_dump.assert_called_once_with([], mock_handle, ensure_ascii=False, indent=2) # Should dump empty list
        mock_makedirs.assert_not_called() # No directory needed

    def test_save_profiles_empty_filename(self):
        """Test save_profiles lève une erreur si le nom de fichier est vide"""
        profiles_data = [{"id": "1"}]
        with self.assertRaisesRegex(TikTokScraperError, "Filename for saving cannot be empty."):
            self.transformer.save_profiles(profiles_data, "")

    def test_save_profiles_invalid_filename_type(self):
        """Test save_profiles lève une erreur si le nom de fichier n'est pas une chaîne"""
        profiles_data = [{"id": "1"}]
        with self.assertRaisesRegex(TikTokScraperError, "Filename for saving cannot be empty or not a string."):
            self.transformer.save_profiles(profiles_data, None)
        with self.assertRaisesRegex(TikTokScraperError, "Filename for saving cannot be empty or not a string."):
            self.transformer.save_profiles(profiles_data, 123)


    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('os.path.exists', return_value=True)
    @patch('os.makedirs')
    def test_save_profiles_io_error(self, mock_makedirs, mock_json_dump, mock_file):
        """Test save_profiles lève TikTokScraperError sur IOError"""
        profiles_data = [{"id": "1"}]
        filename = "profiles.json"
        mock_file.side_effect = IOError("Permission denied")

        with self.assertRaisesRegex(TikTokScraperError, "Erreur lors de la sauvegarde du fichier 'profiles.json': Permission denied"):
            self.transformer.save_profiles(profiles_data, filename)

        mock_file.assert_called_once_with(filename, 'w', encoding='utf-8')
        mock_json_dump.assert_not_called() # json.dump shouldn't be called if open fails


    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('os.path.exists', return_value=True)
    @patch('os.makedirs')
    def test_save_profiles_json_type_error(self, mock_makedirs, mock_json_dump, mock_file):
        """Test save_profiles lève TikTokScraperError sur TypeError (JSON)"""
        # Include non-serializable data
        profiles_data = [{"id": "1", "complex_obj": set([1, 2])}]
        filename = "profiles.json"
        mock_json_dump.side_effect = TypeError("Object of type set is not JSON serializable")

        with self.assertRaisesRegex(TikTokScraperError, "Erreur de type lors de la sérialisation JSON du fichier 'profiles.json': Object of type set is not JSON serializable"):
            self.transformer.save_profiles(profiles_data, filename)

        mock_file.assert_called_once()
        mock_json_dump.assert_called_once() # json.dump should be called before raising error


    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    @patch('os.path.exists', return_value=True)
    @patch('os.makedirs', side_effect=Exception("Unexpected OS error")) # Simulate unexpected error during makedirs
    def test_save_profiles_unexpected_error(self, mock_makedirs, mock_json_dump, mock_file, mock_exists):
        """Test save_profiles lève TikTokScraperError sur une erreur inattendue"""
        profiles_data = [{"id": "1"}]
        filename = "profiles.json"

        with self.assertRaisesRegex(TikTokScraperError, "Une erreur inattendue s'est produite lors de la sauvegarde du fichier 'profiles.json': Unexpected OS error"):
            self.transformer.save_profiles(profiles_data, filename)

        mock_makedirs.assert_called_once() # Error happens during makedirs
        mock_file.assert_not_called()
        mock_json_dump.assert_not_called()

    # --- Tests for get_profiles_summary ---
    def test_get_profiles_summary_empty_list(self):
        """Test get_profiles_summary avec une liste vide"""
        self.transformer.get_profiles_summary([])
        captured_output = self._captured_output.getvalue()
        self.assertIn("Pas de profils à afficher.", captured_output)
        self.assertNotIn("📊", captured_output) # No header printed

    def test_get_profiles_summary_with_profiles(self):
        """Test get_profiles_summary affiche correctement le résumé"""
        profiles_data = [
            {
                "platform": "tiktok", "profile_id": "1", "profile_name": "User One",
                "page_name": "Page One", "url": "url1", "biography": "Bio 1",
                "metrics": {"likes": 1000, "followers": 5000, "video_count": 10},
                "is_verified": True, "scraped_at": "date1"
            },
             {
                "platform": "tiktok", "profile_id": "2", "profile_name": "User Two",
                "page_name": "", "url": "", "biography": "A longer bio that might get truncated if its too long for the print preview...",
                "metrics": {"likes": 50, "followers": 100, "video_count": 5},
                "is_verified": False, "scraped_at": "date2"
            }
        ]
        self.transformer.get_profiles_summary(profiles_data)
        captured_output = self._captured_output.getvalue()

        self.assertIn("📊 RÉSUMÉ DES PROFILS TIKTOK:", captured_output)
        self.assertIn("📝 Nom d'affichage: User One", captured_output)
        self.assertIn("🆔 ID: 1", captured_output)
        self.assertIn("🔗 URL: url1", captured_output)
        self.assertIn("👥 Followers: 5,000", captured_output) # Check formatting
        self.assertIn("💖 Likes totaux: 1,000", captured_output) # Check formatting
        self.assertIn("✅ Vérifié: Oui", captured_output)
        self.assertIn("📄 Bio: Bio 1", captured_output)

        self.assertIn("📝 Nom d'affichage: User Two", captured_output)
        self.assertIn("🆔 ID: 2", captured_output)
        # Should not print URL or Page Name if empty string, but bio should be printed
        self.assertNotIn("🔗 URL: ", captured_output)
        # Biography should be truncated if long
        self.assertIn("📄 Bio: A longer bio that might get truncated if its too long for the print preview...", captured_output)
        self.assertIn("...", captured_output) # Check for truncation marker

        self.assertIn("👥 Followers: 100", captured_output) # Check formatting
        self.assertIn("💖 Likes totaux: 50", captured_output) # Check formatting
        self.assertIn("✅ Vérifié: Non", captured_output)

        self.assertEqual(captured_output.count("=" * 60), 1) # Header line
        self.assertEqual(captured_output.count("-" * 60), 2) # Separator lines for each profile

    def test_get_profiles_summary_with_invalid_profile_in_list(self):
        """Test get_profiles_summary gère les profils invalides dans la liste"""
        profiles_data = [
            {"platform": "tiktok", "profile_id": "1", "profile_name": "User One", "metrics": {"followers": 100}, "is_verified": False, "scraped_at": "date1"}, # Valid enough for summary
            "not a dict", # Invalid item
            {"profile_id": "2", "profile_name": "User Two"}, # Missing metrics, etc.
        ]
        self.transformer.get_profiles_summary(profiles_data)
        captured_output = self._captured_output.getvalue()

        self.assertIn("📝 Nom d'affichage: User One", captured_output)
        # Invalid items should not produce full summaries
        self.assertNotIn("📝 Nom d'affichage: User Two", captured_output)
        # Check for skipping messages (if prints were enabled, but they are not in the testable code)
        # Or check that only one separator line was printed
        self.assertEqual(captured_output.count("-" * 60), 1) # Only one valid profile summary expected


    def test_get_profiles_summary_invalid_input_type(self):
        """Test get_profiles_summary avec une entrée qui n'est pas une liste"""
        self.transformer.get_profiles_summary(None)
        captured_output = self._captured_output.getvalue()
        self.assertIn("Pas de profils à afficher.", captured_output)

        self._captured_output.truncate(0) # Reset captured output
        self._captured_output.seek(0)
        self.transformer.get_profiles_summary({})
        captured_output = self._captured_output.getvalue()
        self.assertIn("Pas de profils à afficher.", captured_output)


    # --- Tests for validate_profile_data ---
    def test_validate_profile_data_valid(self):
        """Test validate_profile_data avec un profil valide"""
        valid_profile = {
            "platform": "tiktok",
            "profile_id": "123",
            "profile_name": "User",
            "page_name": "Page",
            "url": "url",
            "biography": "bio",
            "metrics": {"likes": 100, "followers": 500, "video_count": 10},
            "is_verified": False,
            "scraped_at": "2023-10-27T10:00:00"
        }
        self.assertTrue(self.transformer.validate_profile_data(valid_profile))

    def test_validate_profile_data_invalid_type(self):
        """Test validate_profile_data avec une entrée invalide (pas un dict)"""
        self.assertFalse(self.transformer.validate_profile_data(None))
        self.assertFalse(self.transformer.validate_profile_data([]))
        self.assertFalse(self.transformer.validate_profile_data("string"))
        self.assertFalse(self.transformer.validate_profile_data(123))


    def test_validate_profile_data_missing_required_field(self):
        """Test validate_profile_data avec un champ requis manquant"""
        valid_profile = {
            "platform": "tiktok", "profile_id": "123", "profile_name": "User",
            "page_name": "Page", "url": "url", "biography": "bio",
            "metrics": {"likes": 100, "followers": 500, "video_count": 10},
            "is_verified": False, "scraped_at": "2023-10-27T10:00:00"
        }
        # Remove 'platform'
        del valid_profile['platform']
        self.assertFalse(self.transformer.validate_profile_data(valid_profile))

    def test_validate_profile_data_metrics_not_dict(self):
        """Test validate_profile_data si 'metrics' n'est pas un dictionnaire"""
        valid_profile = {
            "platform": "tiktok", "profile_id": "123", "profile_name": "User",
            "page_name": "Page", "url": "url", "biography": "bio",
            "metrics": "not a dict", # Invalid metrics type
            "is_verified": False, "scraped_at": "2023-10-27T10:00:00"
        }
        self.assertFalse(self.transformer.validate_profile_data(valid_profile))

    def test_validate_profile_data_missing_metric(self):
        """Test validate_profile_data avec une métrique manquante"""
        valid_profile = {
            "platform": "tiktok", "profile_id": "123", "profile_name": "User",
            "page_name": "Page", "url": "url", "biography": "bio",
            "metrics": {"likes": 100, "followers": 500}, # Missing video_count
            "is_verified": False, "scraped_at": "2023-10-27T10:00:00"
        }
        self.assertFalse(self.transformer.validate_profile_data(valid_profile))

    def test_validate_profile_data_metric_wrong_type(self):
        """Test validate_profile_data avec une métrique du mauvais type"""
        valid_profile = {
            "platform": "tiktok", "profile_id": "123", "profile_name": "User",
            "page_name": "Page", "url": "url", "biography": "bio",
            "metrics": {"likes": 100, "followers": "500", "video_count": 10}, # Followers is string
            "is_verified": False, "scraped_at": "2023-10-27T10:00:00"
        }
        self.assertFalse(self.transformer.validate_profile_data(valid_profile))

    def test_validate_profile_data_required_field_wrong_type(self):
         """Test validate_profile_data avec un champ requis du mauvais type"""
         valid_profile = {
            "platform": "tiktok", "profile_id": 123, "profile_name": "User", # profile_id is int
            "page_name": "Page", "url": "url", "biography": "bio",
            "metrics": {"likes": 100, "followers": 500, "video_count": 10},
            "is_verified": "False", "scraped_at": "2023-10-27T10:00:00" # is_verified is string
         }
         self.assertFalse(self.transformer.validate_profile_data(valid_profile)) # Fails on profile_id
         valid_profile["profile_id"] = "123"
         self.assertFalse(self.transformer.validate_profile_data(valid_profile)) # Now fails on is_verified


    # --- Tests for filter_profiles_by_criteria ---
    @patch.object(TikTokDataTransformer, 'validate_profile_data', return_value=True) # Assume all inputs are structurally valid for filtering logic
    def test_filter_profiles_by_criteria_no_criteria(self, mock_validate):
        """Test filter_profiles_by_criteria sans critères (retourne tout si valide)"""
        profiles_data = [
             {"id": "1", "metrics": {"followers": 100}, "is_verified": False},
             {"id": "2", "metrics": {"followers": 500}, "is_verified": True},
        ]
        filtered = self.transformer.filter_profiles_by_criteria(profiles_data)
        self.assertEqual(filtered, profiles_data)
        self.assertEqual(mock_validate.call_count, len(profiles_data))

    @patch.object(TikTokDataTransformer, 'validate_profile_data', return_value=True)
    def test_filter_profiles_by_criteria_min_followers(self, mock_validate):
        """Test filter_profiles_by_criteria par min_followers"""
        profiles_data = [
             {"id": "1", "metrics": {"followers": 100}, "is_verified": False},
             {"id": "2", "metrics": {"followers": 500}, "is_verified": True},
             {"id": "3", "metrics": {"followers": 1500}, "is_verified": False},
        ]
        expected = [
            {"id": "2", "metrics": {"followers": 500}, "is_verified": True},
            {"id": "3", "metrics": {"followers": 1500}, "is_verified": False},
        ]
        filtered = self.transformer.filter_profiles_by_criteria(profiles_data, min_followers=400)
        self.assertEqual(filtered, expected)
        self.assertEqual(mock_validate.call_count, len(profiles_data))


    @patch.object(TikTokDataTransformer, 'validate_profile_data', return_value=True)
    def test_filter_profiles_by_criteria_verified_only(self, mock_validate):
        """Test filter_profiles_by_criteria par verified_only"""
        profiles_data = [
             {"id": "1", "metrics": {"followers": 100}, "is_verified": False},
             {"id": "2", "metrics": {"followers": 500}, "is_verified": True},
             {"id": "3", "metrics": {"followers": 1500}, "is_verified": False},
        ]
        expected = [
            {"id": "2", "metrics": {"followers": 500}, "is_verified": True},
        ]
        filtered = self.transformer.filter_profiles_by_criteria(profiles_data, verified_only=True)
        self.assertEqual(filtered, expected)
        self.assertEqual(mock_validate.call_count, len(profiles_data))


    @patch.object(TikTokDataTransformer, 'validate_profile_data', return_value=True)
    def test_filter_profiles_by_criteria_both_criteria(self, mock_validate):
        """Test filter_profiles_by_criteria par min_followers ET verified_only"""
        profiles_data = [
             {"id": "1", "metrics": {"followers": 100}, "is_verified": False}, # Too few followers
             {"id": "2", "metrics": {"followers": 500}, "is_verified": False}, # Enough followers, not verified
             {"id": "3", "metrics": {"followers": 1500}, "is_verified": True}, # Enough followers, verified
             {"id": "4", "metrics": {"followers": 2000}, "is_verified": True}, # Enough followers, verified
        ]
        expected = [
            {"id": "3", "metrics": {"followers": 1500}, "is_verified": True},
            {"id": "4", "metrics": {"followers": 2000}, "is_verified": True},
        ]
        filtered = self.transformer.filter_profiles_by_criteria(profiles_data, min_followers=1000, verified_only=True)
        self.assertEqual(filtered, expected)
        self.assertEqual(mock_validate.call_count, len(profiles_data))


    def test_filter_profiles_by_criteria_empty_list(self):
        """Test filter_profiles_by_criteria avec une liste vide"""
        filtered = self.transformer.filter_profiles_by_criteria([], min_followers=100)
        self.assertEqual(filtered, [])

    def test_filter_profiles_by_criteria_invalid_input_type(self):
        """Test filter_profiles_by_criteria avec une entrée qui n'est pas une liste"""
        self.assertEqual(self.transformer.filter_profiles_by_criteria(None), [])
        self.assertEqual(self.transformer.filter_profiles_by_criteria({}), [])


    def test_filter_profiles_by_criteria_with_invalid_profiles_in_list(self):
         """Test filter_profiles_by_criteria filtre les profils structurellement invalides avant les critères"""
         profiles_data = [
            {"platform": "tiktok", "profile_id": "1", "profile_name": "Valid User",
             "metrics": {"likes": 100, "followers": 500, "video_count": 10}, "is_verified": True, "scraped_at": "date1"}, # Valid + meets criteria
            {"profile_id": "2", "profile_name": "Invalid User Missing Metrics", "is_verified": True}, # Invalid structure
            {"platform": "tiktok", "profile_id": "3", "profile_name": "Valid User, Fails Criteria",
             "metrics": {"likes": 10, "followers": 50, "video_count": 2}, "is_verified": False, "scraped_at": "date2"}, # Valid + Fails criteria
            "not a profile dict" # Invalid structure
         ]
         expected = [
             {"platform": "tiktok", "profile_id": "1", "profile_name": "Valid User",
              "metrics": {"likes": 100, "followers": 500, "video_count": 10}, "is_verified": True, "scraped_at": "date1"},
         ]

         # Filter for min_followers=100 and verified_only=True
         filtered = self.transformer.filter_profiles_by_criteria(profiles_data, min_followers=100, verified_only=True)
         self.assertEqual(filtered, expected)
         # validate_profile_data should have been called for the 3 list items that are dicts


# --- Exécution des tests ---
if __name__ == '__main__':
    # Run tests and capture output for the report
    # sys.stdout is already captured by the setUp method of the test class

    print("Starting tests...")
    # Using TextTestRunner without stream argument will print to the captured sys.stdout
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(unittest.makeSuite(TestTikTokDataTransformer))


    # Restore original stdout temporarily to print the final report structure
    _original_stdout = sys.stdout
    sys.stdout = sys.__stdout__ # Directly access the original stdout

    # --- Génération du Rapport ---
    print("\n")
    print("=========================================")
    print("      RAPPORT DE TEST TikTokDataTransformer")
    print("=========================================")
    print("\n")
    print("Outil de test: unittest (Python Standard Library)")
    print("Méthodologie: Tests unitaires avec mocking des dépendances externes (datetime, os, json)")
    print("\n")
    print("Résumé des tests :")
    print(f"- Nombre total de tests exécutés : {result.testsRun}")
    print(f"- Succès : {result.testsRun - len(result.errors) - len(result.failures)}")
    print(f"- Échecs : {len(result.failures)}")
    print(f"- Erreurs (exceptions imprévues) : {len(result.errors)}")
    print("\n")

    if result.wasSuccessful():
        print("Conclusion : TOUS LES TESTS ONT RÉUSSI.")
        print("La classe TikTokDataTransformer semble fonctionner correctement selon les scénarios testés.")
    else:
        print("Conclusion : CERTAINS TESTS ONT ÉCHOUÉ OU ONT RENCONTRÉ DES ERREURS.")
        print("Des problèmes ont été détectés dans l'implémentation. Voir les détails ci-dessous.")

    print("\nDétails de l'exécution des tests :")
    print("-----------------------------------------")
    # Print the captured output from the TextTestRunner
    # Note: TextTestRunner already prints to the captured_output StringIO
    # so we need to access that StringIO from the test instance *after* the run.
    # However, since we ran the runner directly, the captured output is in the
    # global StringIO we set up. Let's get it.
    global _captured_output # Assuming _captured_output was declared globally or accessible
    # The current setup puts captured output in the instance's _captured_output
    # If we run via unittest.main(), the instance is created internally.
    # A simpler way for the report is to just let TextTestRunner print to stdout,
    # and restore stdout *before* printing the summary report structure.
    # Let's refactor the main block slightly.
    pass # The stdout capture setup in setUp/tearDown is mainly for testing print *inside* methods.
         # For the runner output, the previous method is fine.

    # Restore stdout to print the report structure itself
    sys.stdout = _original_stdout
    # print("Test runner output below:") # Optional marker

    # Need to re-run with capture only for the runner output if needed in the report structure.
    # Simpler: Just print the collected failures/errors from the result object.

    if result.failures:
        print("\nDÉTAILS DES ÉCHECS :\n")
        print("---------------------\n")
        for test, trace in result.failures:
            print(f"Test échoué : {test}\n")
            print(f"Trace :\n{trace}\n")
            print("---------------------\n")

    if result.errors:
        print("\nDÉTAILS DES ERREURS :\n")
        print("---------------------\n")
        for test, trace in result.errors:
            print(f"Erreur dans le test : {test}\n")
            print(f"Trace :\n{trace}\n")
            print("---------------------\n")

    # The detailed runner output is lost in this simpler report approach.
    # To include it, the TextTestRunner needed to write to the captured_output StringIO.
    # Let's put the original capturing logic back in the __main__ block for the runner output.


if __name__ == '__main__':
    # Setup stdout capture for the test runner output
    old_stdout = sys.stdout
    sys.stdout = captured_output = StringIO()

    print("Starting tests...") # This goes to the captured output

    # Need to temporarily disable the *instance-level* stdout capture
    # while the runner is running, if get_profiles_summary tests are included.
    # A cleaner way is to handle stdout capture *only* within the specific test method
    # (get_profiles_summary) using a context manager or fixture, rather than in setUp/tearDown.

    # Let's simplify: disable the setUp/tearDown stdout capture for now,
    # and handle stdout capture only in the relevant test method using patch.object(sys, 'stdout').
    # And use the original __main__ block capture for the overall runner output report.

    # (Reverting setUp/tearDown stdout capture from the test class for this __main__ block approach)
    # (Ensure you remove the stdout capture setup from setUp/tearDown methods above)

    # --- Re-running with simpler __main__ stdout capture ---
    # (Assuming setUp/tearDown methods in the test class do NOT capture stdout globally anymore)

    # --- Renable the setUp/tearDown stdout capture if needed for get_profiles_summary ---
    # Okay, decided to keep the setUp/tearDown capture for `get_profiles_summary` only.
    # This means the `TextTestRunner` output *also* goes to the captured buffer.
    # We need to retrieve the buffer content *after* the run.

    # Re-setup the global StringIO for the runner output capture
    old_stdout = sys.stdout
    runner_output_capture = StringIO()
    sys.stdout = runner_output_capture

    print("Starting tests...") # This will go to runner_output_capture

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestTikTokDataTransformer)
    # Verbosity 2 shows individual test names and status (ok, FAIL, ERROR)
    runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2)
    result = runner.run(suite)

    # Get the captured runner output
    captured_runner_output_string = runner_output_capture.getvalue()

    # Restore original stdout
    sys.stdout = old_stdout

    # --- Génération du Rapport (using captured runner output) ---
    report = "\n\n"
    report += "=========================================\n"
    report += "      RAPPORT DE TEST TikTokDataTransformer\n"
    report += "=========================================\n"
    report += "\n"
    report += "Outil de test: unittest (Python Standard Library)\n"
    report += "Méthodologie: Tests unitaires avec mocking des dépendances externes (datetime, os, json)\n"
    report += "\n"
    report += "Résumé des tests :\n"
    report += f"- Nombre total de tests exécutés : {result.testsRun}\n"
    report += f"- Succès : {result.testsRun - len(result.errors) - len(result.failures)}\n"
    report += f"- Échecs : {len(result.failures)}\n"
    report += f"- Erreurs (exceptions imprévues) : {len(result.errors)}\n"
    report += "\n"

    if result.wasSuccessful():
        report += "Conclusion : TOUS LES TESTS ONT RÉUSSI.\n"
        report += "La classe TikTokDataTransformer semble fonctionner correctement selon les scénarios testés.\n"
    else:
        report += "Conclusion : CERTAINS TESTS ONT ÉCHOUÉ OU ONT RENCONTRÉ DES ERREURS.\n"
        report += "Des problèmes ont été détectés dans l'implémentation. Voir les détails ci-dessous.\n"

    report += "\nDétails de l'exécution des tests :\n"
    report += "-----------------------------------------\n"
    report += captured_runner_output_string # Include the output from TextTestRunner
    report += "-----------------------------------------\n"

    # failures and errors details are already in the TextTestRunner output for verbosity=2
    # but let's add the separate details section as requested for clarity

    if result.failures:
        report += "\nDÉTAILS DES ÉCHECS :\n"
        report += "---------------------\n"
        # result.failures is a list of (test, traceback_string) tuples
        for test, trace in result.failures:
            report += f"Test échoué : {test}\n"
            report += f"Trace :\n{trace}\n"
            report += "---------------------\n"

    if result.errors:
        report += "\nDÉTAILS DES ERREURS :\n"
        report += "---------------------\n"
        for test, trace in result.errors:
            report += f"Erreur dans le test : {test}\n"
            report += f"Trace :\n{trace}\n"
            report += "---------------------\n"

    print(report)