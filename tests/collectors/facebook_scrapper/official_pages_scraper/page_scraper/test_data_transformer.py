import pytest
from src.collectors.facebook_scrapper.official_pages_scraper.page_scraper.data_transformer import *
import unittest
import json
import os
import tempfile
from datetime import datetime
from unittest import mock
import io
import sys

MOCK_DATETIME_TARGET = 'data_transformer.datetime.datetime'
#
MOCK_DATETIME_TARGET = 'src.collectors.facebook_scrapper.official_pages_scraper.page_scraper.data_transformer.datetime.datetime'


class TestFacebookDataTransformer(unittest.TestCase):

    def setUp(self):
        """Set up test data before each test method."""
        # Sample raw data
        self.valid_raw_page = {
            "pageId": "12345",
            "title": "Test Page Title",
            "pageName": "test_page",
            "pageUrl": "https://www.facebook.com/testpage",
            "creation_date": "2020-01-15",
            "intro": "This is a test page introduction.",
            "likes": 1000,
            "followers": 1500,
            "pageAdLibrary": {
                "is_business_page_active": True,
                "some_other_field": "value"
            }
        }

        self.minimal_raw_page = {
            "pageId": "67890",
            "title": "Minimal Page",
            "pageUrl": "https://www.facebook.com/minimalpage",
        }

        self.no_ad_library_page = {
            "pageId": "11223",
            "title": "No Ad Library",
            "pageUrl": "https://www.facebook.com/noadlib",
            "likes": 50,
            "followers": 75,
            # No pageAdLibrary field
        }

        self.non_business_page = {
            "pageId": "44556",
            "title": "Non Business Page",
            "pageUrl": "https://www.facebook.com/nonbiz",
            "likes": 200,
            "followers": 300,
            "pageAdLibrary": {
                "is_business_page_active": False
            }
        }

        # Invalid input examples
        self.invalid_raw_page_type = "This is not a dict"
        self.invalid_batch_input_type = "This is not a list"
        self.invalid_item_in_batch = 12345 # Example of non-dict item in list


        # Mock datetime.now to have predictable scraped_at timestamps
        self.mock_datetime = datetime(2023, 10, 27, 10, 30, 0)
        # Patch the datetime.datetime class from the module where it's used by the Transformer
        self.patcher_datetime = mock.patch(MOCK_DATETIME_TARGET, new=mock.MagicMock())
        self.mock_dt_class = self.patcher_datetime.start()
        # Make datetime.datetime.now() return our specific mock datetime object
        # The real datetime object returned by now() will then correctly call its own .isoformat()
        self.mock_dt_class.now.return_value = self.mock_datetime


        # Capture stdout and stderr for tests that print
        self._captured_stdout = io.StringIO()
        self._captured_stderr = io.StringIO()
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        sys.stdout = self._captured_stdout
        sys.stderr = self._captured_stderr


    def tearDown(self):
        """Clean up after each test method."""
        # Stop capturing stdout/stderr and restore
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr

        # Stop the datetime patcher
        self.patcher_datetime.stop()

        # Clean up any temporary files created during tests
        if hasattr(self, 'temp_file_path') and os.path.exists(self.temp_file_path):
             try:
                 os.remove(self.temp_file_path)
                 # Also remove the directory if it's temporary and empty
                 temp_dir = os.path.dirname(self.temp_file_path)
                 if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                     os.rmdir(temp_dir)
             except OSError as e:
                 # Print a warning to original stderr if temp file cleanup fails
                 self._original_stderr.write(f"Warning: Could not remove temporary file {self.temp_file_path}: {e}\n")


    # --- Test for _safe_get_nested ---
    def test__safe_get_nested_success(self):
        data = {"a": {"b": {"c": 123}}}
        result = FacebookDataTransformer._safe_get_nested(data, "a", "b", "c")
        self.assertEqual(result, 123)

    def test__safe_get_nested_missing_intermediate_key(self):
        data = {"a": {"x": {"c": 123}}}
        result = FacebookDataTransformer._safe_get_nested(data, "a", "b", "c", default="missing")
        self.assertEqual(result, "missing")

    def test__safe_get_nested_missing_final_key(self):
        data = {"a": {"b": {"x": 123}}}
        result = FacebookDataTransformer._safe_get_nested(data, "a", "b", "c", default="missing")
        self.assertEqual(result, "missing")

    def test__safe_get_nested_non_dict_input(self):
        # Test that input data that is not a dict returns the default immediately
        data = ["a", "b"]
        result = FacebookDataTransformer._safe_get_nested(data, 0, default="missing")
        self.assertEqual(result, "missing")

    def test__safe_get_nested_none_input(self):
        data = None
        result = FacebookDataTransformer._safe_get_nested(data, "a", default="missing")
        self.assertEqual(result, "missing")

    def test__safe_get_nested_non_dict_intermediate(self):
        # Test that if a value along the path is not a dict, it returns the default
        data = {"a": "not a dict", "b": {"c": 123}}
        result = FacebookDataTransformer._safe_get_nested(data, "a", "b", "c", default="missing")
        self.assertEqual(result, "missing")

    def test__safe_get_nested_default_none(self):
        data = {"a": {}}
        result = FacebookDataTransformer._safe_get_nested(data, "a", "b")
        self.assertIsNone(result)

    def test__safe_get_nested_empty_keys(self):
        # Test calling with no keys should return the original data
        data = {"a": 1}
        result = FacebookDataTransformer._safe_get_nested(data, default="missing")
        self.assertEqual(result, data)


    # --- Test for transform_page_data ---
    def test_transform_page_data_valid(self):
        expected_output = {
            "platform": "facebook",
            "profile_id": "12345",
            "profile_name": "Test Page Title",
            "page_name": "test_page",
            "url": "https://www.facebook.com/testpage",
            "creation_date": "2020-01-15",
            "biography": "This is a test page introduction.",
            "metrics": {
                "likes": 1000,
                "followers": 1500,
            },
            "is_business_account": True, # Should be boolean
            "scraped_at": self.mock_datetime.isoformat() # Expect the mocked time
        }
        transformed = FacebookDataTransformer.transform_page_data(self.valid_raw_page)
        self.assertDictEqual(transformed, expected_output)
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


    def test_transform_page_data_minimal(self):
        expected_output = {
            "platform": "facebook",
            "profile_id": "67890",
            "profile_name": "Minimal Page",
            "page_name": None, # Missing in raw
            "url": "https://www.facebook.com/minimalpage",
            "creation_date": None, # Missing in raw
            "biography": None, # Missing in raw
            "metrics": {
                "likes": 0, # Default value
                "followers": 0, # Default value
            },
            "is_business_account": False, # Default from _safe_get_nested when pageAdLibrary is missing, should be boolean
            "scraped_at": self.mock_datetime.isoformat()
        }
        transformed = FacebookDataTransformer.transform_page_data(self.minimal_raw_page)
        self.assertDictEqual(transformed, expected_output)
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


    def test_transform_page_data_no_ad_library(self):
        expected_output = {
            "platform": "facebook",
            "profile_id": "11223",
            "profile_name": "No Ad Library",
            "page_name": None,
            "url": "https://www.facebook.com/noadlib",
            "creation_date": None,
            "biography": None,
            "metrics": {
                "likes": 50,
                "followers": 75,
            },
            "is_business_account": False, # Default because pageAdLibrary is missing, should be boolean
            "scraped_at": self.mock_datetime.isoformat()
        }
        transformed = FacebookDataTransformer.transform_page_data(self.no_ad_library_page)
        self.assertDictEqual(transformed, expected_output)
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


    def test_transform_page_data_non_business(self):
        expected_output = {
            "platform": "facebook",
            "profile_id": "44556",
            "profile_name": "Non Business Page",
            "page_name": None,
            "url": "https://www.facebook.com/nonbiz",
            "creation_date": None,
            "biography": None,
            "metrics": {
                "likes": 200,
                "followers": 300,
            },
            "is_business_account": False, # Explicitly False, should be boolean
            "scraped_at": self.mock_datetime.isoformat()
        }
        transformed = FacebookDataTransformer.transform_page_data(self.non_business_page)
        self.assertDictEqual(transformed, expected_output)
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected

    def test_transform_page_data_weird_is_business_value(self):
         # Test case where is_business_page_active is not a boolean
         raw_page = self.valid_raw_page.copy()
         raw_page['pageId'] = "weird_biz"
         raw_page['title'] = "Weird Biz Page"
         raw_page['pageAdLibrary']['is_business_page_active'] = "maybe" # Not boolean
         
         expected_output = {
            "platform": "facebook",
            "profile_id": "weird_biz",
            "profile_name": "Weird Biz Page",
            "page_name": "test_page", # Copied from valid_raw_page
            "url": "https://www.facebook.com/testpage", # Copied
            "creation_date": "2020-01-15", # Copied
            "biography": "This is a test page introduction.", # Copied
            "metrics": {
                "likes": 1000, # Copied
                "followers": 1500, # Copied
            },
            "is_business_account": True, # bool("maybe") is True
            "scraped_at": self.mock_datetime.isoformat()
         }
         transformed = FacebookDataTransformer.transform_page_data(raw_page)
         self.assertDictEqual(transformed, expected_output)
         self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


    def test_transform_page_data_empty_dict(self):
        expected_output = {
            "platform": "facebook",
            "profile_id": None,
            "profile_name": None,
            "page_name": None,
            "url": None,
            "creation_date": None,
            "biography": None,
            "metrics": {
                "likes": 0,
                "followers": 0,
            },
            "is_business_account": False,
            "scraped_at": self.mock_datetime.isoformat()
        }
        transformed = FacebookDataTransformer.transform_page_data({})
        self.assertDictEqual(transformed, expected_output)
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


    def test_transform_page_data_invalid_input(self):
         # Test handling of non-dict input
         expected_output = { # Based on the added handling in the class
                 "platform": "facebook",
                 "profile_id": None,
                 "profile_name": None,
                 "page_name": None,
                 "url": None,
                 "creation_date": None,
                 "biography": None,
                 "metrics": {"likes": 0, "followers": 0},
                 "is_business_account": False,
                 "scraped_at": self.mock_datetime.isoformat() # Should still have the mocked timestamp
             }
         transformed = FacebookDataTransformer.transform_page_data(self.invalid_raw_page_type)
         self.assertDictEqual(transformed, expected_output)
         # Check if the warning message for the invalid entry was printed to stderr
         stderr_output = self._captured_stderr.getvalue()
         self.assertIn("⚠️ Attention: transform_page_data reçu des données non-dict: str - This is not a dict", stderr_output)


    # --- Test for transform_pages_batch ---
    def test_transform_pages_batch_valid_list(self):
        raw_pages = [self.valid_raw_page, self.minimal_raw_page, self.non_business_page]
        transformed_batch = FacebookDataTransformer.transform_pages_batch(raw_pages)

        self.assertEqual(len(transformed_batch), 3)
        # Verify content of the first transformed page
        self.assertEqual(transformed_batch[0]["profile_id"], "12345")
        self.assertTrue(transformed_batch[0]["is_business_account"])
        # Verify content of the second transformed page
        self.assertEqual(transformed_batch[1]["profile_id"], "67890")
        self.assertEqual(transformed_batch[1]["metrics"]["likes"], 0)
        # Verify content of the third transformed page
        self.assertEqual(transformed_batch[2]["profile_id"], "44556")
        self.assertFalse(transformed_batch[2]["is_business_account"])
        # Check scraped_at timestamp for a few items
        self.assertEqual(transformed_batch[0]["scraped_at"], self.mock_datetime.isoformat())
        self.assertEqual(transformed_batch[1]["scraped_at"], self.mock_datetime.isoformat())

        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


    def test_transform_pages_batch_with_invalid_entry(self):
        # Mix valid and invalid entries
        raw_pages = [
            self.valid_raw_page,
            self.invalid_raw_page_type, # Non-dict type
            {"url": "http://bad-page", "likes": "not a number"}, # Valid dict structure, but potentially invalid data types inside
            self.minimal_raw_page,
            None # Another non-dict type
        ]
        transformed_batch = FacebookDataTransformer.transform_pages_batch(raw_pages)

        # transform_page_data is robust and returns a default dict for non-dict types.
        # It passes dicts through, even if internal data types might be unexpected (like "not a number").
        # The batch loop appends whatever transform_page_data returns.
        self.assertEqual(len(transformed_batch), len(raw_pages)) # All items processed, including default results for invalid ones

        self.assertEqual(transformed_batch[0]["profile_id"], "12345") # Valid page
        self.assertEqual(transformed_batch[1]["profile_id"], None) # Invalid string input result (default output)
        self.assertEqual(transformed_batch[1]["scraped_at"], self.mock_datetime.isoformat()) # Check timestamp on default output
        self.assertEqual(transformed_batch[2]["url"], "http://bad-page") # Page with bad internal data
        self.assertEqual(transformed_batch[2]["metrics"]["likes"], "not a number") # Bad data preserved by .get() - summary will handle this
        self.assertEqual(transformed_batch[3]["profile_id"], "67890") # Valid page
        self.assertEqual(transformed_batch[4]["profile_id"], None) # Invalid None input result (default output)
        self.assertEqual(transformed_batch[4]["scraped_at"], self.mock_datetime.isoformat()) # Check timestamp on default output


        # Check stderr output for warnings from transform_page_data for invalid types
        stderr_output = self._captured_stderr.getvalue()
        self.assertIn("⚠️ Attention: transform_page_data reçu des données non-dict: str - This is not a dict", stderr_output)
        self.assertIn("⚠️ Attention: transform_page_data reçu des données non-dict: NoneType - None", stderr_output)
        # No error expected in the batch loop's *catch* block because transform_page_data is robust to these inputs
        self.assertNotIn("⚠️ Erreur inattendue lors de la transformation d'une page", stderr_output)


    def test_transform_pages_batch_with_unexpected_error(self):
        # Test an item that might cause a specific exception *within* transform_page_data
        # that isn't just caught by the initial non-dict check.
        # Example: Causing a division by zero if we had a field that did that.
        # Or forcing an AttributeError *after* the dict check.
        class BadDict(dict): # Create a dict-like object that will fail later
            def get(self, key, default=None):
                if key == 'pageId': return "bad_dict_page"
                if key == 'pageAdLibrary': return {"is_business_page_active": True} # This part is fine
                if key == 'intro':
                     # This will cause an AttributeError when str.split() is called later if we added such logic
                     # For now, let's simulate an error inside transform_page_data
                     raise ValueError("Simulated error during transformation")
                return super().get(key, default)


        raw_pages = [
            self.valid_raw_page, # Expected to transform successfully
            BadDict(self.minimal_raw_page), # Use the custom dict that raises error
            self.minimal_raw_page # Expected to transform successfully
        ]
        transformed_batch = FacebookDataTransformer.transform_pages_batch(raw_pages)

        # Expected output: 2 transformed pages (valid, minimal).
        # The BadDict item should trigger the EXCEPT block in transform_pages_batch and be SKIPPED.
        self.assertEqual(len(transformed_batch), 2)
        self.assertEqual(transformed_batch[0]["profile_id"], "12345")
        self.assertEqual(transformed_batch[1]["profile_id"], "67890")

        # Check stderr for the error message from the batch loop's catch block
        stderr_output = self._captured_stderr.getvalue()
        self.assertIn("⚠️ Erreur inattendue lors de la transformation d'une page (bad_dict_page): Simulated error during transformation", stderr_output)
        self.assertIn("Page brute:", stderr_output) # Check that raw data is printed


    def test_transform_pages_batch_empty_list(self):
        transformed_batch = FacebookDataTransformer.transform_pages_batch([])
        self.assertEqual(len(transformed_batch), 0)
        self.assertListEqual(transformed_batch, [])
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected

    def test_transform_pages_batch_invalid_input_type(self):
        # Test input that is not a list
        transformed_batch = FacebookDataTransformer.transform_pages_batch(self.invalid_batch_input_type)
        self.assertEqual(len(transformed_batch), 0)
        self.assertListEqual(transformed_batch, [])
        # Check if the warning message for the invalid input type was printed to stderr
        stderr_output = self._captured_stderr.getvalue()
        self.assertIn("⚠️ Attention: transform_pages_batch reçu des données non-liste: str - This is not a list", stderr_output)


    # --- Test for save_pages ---
    def test_save_pages_success(self):
        # Create a temporary file path
        fd, self.temp_file_path = tempfile.mkstemp(suffix=".json")
        os.close(fd) # Close the file descriptor as we'll open it again with json

        pages_to_save = [
            FacebookDataTransformer.transform_page_data(self.valid_raw_page),
            FacebookDataTransformer.transform_page_data(self.minimal_raw_page)
        ]

        # Ensure stdout/stderr capture is active during the call
        # We mock print specifically to check the *success* message printed to stdout
        # as it's one of the few things that goes to stdout besides the summary
        with mock.patch('builtins.print') as mock_print:
             FacebookDataTransformer.save_pages(pages_to_save, self.temp_file_path)


        # Verify print output (success message to stdout)
        mock_print.assert_called_once_with(f"💾 Pages sauvegardées dans '{self.temp_file_path}'")
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected


        # Verify file content
        with open(self.temp_file_path, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)

        self.assertListEqual(loaded_data, pages_to_save)


    def test_save_pages_empty_list(self):
         # Create a temporary file path
        fd, self.temp_file_path = tempfile.mkstemp(suffix=".json")
        os.close(fd) # Close the file descriptor

        # We mock print specifically to check the *success* message printed to stdout
        with mock.patch('builtins.print') as mock_print:
            FacebookDataTransformer.save_pages([], self.temp_file_path)


        # Verify print output
        mock_print.assert_called_once_with(f"💾 Pages sauvegardées dans '{self.temp_file_path}'")
        self.assertEqual(self._captured_stderr.getvalue(), "") # No stderr output expected

        # Verify file content (should be an empty JSON list)
        with open(self.temp_file_path, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)

        self.assertListEqual(loaded_data, [])

    def test_save_pages_invalid_input_type(self):
        # Test input that is not a list
        # Creating a temp file just to avoid IOError on path itself
        fd, self.temp_file_path = tempfile.mkstemp(suffix=".json")
        os.close(fd)

        # Ensure stdout/stderr capture is active during the call
        # We mock print specifically to check the *success* message printed to stdout
        with mock.patch('builtins.print') as mock_print:
             FacebookDataTransformer.save_pages(self.invalid_batch_input_type, self.temp_file_path)

        # Check if the warning message for the invalid input type was printed to stderr
        stderr_output = self._captured_stderr.getvalue()
        self.assertIn("⚠️ Attention: save_pages reçu des données non-liste: str - This is not a list. Sauvegarde d'une liste vide.", stderr_output)

        # Check that it still attempted to save (an empty list in this case, due to added check)
        # The success message is printed to stdout, so check mock_print
        mock_print.assert_any_call(f"💾 Pages sauvegardées dans '{self.temp_file_path}'")

        # Verify file content (should be an empty JSON list because the code now handles non-list input)
        with open(self.temp_file_path, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
        self.assertListEqual(loaded_data, [])


    # --- Test for get_pages_summary ---
    # We've set up stdout/stderr capture in setUp, so we don't need mock.patch here,
    # just access self._captured_stdout and self._captured_stderr
    def test_get_pages_summary_valid_list(self):
        pages = [
            FacebookDataTransformer.transform_page_data(self.valid_raw_page),
            FacebookDataTransformer.transform_page_data(self.minimal_raw_page)
        ]
        # Add a page with a long biography
        long_bio_page_raw = self.valid_raw_page.copy()
        long_bio_page_raw['pageId'] = "77777"
        long_bio_page_raw['title'] = "Long Bio Page"
        long_bio_page_raw['intro'] = "This is a very very very very very very very very very very very very very very very very long biography that should be truncated. It continues for more than 100 characters."
        pages.append(FacebookDataTransformer.transform_page_data(long_bio_page_raw))

        # Add a page with a non-string bio
        non_str_bio_page_raw = self.valid_raw_page.copy()
        non_str_bio_page_raw['pageId'] = "99999"
        non_str_bio_page_raw['title'] = "Non-string Bio Page"
        non_str_bio_page_raw['intro'] = 12345 # Not a string
        pages.append(FacebookDataTransformer.transform_page_data(non_str_bio_page_raw))

        # Add a page with metrics not as a dict
        bad_metrics_page_raw = self.valid_raw_page.copy()
        bad_metrics_page_raw['pageId'] = "88888"
        bad_metrics_page_raw['title'] = "Bad Metrics Page"
        bad_metrics_page_raw['metrics'] = "Should be a dict" # Not a dict
        pages.append(FacebookDataTransformer.transform_page_data(bad_metrics_page_raw))

        # Add a page with non-numeric likes/followers
        bad_metrics_values_page_raw = self.valid_raw_page.copy()
        bad_metrics_values_page_raw['pageId'] = "66666"
        bad_metrics_values_page_raw['title'] = "Bad Metrics Values Page"
        bad_metrics_values_page_raw['metrics'] = {"likes": "a lot", "followers": None} # Non-numeric/None values
        pages.append(FacebookDataTransformer.transform_page_data(bad_metrics_values_page_raw))


        FacebookDataTransformer.get_pages_summary(pages)
        stdout_output = self._captured_stdout.getvalue()
        stderr_output = self._captured_stderr.getvalue()

        # Assertions checking for key elements in the stdout output (the actual summary)
        self.assertIn("📊 RÉSUMÉ DES PAGES FACEBOOK:", stdout_output)
        self.assertIn("=" * 60, stdout_output) # Check the separator

        # Check Valid Page summary
        self.assertIn("📄 Test Page Title", stdout_output)
        self.assertIn("🆔 ID: 12345", stdout_output)
        self.assertIn("🔗 URL: https://www.facebook.com/testpage", stdout_output)
        self.assertIn("👥 Followers: 1,500", stdout_output) # Check formatting
        self.assertIn("👍 Likes: 1,000", stdout_output) # Check formatting
        self.assertIn("🏢 Compte business: Oui", stdout_output)
        self.assertIn("📝 Description: This is a test page introduction.", stdout_output)

        # Check Minimal Page summary
        self.assertIn("📄 Minimal Page", stdout_output)
        self.assertIn("🆔 ID: 67890", stdout_output)
        self.assertIn("🔗 URL: https://www.facebook.com/minimalpage", stdout_output)
        self.assertIn("👥 Followers: 0", stdout_output)
        self.assertIn("👍 Likes: 0", stdout_output)
        self.assertIn("🏢 Compte business: Non", stdout_output)
        # Check that the description line is *not* present for minimal page
        minimal_page_block = stdout_output.split("📄 Minimal Page")[1].split("-" * 60)[0]
        self.assertNotIn("📝 Description:", minimal_page_block)


        # Check Long Bio Page summary
        self.assertIn("📄 Long Bio Page", stdout_output)
        self.assertIn("🆔 ID: 77777", stdout_output)
        self.assertIn("📝 Description: This is a very very very very very very very very very very very very very very very very long biogr...", stdout_output) # Check truncated bio


        # Check Non-string Bio Page summary
        self.assertIn("📄 Non-string Bio Page", stdout_output)
        self.assertIn("🆔 ID: 99999", stdout_output)
        # Check that the description line is *not* present
        non_str_bio_block = stdout_output.split("📄 Non-string Bio Page")[1].split("-" * 60)[0]
        self.assertNotIn("📝 Description:", non_str_bio_block)

        # Check Bad Metrics Page summary (metrics field is string)
        self.assertIn("📄 Bad Metrics Page", stdout_output)
        self.assertIn("🆔 ID: 88888", stdout_output)
        self.assertIn("👥 Followers: 0", stdout_output) # Should default to 0 because metrics is not a dict
        self.assertIn("👍 Likes: 0", stdout_output) # Should default to 0 because metrics is not a dict

        # Check Bad Metrics Values Page summary (values are not numbers)
        self.assertIn("📄 Bad Metrics Values Page", stdout_output)
        self.assertIn("🆔 ID: 66666", stdout_output)
        # These lines should go to stderr now
        # self.assertIn("👥 Followers: N/A (Non-numeric value 'a lot')", stdout_output)
        # self.assertIn("👍 Likes: N/A (Non-numeric value 'None')", stdout_output)


        # Check stderr for warnings for invalid types and values
        self.assertIn("⚠️ Attention: 'biography' field for page 'Non-string Bio Page' is not a string (type: int). Skipping description display.", stderr_output)
        self.assertIn("⚠️ Attention: 'metrics' field for page 'Bad Metrics Page' is not a dictionary (type: str). Skipping metrics display.", stderr_output)
        self.assertIn("👥 Followers: N/A (Non-numeric value 'a lot')", stderr_output) # These specific lines now printed to stderr
        self.assertIn("👍 Likes: N/A (Non-numeric value 'None')", stderr_output) # These specific lines now printed to stderr


    def test_get_pages_summary_empty_list(self):
        FacebookDataTransformer.get_pages_summary([])
        stdout_output = self._captured_stdout.getvalue()
        stderr_output = self._captured_stderr.getvalue()

        self.assertIn("\n📊 Aucun résumé disponible (aucune page scrapée).", stdout_output)
        self.assertNotIn("📊 RÉSUMÉ DES PAGES FACEBOOK:", stdout_output) # Ensure header is not printed
        self.assertEqual(stderr_output, "") # No stderr output expected

    def test_get_pages_summary_invalid_input_type(self):
        FacebookDataTransformer.get_pages_summary(self.invalid_batch_input_type)
        stdout_output = self._captured_stdout.getvalue()
        stderr_output = self._captured_stderr.getvalue()

        # Check the warning printed by the method itself to stderr
        self.assertIn("⚠️ Attention: get_pages_summary reçu des données non-liste: str - This is not a list", stderr_output)

        # Check the summary output indicating invalid input to stdout
        self.assertIn("\n📊 Aucun résumé disponible (input non valide).", stdout_output)
        self.assertNotIn("📊 RÉSUMÉ DES PAGES FACEBOOK:", stdout_output) # Ensure header is not printed


    def test_get_pages_summary_list_with_invalid_items(self):
        # List contains valid pages and items that are not dicts
        pages = [
             FacebookDataTransformer.transform_page_data(self.valid_raw_page), # Valid
             self.invalid_raw_page_type, # Invalid: str
             None, # Invalid: None
             FacebookDataTransformer.transform_page_data(self.minimal_raw_page), # Valid
             self.invalid_item_in_batch # Invalid: int
        ]

        FacebookDataTransformer.get_pages_summary(pages)
        stdout_output = self._captured_stdout.getvalue()
        stderr_output = self._captured_stderr.getvalue()


        # Check the warnings printed by the method for invalid items to stderr
        self.assertIn("⚠️ Attention: skipping non-dict item in summary: str - This is not a dict", stderr_output)
        self.assertIn("⚠️ Attention: skipping non-dict item in summary: NoneType - None", stderr_output)
        self.assertIn("⚠️ Attention: skipping non-dict item in summary: int - 12345", stderr_output)

        # Check that summary header is printed to stdout
        self.assertIn("📊 RÉSUMÉ DES PAGES FACEBOOK:", stdout_output)
        # Check that valid pages are summarized to stdout
        self.assertIn("📄 Test Page Title", stdout_output)
        self.assertIn("📄 Minimal Page", stdout_output)
        # Ensure the invalid items didn't cause a crash and weren't summarized in stdout
        # Check for parts of their repr that shouldn't be in the summary block
        self.assertNotIn("This is not a dict", stdout_output)
        self.assertNotIn("None", stdout_output)
        self.assertNotIn("12345", stdout_output) # Should appear in stderr warning, not stdout summary

        # Ensure no other unexpected stderr
        # (Already checked the skipping warnings above)
        pass # Add more specific checks if needed


# This allows running the tests directly from the script
if __name__ == '__main__':
   
    try:
        unittest.main(argv=['first-arg-is-ignored'], exit=False, verbosity=2)
    except NameError as e:
         if "name 'sys' is not defined" in str(e):
             print("\nCRITICAL ERROR: 'sys' is not defined. Make sure 'import sys' is at the very top of the script.", file=sys.stderr)
         else:
             raise e
    except ImportError as e:
         print(f"\nCRITICAL ERROR: Failed to import module for datetime mock target '{MOCK_DATETIME_TARGET}'. Please verify the path is correct for your file structure.", file=sys.stderr)
         print(f"Original error: {e}", file=sys.stderr)
    except Exception as e:
         print(f"\nAn unexpected error occurred during test execution: {e}", file=sys.stderr)
         raise e
