import unittest
import logging
import os
from prototype_2.metadata import get_meta_dict

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- Define METADATA_DIR for the test ---
TEST_DIR = os.path.dirname(__file__)
PROTOTYPE_2_DIR = os.path.dirname(TEST_DIR)
METADATA_DIR = os.path.join(PROTOTYPE_2_DIR, 'metadata')


class MetadataLoadingTest(unittest.TestCase):

    # --- THIS TEST METHOD IS MODIFIED ---
    def test_get_meta_dict_loads_successfully_and_prints_keys(self): 
        """
        Tests if get_meta_dict() runs without error, returns a non-empty dict,
        contains expected keys, and PRINTS all top-level keys found.
        """
        logger.info("\n--- Test 1: Testing get_meta_dict and listing loaded keys ---")
        logger.info("Attempting to run get_meta_dict()...")
        result_dict = None

        try:
            result_dict = get_meta_dict()
            logger.info(f"Function completed successfully. Found {len(result_dict)} top-level keys.")

            self.assertIsInstance(result_dict, dict, "Function should return a dictionary.")
            self.assertTrue(len(result_dict) > 0, "Metadata dictionary should not be empty.")

            # Check for known, expected keys
            expected_keys = ['Measurement_results', 'Observation', 'Procedure_activity_procedure', 'Visit', 'Person']
            missing_keys = [key for key in expected_keys if key not in result_dict]
            self.assertEqual(len(missing_keys), 0, f"Expected essential keys missing: {missing_keys}")

            print(f"\n[TEST INFO] Top-level keys found in loaded metadata dictionary ({len(result_dict)} total):")
            for key in sorted(result_dict.keys()):
                print(f"  - {key}")
            print("[TEST INFO] End of key list.\n")

            logger.info("--- Test 1 Passed Successfully (get_meta_dict) ---")

        except Exception as e:
            logger.error(f"ERROR during get_meta_dict: {e}", exc_info=True)
            self.fail(f"get_meta_dict raised an unexpected exception: {e}")

    def test_check_for_procedure_results_file_and_list_contents(self):
        """
        Tests if 'procedure_results.py' is present and PRINTS the list
        of files found in the metadata directory for verification.
        """
        logger.info("\n--- Test 2: Listing metadata dir contents & checking for 'procedure_results.py' ---")
       
        try:
            filenames = os.listdir(METADATA_DIR)
            print(f"\n[TEST INFO] Files found in metadata directory ({len(filenames)} total):")
            for fname in sorted(filenames):
                print(f"  - {fname}")
            print("[TEST INFO] End of file list.\n")
            self.assertNotIn('procedure_results.py', filenames, "TEST FAILED: 'procedure_results.py' was found unexpectedly.")
            logger.info("--- Test 2 Passed Successfully ('procedure_results.py' was NOT found) ---")
        except FileNotFoundError:
            logger.error(f"Test setup failed: Metadata directory not found at {METADATA_DIR}")
            self.fail(f"Test setup failed: Metadata directory not found at {METADATA_DIR}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while listing directory: {e}", exc_info=True)
            self.fail(f"An unexpected error occurred while listing directory: {e}")

# Standard block
if __name__ == '__main__':
    unittest.main()