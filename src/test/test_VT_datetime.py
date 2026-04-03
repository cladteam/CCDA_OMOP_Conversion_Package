import unittest
import ccda_to_omop.value_transformations as VT

class TestDateTime(unittest.TestCase):

    # --- ISO 8601 Format Tests ---

    def test_transform_datetime_low_iso8601(self):
        """Test that transform_datetime_low creates a datetime from an ISO 8601 date with time 00:00:00"""
        args = {
            'input_value': '2025-04-02', 
            'default': '1900-01-01T00:00:00.000Z'
        }
        result = VT.transform_datetime_low(args)
        self.assertEqual(str(result), '2025-04-02 00:00:00')

    def test_transform_datetime_high_iso8601(self):
        """Test that transform_datetime_high creates a datetime from an ISO 8601 date with time 23:59:59"""
        args = {
            'input_value': '2025-04-02', 
            'default': '1900-01-01T00:00:00.000Z'
        }
        result = VT.transform_datetime_high(args)
        self.assertEqual(str(result), '2025-04-02 23:59:59')

    # --- HL7 Format Tests ---

    def test_transform_datetime_low_hl7(self):
        """Test that transform_datetime_low creates an ISO datetime from an HL7 (YYYYMMDD) date with time 00:00:00"""
        args = {
            'input_value': '20250402', 
            'default': '1900-01-01T00:00:00.000Z'
        }
        result = VT.transform_datetime_low(args)
        self.assertEqual(str(result), '2025-04-02 00:00:00')

    def test_transform_datetime_high_hl7(self):
        """Test that transform_datetime_high creates an ISO datetime from an HL7 (YYYYMMDD) date with time 23:59:59"""
        args = {
            'input_value': '20250402', 
            'default': '1900-01-01T00:00:00.000Z'
        }
        result = VT.transform_datetime_high(args)
        self.assertEqual(str(result), '2025-04-02 23:59:59')

    # --- Preservation Tests ---

    def test_preserves_existing_time_low(self):
        """Test that transform_datetime_low does NOT overwrite if time is already present"""
        args = {
            'input_value': '2025-04-02T21:43:56.000Z', 
            'default': '1900-01-01T00:00:00.000Z'
        }
        result = VT.transform_datetime_low(args)
        self.assertEqual(str(result), '2025-04-02 21:43:56')

    def test_preserves_existing_time_high(self):
        """Test that transform_datetime_high does NOT overwrite if time is already present"""
        args = {
            'input_value': '2025-04-02T21:43:56.000Z', 
            'default': '1900-01-01T00:00:00.000Z'
        }
        result = VT.transform_datetime_high(args)
        self.assertEqual(str(result), '2025-04-02 21:43:56')

if __name__ == '__main__':
    unittest.main()