import unittest
import numpy as np
import ccda_to_omop.value_transformations as VT


class ValueTransformTest_map_to_standard_1(unittest.TestCase):
    """ this tests an older csv file
    """
    #2.16.840.1.113883.6.1,11579-0,3019762,Measurement
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocab_oid = '2.16.840.1.113883.5.1'
        self.concept_code = 'F'
        self.expected_concept_id = 8532
        self.expected_source_concept_id = 0
        self.expected_domain_id = 'Gender'
        mock_map = {
            ('2.16.840.1.113883.5.1', 'F'): [{'target_concept_id': np.int32(8532), 'target_domain_id': 'Gender'}],
            ('2.16.840.1.113883.6.1', '11579-0'): [{'target_concept_id': np.int32(3019762), 'target_domain_id': 'Measurement'}]
        }
        VT.set_valueset_dict(mock_map)
        VT.set_codemap_dict(mock_map)

    def test_map_hl7_to_omop_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, self.expected_concept_id)
                         

    def test_map_to_domain_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        domain_id = VT.codemap_xwalk_domain_id(args_dict)
        print(args_dict)
        print(domain_id)
        self.assertEqual(domain_id, self.expected_domain_id)


    def test_map_default_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid, 'concept_code': 'bogus', 'default': 0 }
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, None)  # NMC is  off by default


class ValueTransformTest_map_to_standard_2(unittest.TestCase):
    """ this tests an older csv file
    """
    #2.16.840.1.113883.6.1,11579-0,3019762,Measurement
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocab_oid = '2.16.840.1.113883.6.1'
        self.concept_code = '11579-0'
        self.expected_concept_id = 3019762
        self.expected_source_concept_id = 3019762
        self.expected_domain_id = 'Measurement'
        mock_map = {
            ('2.16.840.1.113883.5.1', 'F'): [{'target_concept_id': np.int32(8532), 'target_domain_id': 'Gender'}],
            ('2.16.840.1.113883.6.1', '11579-0'): [{'target_concept_id': np.int32(3019762), 'target_domain_id': 'Measurement'}]
        }
        VT.set_valueset_dict(mock_map)
        VT.set_codemap_dict(mock_map)

    def test_map_to_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, self.expected_concept_id)
                         

    def test_map_to_domain_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid,
                      'concept_code': self.concept_code,
                      'default': 0 }
        print(args_dict)
        domain_id = VT.codemap_xwalk_domain_id(args_dict)
        print(domain_id)
        self.assertEqual(domain_id, self.expected_domain_id)


    def test_map_source_concept_id(self):
        args_dict = { 'vocabulary_oid': self.vocab_oid, 'concept_code': 'bogus', 'default': 0 }
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, None)  # NMC is off by default
