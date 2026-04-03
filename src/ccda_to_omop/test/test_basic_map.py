import unittest
import ccda_to_omop.value_transformations as VT
import numpy as np
from ccda_to_omop import package_constant_access


class Test_basic_concept_mapping(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        package_constant_access.set_allow_no_matching_concept(True)
        super().__init__(*args, **kwargs)

        mock_map = {
            ('2.16.840.1.113883.6.1', '11579-0'): [{'target_concept_id': np.int32(3019762), 'target_domain_id': 'Measurement', 'source_concept_id': np.int32(3019762)}],
            ('2.16.840.1.113883.6.1', '0000-0'): [{'target_concept_id': np.int32(0), 'target_domain_id': 'Observation', 'source_concept_id': np.int32(0)}] 
        }
        VT.set_codemap_dict(mock_map)


    def test_concept_id_good(self):
        package_constant_access.set_allow_no_matching_concept(True)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '11579-0'}
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, np.int32(3019762))

    def test_concept_id_good_disallowed(self):
        package_constant_access.set_allow_no_matching_concept(False)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '11579-0'}
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, np.int32(3019762))


    def test_concept_id_nmc(self):
        package_constant_access.set_allow_no_matching_concept(True)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '0000-0'}
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, 0)


    def test_concept_id_nmc_disallowed(self):
        package_constant_access.set_allow_no_matching_concept(False)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '0000-0'}
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, None)


    def test_concept_id_bogus(self):
        package_constant_access.set_allow_no_matching_concept(True)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': 'bogus'}
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, None)

    def test_concept_id_bogus_disallowed(self):
        package_constant_access.set_allow_no_matching_concept(False)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': 'bogus'}
        concept_id = VT.codemap_xwalk_concept_id(args_dict)
        self.assertEqual(concept_id, None)

#################################################################        

    def test_source_concept_good(self):
        package_constant_access.set_allow_no_matching_concept(True)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '11579-0'}
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, np.int32(3019762))

    def test_source_concept_good_disallowed(self):
        package_constant_access.set_allow_no_matching_concept(False)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '11579-0'}
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, np.int32(3019762))


    def test_source_concept_nmc(self):
        package_constant_access.set_allow_no_matching_concept(True)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '0000-0'}
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, 0)


    def test_source_concept_nmc_disallowed(self):
        package_constant_access.set_allow_no_matching_concept(False)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': '0000-0'}
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, None)


    def test_source_concept_bogus(self):
        package_constant_access.set_allow_no_matching_concept(True)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': 'bogus'}
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, None)

    def test_source_concept_bogus_disallowed(self):
        package_constant_access.set_allow_no_matching_concept(False)
        args_dict = { 'vocabulary_oid': '2.16.840.1.113883.6.1',
                      'concept_code': 'bogus'}
        concept_id = VT.codemap_xwalk_source_concept_id(args_dict)
        self.assertEqual(concept_id, None)

#################################################################        








