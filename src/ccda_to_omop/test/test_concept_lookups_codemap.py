
import unittest
import ccda_to_omop.value_transformations as VT
##from foundry.transforms import Dataset
import ccda_to_omop.util as U
import numpy as np

mock_map = { 
    ('2.16.840.1.113883.6.1', '742-7'): [{'target_concept_id': np.int32(3033575), 'target_domain_id': 'Measurement', 'source_concept_id': np.int32(3033575)} ] ,
    ('2.16.840.1.113883.6.1', '788-0'): [{'target_concept_id': np.int32(3019897), 'target_domain_id': 'Measurement', 'source_concept_id': np.int32(3019897)} ], 
    ('2.16.840.1.113883.6.96', '266919005'): [{'target_concept_id': np.int32(903653), 'target_domain_id': 'Observation', 'source_concept_id': np.int32(4144272)} ], 
    ('2.16.840.1.113883.6.12', '99213'): [{'target_concept_id': np.int32(9202), 'target_domain_id': 'Visit', 'source_concept_id': np.int32(2414397)}] 
}
VT.set_codemap_dict(mock_map)


# test_data includes keys used in newer codemap and valueset calls so
# this dictionary can be passed in as args.
test_data = {
    'different domain': {
        # args
        'vocabulary_oid': "2.16.840.1.113883.6.12",
        'concept_code': "99213",
        'default':0,
        # expected outputs
        'source_concept_id': 2414397,
        'source_domain_id': 'Observation',
        'target_concept_id': 9202,   
        'target_domain_id': 'Visit'
    },
    
    'different concept': {
        # args
        'vocabulary_oid': "2.16.840.1.113883.6.96", 
        'concept_code': "266919005", 
        'default':0,
        # expected outputs
        'source_concept_id': 4144272,
        'source_domain_id': 'Observation',
        'target_concept_id': 903653, 
        'target_domain_id': 'Observation'
    },

    'same same': {
        # args
        'vocabulary_oid': "2.16.840.1.113883.6.1", 
        'concept_code': "788-0",
        'default':0,
        # expected outputs
        'source_concept_id': 3019897,
        'source_domain_id': 'Measurement',
        'target_concept_id': 3019897,
        'target_domain_id': 'Measurement'
    }
}

class TestConceptLookup_codemap(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #visit_map_df = Dataset.get("visit_concept_xwalk_mapping_dataset").read_table(format="pandas")
        #visitmap_dict = U.create_visit_dict(visit_map_df)
        #VT.set_visitmap_dict(visitmap_dict)

        #valueset_map_df = Dataset.get("ccda_value_set_mapping_table_dataset").read_table(format="pandas")
        #valueset_dict = U.create_valueset_dict(valueset_map_df)
        #VT.set_valueset_dict(valueset_dict)
        
        #codemap_df = Dataset.get("codemap_xwalk").read_table(format="pandas")
        #codemap_dict = U.create_codemap_dict(codemap_df)
        #VT.set_codemap_xwalk_dict(codemap_dict)

        
    def test_concept_id_lookup(self):
        for test_case_key, test_case_dict in test_data.items():
            target_concept_id = VT.codemap_xwalk_concept_id(test_case_dict)
            self.assertEqual(target_concept_id, test_case_dict['target_concept_id'])
           
    def test_domain_id_lookup(self):                           
        for test_case_key, test_case_dict in test_data.items():                         
            target_domain_id = VT.codemap_xwalk_domain_id(test_case_dict)
            self.assertEqual(target_domain_id, test_case_dict['target_domain_id'])
                              
    def test_source_concept_id_lookup(self):
        for test_case_key, test_case_dict in test_data.items():                     
            source_concept_id = VT.codemap_xwalk_source_concept_id(test_case_dict)
            self.assertEqual(source_concept_id, test_case_dict['source_concept_id'])
   