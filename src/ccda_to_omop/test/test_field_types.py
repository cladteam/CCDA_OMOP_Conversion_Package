import unittest
import io
import numpy as np
from lxml import etree as ET
from collections import defaultdict
import ccda_to_omop.value_transformations as VT
from ccda_to_omop.data_driven_parse import parse_config_from_xml_file

mock_map = { 
    ('2.16.840.1.113883.6.1', '742-7'): [{'target_concept_id': np.int32(3033575), 'target_domain_id': 'Measurement', 'source_concept_id': np.int32(3033575)}] 
}
VT.set_codemap_dict(mock_map)

class FieldTypeTest_DERIVED(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xml_text = """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
            <recordTarget>
                <patientRole>
                    <id extension="444222222" root="2.16.840.1.113883.4.1"/>
                    <code code="742-7" codeSystem="2.16.840.1.113883.6.1"/>
                    <addr use="HP">
                        <streetAddressLine>2222 Home Street</streetAddressLine>
                        <city>Beaverton</city>
                        <state>MD</state>
                        <postalCode>21014</postalCode>
                        <country>US</country>
                    </addr>
                </patientRole>
            </recordTarget>
        </ClinicalDocument>
        """
        self.config_dict = {

            'Test': {
                'root': {
                    'config_type': 'ROOT',
                    'expected_domain_id' : 'Measurement',
                    'element': "./hl7:recordTarget/hl7:patientRole"
                },
                'concept_codeSystem': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "codeSystem",
                    'order': 3
                },
                'concept_code': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "code",
                    'order': 4
                },
                'measurement_id': {
                    'config_type': 'CONSTANT',
                    'constant_value': 1,
                    'order': 1
                },
               	'measurement_concept_id': {
    	            'config_type': 'DERIVED',
    	            'FUNCTION': VT.codemap_xwalk_concept_id,
    	            'argument_names': {
    		            'concept_code': 'concept_code',
    		            'vocabulary_oid': 'concept_codeSystem'
    	            },
                    'order': 8
    	        },
              	'domain_id': {
    	            'config_type': 'DERIVED',
    	            'FUNCTION': VT.codemap_xwalk_domain_id,
    	            'argument_names': {
    		            'concept_code': 'concept_code',
    		            'vocabulary_oid': 'concept_codeSystem'
    	            },
                    'order': 9
    	        }
            }
        }

    def test_derived(self):
            # a deep test that not only tests the DERVIED mechanisms, but the availability
            # of the map file.
            # 2.16.840.1.113883.6.1,742-7,3033575,Measurement
        #    def _codemap_xwalk(vocabulary_oid, concept_code, column_name, default)
        self.assertEqual(VT._codemap_xwalk('2.16.840.1.113883.6.1', '742-7', 'target_concept_id', 0), 3033575)
        self.assertEqual(VT.codemap_xwalk_concept_id({'vocabulary_oid': '2.16.840.1.113883.6.1', 'concept_code': '742-7', 'default':0}), 3033575)

        with io.StringIO(self.xml_text) as file_obj:
            tree = ET.parse(file_obj)
            pk_dict = defaultdict(list)
            for domain, domain_meta_dict in self.config_dict.items():
                # print(f"INPUT {domain} {domain_meta_dict}")
                data_dict_list= parse_config_from_xml_file(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                print(f"OUTPUT {data_dict}")
                self.assertEqual(data_dict['concept_codeSystem'], "2.16.840.1.113883.6.1")
                self.assertEqual(data_dict['concept_code'], "742-7")
                self.assertEqual(data_dict['domain_id'], "Measurement")
                self.assertEqual(data_dict['measurement_concept_id'], 3033575)
            
            
            
class FieldTypeTest_FIELD(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xml_text = """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
            <recordTarget>
                <patientRole>
                    <id extension="444222222" root="2.16.840.1.113883.4.1"/>
                    <addr use="HP">
                        <streetAddressLine>2222 Home Street</streetAddressLine>
                        <city>Beaverton</city>
                        <state>MD</state>
                        <postalCode>21014</postalCode>
                        <country>US</country>
                    </addr>
                </patientRole>
            </recordTarget>
        </ClinicalDocument>
        """
        self.config_dict = {
            'Test': {
                'root': {
                    'config_type': 'ROOT',
                    'expected_domain_id' : 'Test',
                    'element': "./hl7:recordTarget/hl7:patientRole"
                },
                'attribute_value': {
                    'config_type': 'FIELD',
                    'element': 'hl7:id',
                    'attribute': "root",
                    'order': 1
                },
                'text_value': {
                    'config_type': 'FIELD',
                    'element': 'hl7:addr/hl7:streetAddressLine',
                    'attribute': "#text",
                    'order': 2
                },
                'domain_id': {
                    'config_type': 'CONSTANT',
                    'constant_value': 'Test'
                }
            }
        }

    def test_field(self):
        with io.StringIO(self.xml_text) as file_obj:
            tree = ET.parse(file_obj)
            pk_dict = defaultdict(list)
            for domain, domain_meta_dict in self.config_dict.items():
                print(f"INPUT domain:{domain} ")
                #print(f"INPUT meta:{domain_meta_dict}")
                data_dict_list= parse_config_from_xml_file(tree, domain, domain_meta_dict, "test_file", pk_dict)
                print(f"OUTPUT list {data_dict_list}")
                data_dict = data_dict_list[0]
                if len(data_dict_list) > 0:
                    print(f"OUTPUT first {data_dict}")
                    self.assertEqual(data_dict['attribute_value'], "2.16.840.1.113883.4.1")
                    self.assertEqual(data_dict['text_value'], "2222 Home Street")
                print("done")

                
class FieldTypeTest_HASH(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xml_text = """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
            <recordTarget>
                <patientRole>
                    <id extension="444222222" root="2.16.840.1.113883.4.1"/>
                    <code code="742-7" codeSystem="2.16.840.1.113883.6.1"/>
                    <addr use="HP">
                        <streetAddressLine>2222 Home Street</streetAddressLine>
                        <city>Beaverton</city>
                        <state>MD</state>
                        <postalCode>21014</postalCode>
                        <country>US</country>
                    </addr>
                </patientRole>
            </recordTarget>
        </ClinicalDocument>
        """
        self.config_dict = {
            'Test': {
                'root': {
                    'config_type': 'ROOT',
                    'expected_domain_id' : 'Test',
                    'element': "./hl7:recordTarget/hl7:patientRole"
                },
                'concept_codeSystem': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "codeSystem"
                },
                'concept_code': {
                    'config_type': 'FIELD',
                    'element': 'hl7:code',
                    'attribute': "code"
                },
               	'domain_id': {
                    'config_type': 'CONSTANT',
                    'constant_value': 'Test'
                },
                'test_hash_0': { 
                    'config_type': 'HASH',
                    'fields' : [ 'concept_codeSystem', 'concept_code' ], 
                    'order' : 0
                },
               	'test_hash_1': { 
                    'config_type': 'HASH',
                    'fields' : [ 'concept_code', 'concept_codeSystem' ], 
                    'order' : 1
                },
                'test_hash_2': { 
                    'config_type': 'HASH',
                    'fields' : [ 'concept_code', None ], 
                    'order' : 2
                },
                'test_hash_3': { 
                    'config_type': 'HASH',
                    'fields' : [ None, 'concept_codeSystem' ], 
                    'order' : 3
                },
                'test_hash_4': { 
                    'config_type': 'HASH',
                    'fields' : [ None ], 
                    'order' : 4
                }
            }
        }

    def test_hash(self):
        with io.StringIO(self.xml_text) as file_obj:
            tree = ET.parse(file_obj)
            pk_dict = defaultdict(list)
            for domain, domain_meta_dict in self.config_dict.items():
                data_dict_list= parse_config_from_xml_file(tree, domain, domain_meta_dict, "test_file", pk_dict)
                data_dict = data_dict_list[0]
                print(f"OUTPUT {data_dict}")
                #self.assertEqual(data_dict['test_hash_0'][0], 2730455650958355)
                #self.assertEqual(data_dict['test_hash_1'][0], 1347390606787380)
                #self.assertEqual(data_dict['test_hash_2'][0], 1136581816084342)
                #self.assertEqual(data_dict['test_hash_3'][0], 2914246837974734)
                #self.assertEqual(data_dict['test_hash_4'][0], None)
                self.assertEqual(data_dict['test_hash_0'], 2730455650958355)
                self.assertEqual(data_dict['test_hash_1'], 1347390606787380)
                self.assertEqual(data_dict['test_hash_2'], 1136581816084342)
                self.assertEqual(data_dict['test_hash_3'], 2914246837974734)
                self.assertEqual(data_dict['test_hash_4'], None)

            
            

