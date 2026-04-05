
from numpy import int32
import ccda_to_omop.value_transformations as VT

metadata = {
    'OBSERVATION-from-Encounter': {

        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Observation',
            # Encounters sections
            'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section'
                        '/hl7:templateId[ @root="2.16.840.1.113883.10.20.22.2.22" or @root="2.16.840.1.113883.10.20.22.2.22.1" ]/..'
                        '/hl7:entry/hl7:encounter')  
        },
 
    	'observation_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root'
    	},
    	'observation_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension'
    	},
    	'observation_id': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'provider_id',
                        #'visit_occurrence_id',
                        'observation_concept_code', 'observation_concept_codeSystem',
                        'observation_date', 'observation_datetime',
                        'value_as_string', 'value_as_number', 'value_as_concept_id',
                        'observation_id_extension', 'observation_id_root','unit_source_value'],
            'order': 1
    	},


    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

    	'observation_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code" ,
    	    'attribute': "code"
    	},
    	'observation_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'observation_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem',
                'default': 0
    	    },
            'order': 3
    	},

    	'domain_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem',
                'default': 0
    	    }
    	},

       	'observation_date_effectiveTime': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
			'priority': ['observation_date', 1]
    	},
    	'observation_date_low': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
			'priority': ['observation_date', 2]
    	},
       	'observation_date_high': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/hl7:high",
    	    'attribute': "value",
			'priority': ['observation_date', 3]
    	},
    	'observation_date': {
    	    'config_type': 'PRIORITY',
            'order': 4
    	},
		
		'observation_datetime_effectiveTime': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATETIME',
			'priority': ['observation_datetime', 1]
        },  
        'observation_datetime_low': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime/hl7:low", 
            'attribute': "value",
            'data_type': 'DATETIME',
			'priority': ['observation_datetime', 2]
        },  
     	'observation_datetime_high': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime/hl7:high",
    	    'attribute': "value",
			'priority': ['observation_datetime', 3]
    	},
        'observation_datetime': {
    	    'config_type': 'PRIORITY',
            'order': 5
        },               
    	'observation_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32827),
            'order': 6
        },
    	'value_as_number': {
    	    'config_type': 'FIELD',
            'data_type':'FLOAT',
    	    'element': 'hl7:value[@xsi:type="PQ"]' ,
    	    'attribute': "value",
            'order': 7
    	},
		'value_as_number_string': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value' ,
    	    'attribute': "value"
    	},
    	'value_as_string': {
    	    'config_type': 'FIELD',
            'element': 'hl7:value',  #'element': 'hl7:value[@xsi:type="ST"]', 
    	    'attribute': "#text",
            'order': 8
    	},
		'value_code': {
    	    'config_type': 'FIELD',
            'element': 'hl7:value', 
    	    'attribute': "code",
        },
        'value_codeSystem': {
    	    'config_type': 'FIELD',
            'element': 'hl7:value',
    	    'attribute': "codeSystem",
        },
		'value_as_concept_id': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'value_code',
    		    'vocabulary_oid': 'value_codeSystem'
    	    },
            'order':  9
    	},
        'qualifier_concept_id' : { 'config_type': None, 'order': 10 },

		'unit_source_value':  {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value',
    	    'attribute': 'unit',
            'order':  17
    	},
        'unit_codeSystem':  {
    	    'config_type': 'CONSTANT',
			'constant_value' : '2.16.840.1.113883.6.8',
    	},
        'unit_concept_id': { 
			'config_type': 'DERIVED', 
    	    'FUNCTION': VT.valueset_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'unit_source_value',
    		    'vocabulary_oid': 'unit_codeSystem',
                'default': None
            },
			'order': 11 
		},
	   
        'provider_id': { 'config_type': None, 'order': 12 },
        
    	'visit_occurrence_id':	{
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order': 13
    	},    
        
        'visit_detail_id': { 'config_type': None, 'order': 14},

   		'observation_source_value': {
    	    'config_type': 'DERIVED2',
    	    'FUNCTION': VT.concat_field_list_values,
    	    'argument_list': {
       		    'key_list': [
                    'observation_concept_code',
                    'observation_concept_codeSystem',
                    'value_codeSystem',
                    'value_code', 
                    'value_as_string',  # yes, redundant
                    'value_as_number_string'
				]
            },
            'order': 15
        },

        'observation_source_concept_id': { 
	        'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'observation_concept_code',
                'vocabulary_oid': 'observation_concept_codeSystem',
                'default': None
            },
			'order': 16 
		},
    	# (above) 'unit_source_value':	{'config_type': None,  'order':  17 }
        'qualifier_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 18 
        },
        'data_partner_id': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.get_data_partner_id, 
            'argument_names': { 'filename': 'filename' },
            'order': 20
        },
		
        'filename' : {
		    'config_type': 'FILENAME',
		    'order':100
        },
        'cfg_name' : { 
			'config_type': 'CONSTANT', 
            'constant_value': 'OBSERVATION-from-Encounter',
			'order':101
		} 
    }
}