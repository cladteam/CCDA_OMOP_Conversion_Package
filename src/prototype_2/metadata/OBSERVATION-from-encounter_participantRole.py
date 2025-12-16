
from numpy import int32
import prototype_2.value_transformations as VT

#541 

metadata = {
    'OBSERVATION-from-Encounter_participantRole': {

        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Observation',
            # Encounters sections
            'element': ('./hl7:component/hl7:structuredBody/hl7:component/hl7:section'
                        '/hl7:templateId[ @root="2.16.840.1.113883.10.20.22.2.22" or @root="2.16.840.1.113883.10.20.22.2.22.1" ]/..'
                        '/hl7:entry/hl7:encounter/hl7:participant/hl7:participantRole')
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
                        'observation_id_extension', 'observation_id_root'],
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

    	'observation_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 4
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
    	# 'observation_datetime': { 'config_type': None, 'order': 5 },
        'observation_datetime': {
            'config_type': 'FIELD',
            'element': "hl7:effectiveTime", 
            'attribute': "value",
            'data_type': 'DATETIME',
            'order': 5
        },               
        
    	'observation_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32827),
            'order': 6
        },

    	'value_as_number': { 'config_type': None, 'order': 7 },
    	'value_as_string': { 'config_type': None, 'order': 8 },
    	'value_as_concept_id': { 'config_type': None, 'order':  9 },
        'qualifier_concept_id' : { 'config_type': None, 'order': 10 },
        'unit_concept_id': { 'config_type': None, 'order': 11 },
        'provider_id': { 'config_type': None, 'order': 12 },
        
    	'visit_occurrence_id':	{
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order': 13
    	},    
        
        'visit_detail_id': { 'config_type': None, 'order': 14},

        'observation_source_value': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,
    	    'argument_names': {
    		    'first_field': 'observation_concept_code',
    		    'second_field': 'observation_concept_codeSystem',
                'default': 'n/a'
    	    },
            'order' : 15
    	},

        'observation_source_concept_id': { 
	        'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'observation_concept_code',
                'vocabulary_oid': 'observation_concept_system',
                'default': None
            },
			'order': 16 
		},

        'unit_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 17 
        },
        'qualifier_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 18 
        },
		
        'filename' : {
		    'config_type': 'FILENAME',
		    'order':100
        },
        'cfg_name' : { 
			'config_type': 'CONSTANT', 
            'constant_value': 'OBSERVATION-from-Encounter_participantRole',
			'order': 101
		} 
    }
}