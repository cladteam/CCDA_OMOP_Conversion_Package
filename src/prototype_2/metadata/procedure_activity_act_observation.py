from numpy import int32
from numpy import float32
import prototype_2.value_transformations as VT
# Rulebook solution for creating a measurement from a procedure.Technically correct but will not have a value as number.

metadata = {
	'Procedure_activity_act_observation': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Observation',
            # Results section
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.7' or @root='2.16.840.1.113883.10.20.22.2.7.1']"
    		   "/../hl7:entry/hl7:act[@moodCode='EVN']/"
               "hl7:statusCode[@code='active' or @code='completed']/..")
        },
        'source_section': {
            'config_type': 'CONSTANT',
            'constant_value': 'RESULTS',
            'order': 9999
    	},

    	'observation_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root',
            'order': 1001
    	},
    	'observation_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension',
            'order': 1002
    	},
    	'observation_id_hash': {
    	    'config_type': 'HASH',
            'fields' : [ 'observation_id_root', 'observation_id_extension' ],
            'priority': ('observation_id', 1)
    	},
    	'observation_id_constant': {
            'config_type': 'CONSTANT',
            'constant_value' : 999,
            'priority': ('observation_id', 2)
        },
    	'observation_id_field_hash': {
    	    'config_type': 'HASH',
            'fields' : ['person_id', 'visit_occurrence_id', 'observation_concept_id', 'observation_time',
                    'value_as_number', 'value_as_concept_id'],
            'priority': ('observation_id', 100)
    	},
        'observation_id': {
            'config_type': 'PRIORITY',
            'order': 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

        # <code code="8029-1" codeSystem="1232.23.3.34.3..34"> 
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

    	'observation_concept_domain_id': {
    	    'config_type': 'DOMAIN',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'observation_concept_code',
    		    'vocabulary_oid': 'observation_concept_codeSystem',
                'default': 'n/a'
    	    }
    	},
    	# FIX same issue as above. Is it always just a single value, or do we ever get high and low?
        'observation_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'order': 4
    	},
        'observation_datetime': {
    	    'config_type': None,
            'order': 5
    	},
        'observation_time': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
            'order': 6 
        },
        'observation_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32827),
            'order': 7
        },
        'operator_concept_id': {
    	    'config_type': 'CONSTANT',
    	    'constant_value': "0",
            'order': 8 
        },

    	'value_type': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:value",
    	    'attribute': "{http://www.w3.org/2001/XMLSchema-instance}type",
    	},
   
    	'value_as_number': {
    	    'config_type': None,
            'order': 9
    	},
    	'value_as_concept_id': {
    	    'config_type': None,
            'order':  10
    	},

    	'unit_concept_id': { 'config_type': None, 'order':  11 },
    	'range_low': { 'config_type': None, 'order':  12 },
    	'range_high': { 'config_type': None, 'order':  13 },
    	'provider_id': { 'config_type': None, 'order':  14 },

    	'visit_occurrence_id':	{
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order':  15
    	},
    	'visit_detail_id':	{ 'config_type': None, 'order':  16 },

    	'observation_source_value':	{
    	    'config_type': 'FIELD',
    	    'element': "hl7:code" ,
    	    'attribute': "code",
            'order':  17
        },

    	'observation_source_concept_id':	{ 'config_type': None, 'order':  18 },

    	'unit_source_value':	{ 
    	    'config_type': 'CONSTANT',
            'constant_value': '',
            'order':  19 
        },

    	'value_source_value_constant': {
    	    'config_type': 'CONSTANT',
            'constant_value': 'n/a',
            'priority': ['value_source_value', 4],
        },
    	#'value_source_value_text': {
    	#    'config_type': 'FIELD',
    	#    'element': 'hl7:value[@xsi:type="ST"]' ,
    	#    'attribute': "#text",
        #    'priority': ['value_source_value', 3],
        #},
    	'value_source_value_code': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value[@xsi:type="CD"]' ,
    	    'attribute': "code",
            'priority': ['value_source_value', 2],
        },
    	'value_source_value_value': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value[@xsi:type="PQ"]' ,
    	    'attribute': "value",
            'priority': ['value_source_value', 1],
        },
        'value_source_value' : {
            'config_type': 'PRIORITY',
            'order':20
        },

	'filename' : {
		'config_type': 'FILENAME',
		'order':100
	} 
}
}
