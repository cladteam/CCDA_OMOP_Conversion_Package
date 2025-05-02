
import prototype_2.value_transformations as VT

metadata = {
    'Observation_social_history_cultural': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Observation',
            # Results section
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.17']/../"
    		   "hl7:entry/hl7:observation/hl7:templateId[@root='2.16.840.1.113883.10.20.22.4.111']/..")
    		    # FIX: another template at the observation level here: "2.16.840.1.113883.10.20.22.4.2  Result Observation is an entry, not a section
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
  	    'observation_id': {
    	    'config_type': 'HASH',
            'fields' : ['person_id',
                        #'visit_occurrence_id',
                        'observation_concept_code', 'observation_concept_codeSystem',
			            'observation_time',
                        'value_as_string', 'value_as_nmber', 'value_as_concept_id',
                        'observation_id_extension', 'observation_id_root'],
            'order': 1
    	},

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},

        # <code code="72166-2"" codeSystem="2.16.840.1.113883.5.4"> 
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

# TODO        
        'observation_date': {
    	    'config_type': 'FIELD',
            'data_type':'DATE',
    	    'element': "hl7:effectiveTime/low",
    	    'attribute': "value",
            'order': 4
    	},
        'observation_datetime': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:effectiveTime/low",
    	    'attribute': "value",
            'order': 5
    	},
        
        'observation_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : 32035,
            'order': 7
        },
        'operator_concept_id': { 'config_type': None, 'order': 8 },

    	'value_as_number': { 'config_type': None,'order': 9	},
        
       	'value_as_string': { 'config_type': None,'order': 10},


    	'value_as_code': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value' ,
    	    'attribute': "code",
        },
    	'value_as_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': 'hl7:value' ,
    	    'attribute': "codeSystem",
        },
    	'value_as_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'value_as_code',
    		    'vocabulary_oid': 'value_as_codeSystem',
                'default': None
            },
            'order':  11
    	},

       	'qualifier_concept_id': { 'config_type': None,'order': 12	},

    	'unit_concept_id': { 'config_type': None, 'order':  13 },

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

    	'unit_source_value':	{ 'config_type': None, 'order':  19 },
       	'qualifier_source_value':	{ 'config_type': None, 'order':  20 },

       	'value_source_value': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,
    	    'argument_names': {
       		    'first_field': 'value_as_codeSystem',
    		    'second_field': 'value_as_code',
                'default': '0'
            },
            'order':  21
    	},
        'filename' : {
            'config_type': 'FILENAME',
            'order':100
		}
         
        
    }
}
