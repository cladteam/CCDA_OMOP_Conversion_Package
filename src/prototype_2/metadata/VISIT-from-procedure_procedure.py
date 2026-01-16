
from numpy import int32
from numpy import float32
import prototype_2.value_transformations as VT

metadata = {
    'VISIT-from-procedure_procedure': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Visit',
            # Results section
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.7' or @root='2.16.840.1.113883.10.20.22.2.7.1']"
    		   '/../hl7:entry/hl7:procedure')
    		    # FIX: another template at the observation level here: "2.16.840.1.113883.10.20.22.4.2  Result Observation is an entry, not a section
        },
        
    	
        'visit_occurrence_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "root"
        },
        'visit_occurrence_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': "extension"
        },
        'visit_occurrence_id': {
            'config_type': 'HASH',
            'fields' : ['visit_occurrence_id_root', 'visit_occurrence_id_extension',
                        'person_id', 'provider_id',
                        'visit_concept_id', 'visit_source_value',
                        'visit_start_date', 'visit_start_datetime',
                        'visit_end_date', 'visit_end_datetime'],
            'order' : 1
        },

        'person_id': {
            'config_type': 'FK',
            'FK': 'person_id',
            'order': 2
        },
        'visit_concept_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code" ,
    	    'attribute': "code"
    	},
    	'visit_concept_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:code",
    	    'attribute': "codeSystem"
    	},
		   'visit_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,
    	    'argument_names': {
    		    'concept_code': 'visit_concept_code',
    		    'vocabulary_oid': 'visit_concept_codeSystem',
                'default': 0
    	    },
            'order': 3
    	},
        	'domain_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'visit_concept_code',
    		    'vocabulary_oid': 'visit_concept_codeSystem',
                'default': 0
    	    }
    	},
			
        'visit_start_date_low': { 
            'config_type': 'FIELD', 
            'data_type': 'DATE', 
            'element': "hl7:effectiveTime/hl7:low[not(@nullFlavor=\"UNK\")]", 
            'attribute': "value", 
            'priority': ['visit_start_date', 1] 
        },
          'visit_start_date_value': { 
            'config_type': 'FIELD', 
            'data_type': 'DATE', 
            'element': "hl7:effectiveTime", 
            'attribute': "value", 
            'priority': ['visit_start_date', 2] 
        },
        'visit_start_date': { 
			'config_type': 'PRIORITY', 
			'order': 4 
		},
         'visit_end_date_high': { 
            'config_type': 'FIELD', 
            'data_type': 'DATE', 
            'element': "hl7:effectiveTime/hl7:high[not(@nullFlavor=\"UNK\")]", 
            'attribute': "value", 
            'priority': ['visit_end_date', 1] 
        },
         'visit_end_date_value': { 
            'config_type': 'FIELD', 
            'data_type': 'DATE', 
            'element': "hl7:effectiveTime", 
            'attribute': "value", 
            'priority': ['visit_end_date', 2] 
        },
		 'visit_end_date':  { 
			'config_type': 'PRIORITY',
			 'order':6 
        }, 
          'visit_start_datetime_low': { 
            'config_type': 'FIELD', 
            'data_type': 'DATETIME', 
            'element': "hl7:effectiveTime/hl7:low[not(@nullFlavor=\"UNK\")]", 
            'attribute': "value", 
            'priority': ['visit_start_datetime', 1] 
        },
        'visit_start_datetime_value': { 
            'config_type': 'FIELD', 
            'data_type': 'DATETIME', 
            'element': "hl7:effectiveTime", 
            'attribute': "value", 
            'priority': ['visit_start_datetime', 2] 
        },
		 'visit_start_datetime' : {
			 'config_type': 'PRIORITY', 
		 'order': 5
		},
      'visit_end_datetime_high': { 
            'config_type': 'FIELD', 
            'data_type': 'DATETIME', 
            'element': "hl7:effectiveTime/hl7:high[not(@nullFlavor=\"UNK\")]", 
            'attribute': "value", 
            'priority': ['visit_end_datetime', 1] 
        },
        'visit_end_datetime_value': { 
            'config_type': 'FIELD', 
            'data_type': 'DATETIME', 
            'element': "hl7:effectiveTime", 
            'attribute': "value", 
            'priority': ['visit_end_datetime', 2] 
        },
		 'visit_end_datetime' : { 
			'config_type': 'PRIORITY', 
			'order': 7 
		},
        'visit_type_concept_id' : { 
			'config_type': 'CONSTANT', 
			'constant_value' : int32(32827), 
			'order': 8 
		},
            'provider_id': {
            'config_type': 'HASH',
            'fields' : ['provider_id_street', 'provider_id_city', 'provider_id_state', 'provider_id_zip',
                        'provider_id_given', 'provider_id_family',
                        'provider_id_performer_root', 'provider_id_performer_extension'],
            'order': 9
        },
		 'care_site_id': {
            'config_type': 'HASH',
            'fields': ['care_site_id_root', 'care_site_id_extension'],
            'order': 10
        },
		'visit_source_value': { 
			'config_type': 'PRIORITY', 
			'order': 11 
		},
		  'visit_source_concept_id': {
            'config_type': 'PRIORITY',
            'order': 12
        },
		 'admitting_source_concept_id': { 'config_type': None, 'order': 13},
        'admitting_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':14
        },
		'discharge_to_concept_id': { 'config_type': None, 'order': 15},
		
        'discharge_to_source_value':  {
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':16
        },
        'preceding_visit_occurrence_id': { 'config_type': None, 'order': 17},

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
            'constant_value': 'VISIT-from-procedure_procedure',
			'order':101
         }, 
}
