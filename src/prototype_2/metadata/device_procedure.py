
import prototype_2.value_transformations as VT
from numpy import int32
#
metadata = {
    'Device_procedure': {
    	'root': {
    	    'config_type': 'ROOT',
            'expected_domain_id': 'Device',
            # Medical equipment section, entry, procedure
    	    'element':
    		  ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		   "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.23']"
    		   "/../hl7:entry/hl7:procedure[@moodCode='EVN']/"
               "hl7:statusCode[@code='active' or @code='completed']/..")
        },

        'device_exposure_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'root'
    	},
    	'device_exposure_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
            'attribute': 'extension'
    	},
        'device_exposure_id': {
    	    'config_type': 'HASH', 
            'fields' : ['person_id', 'provider_id',
                        #'visit_occurrence_id',
                        'device_concept_id_code', 'device_concept_id_codeSystem',
                        'device_exposure_start_date', 'device_exposure_start_datetime',
                        'device_exposure_end_date', 'device_exposure_end_datetime',
                        'device_exposure_id_root', 'device_exposure_id_extension'],
            'order': 1
        },

    	'person_id': {
    	    'config_type': 'FK',
    	    'FK': 'person_id',
            'order': 2
    	},
        
        # participant[@typeCode='DEV']/participantRole/playingDevice/..
    	'device_concept_id_code': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:participant[@typeCode='DEV']/hl7:participantRole/hl7:playingDevice/hl7:code" ,
    	    'attribute': "code"
    	},
    	'device_concept_id_codeSystem': {
    	    'config_type': 'FIELD',
    	    'element': "hl7:participant[@typeCode='DEV']/hl7:participantRole/hl7:playingDevice/hl7:code",
    	    'attribute': "codeSystem"
    	},
    	'device_concept_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_concept_id,  
    	    'argument_names': {
    		    'concept_code': 'device_concept_id_code',
    		    'vocabulary_oid': 'device_concept_id_codeSystem',
                'default': 0
            },
            'order': 3
    	},

    	'domain_id': {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.codemap_xwalk_domain_id,
    	    'argument_names': {
    		    'concept_code': 'device_concept_id_code',
    		    'vocabulary_oid': 'device_concept_id_codeSystem',
                'default': 0
    	    }
    	},
               
        'device_exposure_start_date': {
            'config_type': 'PRIORITY',
            'order': 4
        },
        'device_exposure_start_date_low': {
    	    'config_type': 'FIELD',
            'data_type': 'DATE',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
            'priority': ('device_exposure_start_date', 1)
    	},
        'device_exposure_start_date_value': {
    	    'config_type': 'FIELD',
            'data_type': 'DATE',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'priority': ('device_exposure_start_date', 2)
    	},
                
        'device_exposure_start_datetime': {
            'config_type': 'PRIORITY',
            'order': 5
        },
        'device_exposure_start_datetime_low': {
    	    'config_type': 'FIELD',
            'data_type': 'DATETIME',
    	    'element': "hl7:effectiveTime/hl7:low",
    	    'attribute': "value",
            'priority': ('device_exposure_start_datetime', 1)
    	},     
        'device_exposure_start_datetime_value': {
    	    'config_type': 'FIELD',
            'data_type': 'DATETIME',
    	    'element': "hl7:effectiveTime",
    	    'attribute': "value",
            'priority': ('device_exposure_start_datetime', 2)
    	},
        
        'device_exposure_end_date': {'config_type': None, 'order': 6},
        
        'device_exposure_end_datetime': {'config_type': None, 'order': 7},
        
        'device_type_concept_id': {
            'config_type': 'CONSTANT',
            'constant_value' : int32(32817), # OMOP concept ID for 'EHR'
            'order': 8
        },
        
        # participant[@typeCode='DEV']/participantRole/..
        'unique_device_id':{
            'config_type': 'FIELD',
            'element': "hl7:participant[@typeCode='DEV']/hl7:participantRole/hl7:id[@root='2.16.840.1.113883.3.3719']", 
            'attribute': "extension",
            'order': 9
        },

        'quantity': {'config_type': None, 'order': 10},
        
        'provider_id': { 
    	    'config_type': 'FK',
    	    'FK': 'provider_id',
            'order': 11
    	},

        'visit_occurrence_id': {
    	    'config_type': 'FK',
    	    'FK': 'visit_occurrence_id',
            'order': 12
    	},     
        
        'visit_detail_id': {'config_type': None, 'order': 13},

        'device_source_value': {
       	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.concat_fields,  
    	    'argument_names': {
    		    'first_field': 'device_concept_id_code',
    		    'second_field': 'device_concept_id_codeSystem',
                'default': 'error'
    	    },
            'order': 14
        },

        'device_source_concept_id': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.codemap_xwalk_source_concept_id,  
            'argument_names': {
                'concept_code': 'device_concept_id_code',
                'vocabulary_oid': 'device_concept_id_codeSystem',
                'default': 0
            },
            'order': 15
        },
        
     	'filename' : {
		    'config_type': 'FILENAME',
		    'order':100
	    },
        'cfg_name' : { 
			'config_type': 'CONSTANT', 
            'constant_value': 'Device_procedure',
			'order':101
		}         
    }
}