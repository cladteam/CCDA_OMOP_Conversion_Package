from numpy import int32
import prototype_2.value_transformations as VT

# This configuration has been updated to apply the same detailed priority logic from visit.py
# to the encompassingEncounter path, as requested in the PR review.
# The priority is: 1) Primary Code, 2) Translations, 3) Location Code, 4) Location Translations.

metadata = {
    'Visit_encompassingEncounter': {
		# FIX: there's a code for what might be admitting diagnosis here
        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Visit',
			 # encompassingEncounter in header
            'element': './hl7:componentOf/hl7:encompassingEncounter'
        },

        # NOTE: provider_id (column 9) is defined before visit_occurrence_id (column 1)
        # Reason: visit_occurrence_id hash depends on provider_id.
        # Fields are processed in the order they appear here. The 'order' attribute tells
        # the order of the columns in the resulting table. They are often the same, but not here. 
        # This ensures provider_id is fully resolved before computing visit_occurrence_id,
        # preventing incomplete or duplicate visit hashes.

        'provider_id_performer_root': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:id[not(@nullFlavor="UNK")]',
			'attribute': "root", 
		},
        'provider_id_performer_extension': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:id[not(@nullFlavor="UNK")]',
			'attribute': "extension", 
		},
        'provider_id_street': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:addr/hl7:streetAddressLine', 
			'attribute': "#text" 
		},
        'provider_id_city': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:addr/hl7:city', 
			'attribute': "#text" 
		},
        'provider_id_state': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:addr/hl7:state', 
			'attribute': "#text" 
		},
        'provider_id_zip': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:addr/hl7:postalCode', 
			'attribute': "#text" 
		},
        'provider_id_given': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:assignedPerson/hl7:name/hl7:given', 
			'attribute': "#text" 
		},
        'provider_id_family': { 
			'config_type': 'FIELD', 
			'element': 'hl7:encounterParticipant[@typeCode="ATND"]/hl7:assignedEntity/hl7:assignedPerson/hl7:name/hl7:family', 
			'attribute': "#text" 
		},
        'provider_id': {
            'config_type': 'HASH',
            'fields' : ['provider_id_street', 'provider_id_city', 'provider_id_state', 'provider_id_zip',
                        'provider_id_given', 'provider_id_family',
                        'provider_id_performer_root', 'provider_id_performer_extension'],
            'order': 9
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

        # --- Code Source: Primary Encounter Code (Priority 1) ---
        'visit_concept_code_encounter': {
            'config_type': 'FIELD',
            'element': "hl7:code",
            'attribute': "code"
        },
        'visit_concept_codeSystem_encounter': {
            'config_type': 'FIELD',
            'element': "hl7:code",
            'attribute': "codeSystem"
        },
        'visit_concept_id_encounter': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_concept_id,
            'argument_names': { 
				'concept_code': 'visit_concept_code_encounter', 
				'vocabulary_oid': 'visit_concept_codeSystem_encounter', 
				'default': None },
            'priority':  ['visit_concept_id', 1]
        },
        'visit_source_value_encounter': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names': { 
				'first_field': 'visit_concept_codeSystem_encounter', 
				'second_field': 'visit_concept_code_encounter', 
				'default': None },
            'priority':  ['visit_source_value', 1]
        },
        'visit_source_concept_id_encounter': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'visit_concept_code_encounter',
                'vocabulary_oid': 'visit_concept_codeSystem_encounter',
                'default': None
            },
            'priority': ['visit_source_concept_id', 1]
        },
        
        # --- Code Source: Encounter Translations (Priorities 2-5) ---
        'visit_concept_code_trans1': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[1]",
            'attribute': "code"
        },
        'visit_concept_system_trans1': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[1]",
            'attribute': "codeSystem"
        },
        'visit_concept_id_trans1': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_concept_id,
            'argument_names': { 
				'concept_code': 'visit_concept_code_trans1', 
				'vocabulary_oid': 'visit_concept_system_trans1', 
				'default': None },
            'priority':  ['visit_concept_id', 2]
        },
        'visit_source_value_trans1': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names': { 
				'first_field': 'visit_concept_system_trans1', 
				'second_field': 'visit_concept_code_trans1', 
				'default': None },
            'priority':  ['visit_source_value', 2]
        },
        'visit_source_concept_id_trans1': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'visit_concept_code_trans1',
                'vocabulary_oid': 'visit_concept_system_trans1',
                'default': None
            },
            'priority': ['visit_source_concept_id', 2]
        },
        'visit_concept_code_trans2': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[2]",
            'attribute': "code"
        },
        'visit_concept_system_trans2': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[2]",
            'attribute': "codeSystem"
        },
        'visit_concept_id_trans2': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_concept_id,
            'argument_names': { 
				'concept_code': 'visit_concept_code_trans2', 
				'vocabulary_oid': 'visit_concept_system_trans2', 
				'default': None },
            'priority':  ['visit_concept_id', 3]
        },
        'visit_source_value_trans2': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names': { 
				'first_field': 'visit_concept_system_trans2', 
				'second_field': 'visit_concept_code_trans2', 
				'default': None },
            'priority':  ['visit_source_value', 3]
        },
        'visit_source_concept_id_trans2': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'visit_concept_code_trans2',
                'vocabulary_oid': 'visit_concept_system_trans2',
                'default': None
            },
            'priority': ['visit_source_concept_id', 3]
        },
        'visit_concept_code_trans3': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[3]",
            'attribute': "code"
        },
        'visit_concept_system_trans3': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[3]",
            'attribute': "codeSystem"
        },
        'visit_concept_id_trans3': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_concept_id,
            'argument_names': { 
				'concept_code': 'visit_concept_code_trans3', 
				'vocabulary_oid': 'visit_concept_system_trans3', 
				'default': None },
            'priority':  ['visit_concept_id', 4]
        },
        'visit_source_value_trans3': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names': { 
				'first_field': 'visit_concept_system_trans3', 
				'second_field': 'visit_concept_code_trans3', 
				'default': None },
            'priority':  ['visit_source_value', 4]
        },
        'visit_source_concept_id_trans3': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'visit_concept_code_trans3',
                'vocabulary_oid': 'visit_concept_system_trans3',
                'default': None
            },
            'priority': ['visit_source_concept_id', 4]
        },
        'visit_concept_code_trans4': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[4]",
            'attribute': "code"
        },
        'visit_concept_system_trans4': {
            'config_type': 'FIELD',
            'element': "hl7:code/hl7:translation[4]",
            'attribute': "codeSystem"
        },
        'visit_concept_id_trans4': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_concept_id,
            'argument_names': { 
				'concept_code': 'visit_concept_code_trans4', 
				'vocabulary_oid': 'visit_concept_system_trans4', 
				'default': None },
            'priority':  ['visit_concept_id', 5]
        },
        'visit_source_value_trans4': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names': { 
				'first_field': 'visit_concept_system_trans4', 
				'second_field': 'visit_concept_code_trans4', 
				'default': None },
            'priority':  ['visit_source_value', 5]
        },
        'visit_source_concept_id_trans4': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.visit_xwalk_source_concept_id,
            'argument_names': {
                'concept_code': 'visit_concept_code_trans4',
                'vocabulary_oid': 'visit_concept_system_trans4',
                'default': None
            },
            'priority': ['visit_source_concept_id', 5]
        },

      
        # --- Default / Fallback values (Lowest Priority 9) ---
        'visit_concept_id_default': {
            'config_type': 'CONSTANT',
            'constant_value': 0,
            'priority': ['visit_concept_id', 9]
        },
        'visit_source_value_default': {
            'config_type': 'CONSTANT',
            'constant_value': '',
            'priority': ['visit_source_value', 9]
        },
        'visit_source_concept_id_default': {
            'config_type': 'CONSTANT',
            'constant_value': None,
            'priority': ['visit_source_concept_id', 9]
        },
        
        # --- Final Coalesced Fields ---
        'visit_concept_id': { 
			'config_type': 'PRIORITY', 
			'order': 3 
		},
        'visit_source_value': { 
			'config_type': 'PRIORITY', 
			'order': 11 
		},
        'visit_source_concept_id': {
            'config_type': 'PRIORITY',
            'order': 12
        },

        'visit_start_date': { 
			'config_type': 'PRIORITY', 
			'order': 4 
		},
        'visit_start_date_low': { 
			'config_type': 'FIELD', 
			'data_type': 'DATE', 
			'element': "hl7:effectiveTime/hl7:low[not(@nullFlavor=\"UNK\")]", 
			'attribute': "value", 
			'priority':  ['visit_start_date', 1] 
		},
        'visit_start_date_value': { 
			'config_type': 'FIELD', 
			'data_type': 'DATE', 
			'element': "hl7:effectiveTime", 
			'attribute': "value", 
			'priority':  ['visit_start_date', 2] 
		},
        'visit_start_datetime' : { 'config_type': 'PRIORITY', 'order': 5 },
        'visit_start_datetime_low': { 
			'config_type': 'FIELD', 
			'data_type': 'DATETIME', 
			'element': "hl7:effectiveTime/hl7:low[not(@nullFlavor=\"UNK\")]", 
			'attribute': "value", 
			'priority':  ['visit_start_datetime', 1] 
		},
        'visit_start_datetime_value': { 
			'config_type': 'FIELD', 
			'data_type': 'DATETIME', 
			'element': "hl7:effectiveTime", 
			'attribute': "value", 
			'priority':  ['visit_start_datetime', 2] 
		},        
        'visit_end_date':  { 'config_type': 'PRIORITY', 'order':6 },        
        'visit_end_date_high':  { 
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
			'priority':  ['visit_end_date', 2] 
		},
        'visit_end_date_low': { 
			'config_type': 'FIELD', 
			'data_type': 'DATE', 
			'element': "hl7:effectiveTime/hl7:low", 
			'attribute': "value", 
			'priority':  ['visit_end_date', 3] 
		},
        'visit_end_datetime' : { 'config_type': 'PRIORITY', 'order': 7 },
        'visit_end_datetime_high':  { 
			'config_type': 'FIELD', 
			'data_type': 'DATETIME', 
			'element': "hl7:effectiveTime/hl7:high[not(@nullFlavor=\"UNK\")]", 
			'attribute': "value", 
			'priority': ['visit_end_datetime', 1] 
		},
		# too loose! too forgiving? If the high value is UNK, this
        
        'visit_end_datetime_value': { 
			'config_type': 'FIELD', 
			'data_type': 'DATETIME', 
			'element': "hl7:effectiveTime", 
			'attribute': "value", 
			'priority': ['visit_end_datetime', 2] 
		},
        'visit_end_datetime_low': { 
			'config_type': 'FIELD', 
			'data_type': 'DATETIME', 
			'element': "hl7:effectiveTime/hl7:low", 
			'attribute': "value", 
			'priority': ['visit_end_datetime', 3] 
		},        
        'visit_type_concept_id' : { 
			'config_type': 'CONSTANT', 
			'constant_value' : int32(32827), 
			'order': 8 
		},

        'care_site_id_root': {
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:healthCareFacility/hl7:id',
            'attribute': "root",
        },
        'care_site_id_extension': {
            'config_type': 'FIELD',
            'element': 'hl7:location/hl7:healthCareFacility/hl7:id',
            'attribute': "extension",
        },
        'care_site_id': {
            'config_type': 'HASH',
            'fields': ['care_site_id_root', 'care_site_id_extension'],
            'order': 10
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
        
        'filename' : { 
			'config_type': 'FILENAME', 
			'order':100 
		} ,
        'cfg_name' : { 
			'config_type': 'CONSTANT', 
            'constant_value': 'Visit_encompassingEncounter',
			'order':101
		} 
    }
}

