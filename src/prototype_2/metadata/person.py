from numpy import int64
from numpy import int32
import prototype_2.value_transformations as VT

metadata = {
    'Person': {
        'root': {
            'config_type': 'ROOT',
            'expected_domain_id': 'Person',
            'element': "./hl7:recordTarget/hl7:patientRole"
        },

        'person_id_root': {
    	    'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
    	    'attribute': "root",
    	},

        'person_id_extension': {
    	    'config_type': 'FIELD',
            'element': 'hl7:id[not(@nullFlavor="UNK")]',
    	    'attribute': "extension",
    	},

    	'person_id': { 
       	    'config_type': 'HASH',
            'fields' : [ 'person_id_root', 'person_id_extension', 
			             'gender_concept_code', 'gender_concept_codeSystem',
			             'race_concept_code', 'race_concept_codeSystem',
			             'gender_concept_code', '_concept_codeSystem',
						 'ethnicity_concept_code', 'ethnicity_concept_codeSystem',
						 'birth_datetime', 'address_1', 'city', 'state', 'zip'],
            'order' : 1
        },

        # Step 1: Define parsers and a derived lookup for the 'direct' path
        'gender_concept_code_direct': { 
            'config_type': 'FIELD', 
            'element': 'hl7:patient/hl7:administrativeGenderCode[not(@nullFlavor="OTH")]', 
            'attribute': "code" 
        },
        'gender_concept_codeSystem_direct': { 
            'config_type': 'FIELD', 
            'element': 'hl7:patient/hl7:administrativeGenderCode[not(@nullFlavor="OTH")]', 
            'attribute': "codeSystem" 
        },
        'gender_concept_id_direct': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'gender_concept_code_direct', 
                                'vocabulary_oid': 'gender_concept_codeSystem_direct', 
                                'default': None },
            'priority': ('gender_concept_id', 1)
        },
        # Step 2: Define parsers and a derived lookup for the 'translation' path
        'gender_concept_code_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:administrativeGenderCode/hl7:translation", 
            'attribute': "code" 
        },
        'gender_concept_codeSystem_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:administrativeGenderCode/hl7:translation", 
            'attribute': "codeSystem" 
        },
        'gender_concept_id_translated': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'gender_concept_code_translated', 
                                'vocabulary_oid': 'gender_concept_codeSystem_translated', 
                                'default': None },
            'priority': ('gender_concept_id', 2)
        },
        # Step 3: Define the final field that coalesces the results from the prioritized derived fields
        'gender_concept_id': { 
            'config_type': 'PRIORITY', 
            'order': 2 
        },


        'year_of_birth': {
            'config_type': 'DERIVED',
    	    'FUNCTION': VT.extract_year_of_birth,
    	    'argument_names': {
    		    'date_object': 'birth_datetime',
    	    },
            'order': 3
        },
        'month_of_birth': {
            'config_type': 'DERIVED',
    	    'FUNCTION': VT.extract_month_of_birth,
    	    'argument_names': {
    		    'date_object': 'birth_datetime',
    	    },
            'order': 4
        },
    	'day_of_birth': {
            'config_type': 'DERIVED',
    	    'FUNCTION': VT.extract_day_of_birth,
    	    'argument_names': {
    		    'date_object': 'birth_datetime',
    	    },
            'order': 5
    	},
    	'birth_datetime': {
    	    'config_type': 'FIELD',
            'data_type':'DATETIME',
    	    'element': "hl7:patient/hl7:birthTime",
    	    'attribute': "value",
            'order': 6
    	},
        # Step 1: Define parsers and a derived lookup for the 'direct' path
        'race_concept_code_direct': { 
            'config_type': 'FIELD', 
            'element': 'hl7:patient/hl7:raceCode[not(@nullFlavor="OTH")]', 
            'attribute': "code" 
        },
        'race_concept_codeSystem_direct': { 
            'config_type': 'FIELD', 
            'element': 'hl7:patient/hl7:raceCode[not(@nullFlavor="OTH")]', 
            'attribute': "codeSystem" 
        },
        'race_concept_id_direct': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'race_concept_code_direct', 
                                'vocabulary_oid': 'race_concept_codeSystem_direct', 
                                'default': None },
            'priority': ('race_concept_id', 1)
        },
        # Step 2: Define parsers and a derived lookup for the 'sdtc' path
        'race_concept_code_sdtc': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/sdtc:raceCode", 
            'attribute': "code" 
        },
        'race_concept_codeSystem_sdtc': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/sdtc:raceCode", 
            'attribute': "codeSystem" 
        },
        'race_concept_id_sdtc': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'race_concept_code_sdtc', 
                                'vocabulary_oid': 'race_concept_codeSystem_sdtc', 
                                'default': None },
            'priority': ('race_concept_id', 2)
        },
        # Step 3: Define parsers and a derived lookup for the 'translation' path
        'race_concept_code_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:raceCode/hl7:translation", 
            'attribute': "code" 
        },
        'race_concept_codeSystem_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:raceCode/hl7:translation", 
            'attribute': "codeSystem" 
        },
        'race_concept_id_translated': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'race_concept_code_translated', 
                                'vocabulary_oid': 'race_concept_codeSystem_translated', 
                                'default': None },
            'priority': ('race_concept_id', 3)
        },
        # Step 4: Define the final field that coalesces the results from the prioritized derived fields
        'race_concept_id':{ 
            'config_type': 'PRIORITY', 
            'order': 7 
        },

        # Step 1: Define parsers and a derived lookup for the 'direct' path
        'ethnicity_concept_code_direct': { 
            'config_type': 'FIELD', 
            'element': 'hl7:patient/hl7:ethnicGroupCode[not(@nullFlavor="OTH")]', 
            'attribute': "code" 
        },
        'ethnicity_concept_codeSystem_direct': { 
            'config_type': 'FIELD', 
            'element': 'hl7:patient/hl7:ethnicGroupCode[not(@nullFlavor="OTH")]', 
            'attribute': "codeSystem" 
        },
        'ethnicity_concept_id_direct': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'ethnicity_concept_code_direct', 
                                'vocabulary_oid': 'ethnicity_concept_codeSystem_direct', 
                                'default': None },
            'priority': ('ethnicity_concept_id', 1)
        },
        # Step 2: Define parsers and a derived lookup for the 'sdtc' path
        'ethnicity_concept_code_sdtc': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/sdtc:ethnicGroupCode", 
            'attribute': "code" 
        },
        'ethnicity_concept_codeSystem_sdtc': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/sdtc:ethnicGroupCode", 
            'attribute': "codeSystem" 
        },
        'ethnicity_concept_id_sdtc': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'ethnicity_concept_code_sdtc', 
                                'vocabulary_oid': 'ethnicity_concept_codeSystem_sdtc', 
                                'default': None },
            'priority': ('ethnicity_concept_id', 2)
        },
        # Step 3: Define parsers and a derived lookup for the 'translation' path
        'ethnicity_concept_code_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:ethnicGroupCode/hl7:translation", 
            'attribute': "code" 
        },
        'ethnicity_concept_codeSystem_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:ethnicGroupCode/hl7:translation", 
            'attribute': "codeSystem" 
        },
        'ethnicity_concept_id_translated': {
            'config_type': 'DERIVED', 
            'FUNCTION': VT.valueset_xwalk_concept_id,
            'argument_names': { 'concept_code': 'ethnicity_concept_code_translated', 
                                'vocabulary_oid': 'ethnicity_concept_codeSystem_translated', 
                                'default': None },
            'priority': ('ethnicity_concept_id', 3)
        },
        # Step 4: Define the final field that coalesces the results from the prioritized derived fields
        'ethnicity_concept_id': { 
            'config_type': 'PRIORITY', 
            'order': 8 
        },
        
        'address_1': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:streetAddressLine',
            'attribute': "#text"
        },
        'city': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:city',
            'attribute': "#text"
        },
        'state': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:state',
            'attribute': "#text"
        },
        'zip': {
            'config_type': 'FIELD',
            'element': 'hl7:addr/hl7:postalCode',
            'attribute': "#text"
        },
        'location_id': { 
            'config_type': 'HASH',
            'fields' : [ 'address_1', 'city', 'state', 'zip'  ],
            'order': 9
        },
        'provider_id': { 'config_type': None, 'order': 10 },
        'care_site_id': { 
            'config_type': 'CONSTANT',
            'constant_value' : int64(0),
	    'order':11
        },
        'person_source_value': { 
            'config_type': 'CONSTANT',
            'constant_value' : '',
	    'order':12
        },
        
        'gender_source_value_direct': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:administrativeGenderCode", 
            'attribute': "code", 
            'priority': ('gender_source_value', 1) 
        },
        'gender_source_value_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:administrativeGenderCode/hl7:translation", 
            'attribute': "code", 
            'priority': ('gender_source_value', 2) 
        },
        'gender_source_value': {
       	    'config_type': 'PRIORITY',
            'order': 13 
         },
        'gender_source_concept_id': { 'config_type': None, 'order': 14 },
        
        'race_source_value_direct': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:raceCode", 
            'attribute': "code", 
            'priority': ('race_source_value', 1) 
        },
        'race_source_value_sdtc': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/sdtc:raceCode", 
            'attribute': "code", 
            'priority': ('race_source_value', 2) 
        },
        'race_source_value_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:raceCode/hl7:translation", 
            'attribute': "code", 
            'priority': ('race_source_value', 3) 
        },
        'race_source_value': {
      	    'config_type': 'PRIORITY',
            'order': 15
        },
        'race_source_concept_id': { 'config_type': None, 'order': 16 },

        'ethnicity_source_value_direct': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:ethnicGroupCode", 
            'attribute': "code", 
            'priority': ('ethnicity_source_value', 1) 
        },
        'ethnicity_source_value_sdtc': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/sdtc:ethnicGroupCode", 
            'attribute': "code", 
            'priority': ('ethnicity_source_value', 2) 
        },
        'ethnicity_source_value_translated': { 
            'config_type': 'FIELD', 
            'element': "hl7:patient/hl7:ethnicGroupCode/hl7:translation", 
            'attribute': "code", 
            'priority': ('ethnicity_source_value', 3) 
        },
        'ethnicity_source_value': {
       	    'config_type': 'PRIORITY',
            'order': 17
        },
        'ethnicity_source_concept_id': { 'config_type': None, 'order': 18 },

        'filename' : {
		'config_type': 'FILENAME',
		'order':100
	}
    
    }
}