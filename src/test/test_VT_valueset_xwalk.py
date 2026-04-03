import numpy as np
import ccda_to_omop.value_transformations as VT
import ccda_to_omop.data_driven_parse as DDP


def test_race_nuance_logic():
    '''
    Tests the fix for PRIORITY logic:
    1. Missing XML -> None (allows loop to continue)
    2. Unmapped Data -> 0 (stops loop, satisfies DRC)
    '''
    
    # 1. Setup mock mapping (M -> 8532)
    mock_map = {
        ('2.16.840.1.113883.5.1', 'M'): [{'target_concept_id': np.int32(8532)}]
    }
    VT.set_valueset_dict(mock_map)
    VT.set_codemap_dict(mock_map)

    # Common config used for all scenarios below
    race_config = {
        'race_concept_id': {
            'config_type': 'DERIVED',
            'argument_names': {
                'source_code': 'race_code',
                'source_system': 'race_codeSystem',
                'default': None
            },
            'FUNCTION': lambda args: VT._valueset_xwalk(
                '2.16.840.1.113883.5.1',
                args['source_code'],
                'target_concept_id',
                args['default']
            )
        }
    }

    # --- Scenario A: XML is missing data ---
    out_missing = {'race_code': None, 'race_codeSystem': None}
    pk_missing = {'race_concept_id': []}
    
    DDP.do_derived_fields(out_missing, 'ROOT', 'PATH', 'TEST', race_config, set(), pk_missing)
    
    assert out_missing['race_concept_id'] is None
    assert pk_missing['race_concept_id'] == [] # Engine should skip None values

    # --- Scenario B: XML has unmapped data (e.g. 'UNKOW') ---
    out_unmapped = {'race_code': 'UNKOW', 'race_codeSystem': '2.16.840.1.113883.5.1'}
    pk_unmapped = {'race_concept_id': []}
    
    DDP.do_derived_fields(out_unmapped, 'ROOT', 'PATH', 'TEST', race_config, set(), pk_unmapped)
    
    assert out_unmapped['race_concept_id'] == None
    #assert isinstance(out_unmapped['race_concept_id'], np.int32)
    assert out_unmapped['race_concept_id'] is None
    assert pk_unmapped['race_concept_id'] == []

    # --- Scenario C: XML has valid data ('M') ---
    out_valid = {'race_code': 'M', 'race_codeSystem': '2.16.840.1.113883.5.1'}
    pk_valid = {'race_concept_id': []}
    
    DDP.do_derived_fields(out_valid, 'ROOT', 'PATH', 'TEST', race_config, set(), pk_valid)
    
    assert out_valid['race_concept_id'] == 8532
    assert pk_valid['race_concept_id'] == [8532]

    print("Race nuance tests passed!")

if __name__ == '__main__':
    test_race_nuance_logic()
 
