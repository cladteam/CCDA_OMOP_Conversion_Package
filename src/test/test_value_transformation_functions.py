
import prototype_2.value_transformations as VT
import prototype_2.data_driven_parse as DDP


data_dict = {
    "a": 1, "b": 2, "c":3
}


def test_example():
    assert 1==1

def this_one_wont_run():
    assert 1==1



def test_concat_field_list_values():
    ''' tests by calling through do_derived2_field()
    '''
    output_dict = { "a": 1, "b": 2, "c":3 }
    root_element='TEST_ROOT'
    root_path = "a/b/c/"
    config_name="TEST"
    config_dict = {
        'id_values_chris': { 
            'config_type': 'DERIVED2',
            'FUNCTION': VT.concat_field_list_values,
            'argument_list': { 'key_list': ['a', 'c'] },
           'order': 201  
        },
    }

    error_fields_set = set()
    DDP.do_derived2_fields( output_dict, root_element, root_path, config_name, config_dict, error_fields_set)
    assert output_dict['id_values_chris'] == '1|3'

    
def test_do_derived_lakshmi():
    ''' tests by calling through do_derived_fields()
    '''
    output_dict = {"a": 1, "b": 2, "c":3 }
    root_element = 'TEST_ROOT'
    root_path = "a/b/c/"
    config_name = "TEST"
    # config_dict :dict[str, dict[str, str | None]],
    config_dict = {
        'id_values_lakshmi': { 
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_field_list_lakshmi,
            'argument_names': { 'f1': 'a', 'f2': 'c' },
            'order': 200
        }
    }

    error_fields_set = set()
    DDP.do_derived_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set)
    assert output_dict['id_values_lakshmi'] == '1|3'

        
def test_do_derived_lakshmi_2():
    ''' tests by calling through do_derived_field()
    '''
    output_dict = {"a": 1, "b": 2, "c":3 }
    root_element = 'TEST_ROOT'
    root_path = "a/b/c/"
    config_name = "TEST"
    # config_dict :dict[str, dict[str, str | None]],
    config_dict = {
        'id_values_lakshmi_2': { 
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_field_list_lakshmi_2,
            'argument_names': { 'f1': 'a', 'f2': 'c' },
            'order': 200
        }
    }

    error_fields_set = set()
    DDP.do_derived_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set)
    print(output_dict)
    assert output_dict['id_values_lakshmi_2'] == '1|3'


def test_concat_field_list_names():
    ''' tests by calling through do_derived2_field()
    '''
    output_dict = { "a": 1, "b": 2, "c":3}
    root_element='TEST_ROOT'
    root_path = "a/b/c/"
    config_name="TEST"
    config_dict = {
        'id_args': { 
            'config_type': 'DERIVED2',
            'FUNCTION': VT.concat_field_list_names,
            'argument_list': { 'key_list': ['a', 'c']},
            'order': 202
        },
    }

    error_fields_set = set()
    DDP.do_derived2_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set)
    assert output_dict['id_args'] == 'a|c'
 