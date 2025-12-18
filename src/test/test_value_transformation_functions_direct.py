
import prototype_2.value_transformations as VT
import prototype_2.data_driven_parse as DDP


data_dict = {
    "a": 1, "b": 2, "c":3
}


def test_example():
    assert 1==1

def this_one_wont_run():
    assert 1==1


def test_concat_fields_lakshmi():
    print("\n-->concat lakshmi")
    output_dict = { "a": 1, "b": 2, "c":3 }
    args_dict={
        "f1": "a",
        "f2": "c"
    } 
    x = VT.concat_field_list_lakshmi(args_dict)
    assert x=="a|c"


def test_concat_fields_names():
    print("\n-->concat chris ")
    output_dict = { "a": 1, "b": 2, "c":3 }
    args_dict={
        "argument_list":{'args':  ["a", "c"]}
    }
    x = VT.concat_field_list_names(args_dict, output_dict)
    assert x=="a|c"
