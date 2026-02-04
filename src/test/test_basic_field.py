
import prototype_2.value_transformations as VT
import prototype_2.data_driven_parse as DDP
from lxml import etree as ET


def test_simple_field():
    ns = {
        # '': 'urn:hl7-org:v3',  # default namespace
        'hl7': 'urn:hl7-org:v3',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'sdtc': 'urn:hl7-org:sdtc'
    }

    XML_text="""
        <grouping_element>
            <simple_element>
                <value value="simple text" />
            </simple_element>
        </grouping_element>
    """

    output_dict = {}
    config_name="TEST"
    #root_path="./grouping_element/simple_element" 
    root_path="./simple_element" 

    try:
        tree=ET.fromstring(XML_text)
        root_element_list = tree.xpath(root_path, namespaces=ns)

        config_dict = {
            'text_field': { 
                'config_type': 'FIELD',
                'element': 'value',
                'attribute': 'value',
                'order': 202
            },
        }

        error_fields_set = set()
        pk_dict={}
        print(f"\nstarting simple {len(root_element_list)}")
        for root_element in root_element_list:
            DDP.do_basic_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, pk_dict)
            print(f"dict: {output_dict}")
            assert output_dict['text_field'] == 'simple text'
        print("done")
    except Exception as e:
        print(e)
        assert False
        raise e

def test_simple_field_that_needs_stripped():
    ns = {
        # '': 'urn:hl7-org:v3',  # default namespace
        'hl7': 'urn:hl7-org:v3',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'sdtc': 'urn:hl7-org:sdtc'
    }

    XML_text="""
        <grouping_element>
            <simple_element>
                <value value=" simple text " />
            </simple_element>
        </grouping_element>
    """

    output_dict = {}
    config_name="TEST"
    root_path="./simple_element" 

    try:
        tree=ET.fromstring(XML_text)
        root_element_list = tree.xpath(root_path, namespaces=ns)

        config_dict = {
            'text_field': { 
                'config_type': 'FIELD',
                'element': 'value',
                'attribute': 'value',
                'order': 202
            },
        }

        error_fields_set = set()
        pk_dict={}
        print(f"\nstarting simple {len(root_element_list)}")
        for root_element in root_element_list:
            DDP.do_basic_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, pk_dict)
            print(f"dict: {output_dict}")
            assert output_dict['text_field'] == 'simple text'
        print("done")
    except Exception as e:
        print(e)
        assert False
        raise 

def test_code_field():
    ns = {
        # '': 'urn:hl7-org:v3',  # default namespace
        'hl7': 'urn:hl7-org:v3',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'sdtc': 'urn:hl7-org:sdtc'
    }
                #<hl7:value[@xsi:type="CE"] code="123" />
    XML_text="""
        <grouping_element>
            <code_element>
                <value type="CE" code="123" />
            </code_element>
        </grouping_element>
    """

    output_dict = {}
    config_name="TEST"
    root_path="code_element" 

    try:
        tree=ET.fromstring(XML_text)
        root_element_list = tree.xpath(root_path, namespaces=ns)

        config_dict = {
            'text_field': { 
                'config_type': 'FIELD',
                'element': 'value',
                #'attribute': 'hl7:value',
                #'attribute': 'hl7:value[@type="CE"]',
                'attribute': 'code',
                'order': 202
            },
        }

        error_fields_set = set()
        pk_dict={}
        print(f"\nstarting code {len(root_element_list)}")
        for root_element in root_element_list:
            DDP.do_basic_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, pk_dict)
            print(f"dict: {output_dict}")
            assert output_dict['text_field'] == '123'
        print("done")

    except Exception as e:
        print(e)
        assert False

        
def test_text_field():
    ns = {
        # '': 'urn:hl7-org:v3',  # default namespace
        'hl7': 'urn:hl7-org:v3',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'sdtc': 'urn:hl7-org:sdtc'
    }

    XML_text="""
        <grouping_element>
            <text_element>
                <value>multi-line
                       element</value>
            </text_element>
        </grouping_element>
    """
    the_text="""multi-line
                       element"""

    output_dict = {}
    config_name="TEST"
    root_path="text_element" 

    try:
        tree=ET.fromstring(XML_text)
        root_element_list = tree.xpath(root_path, namespaces=ns)

        config_dict = {
            'text_field': { 
                'config_type': 'FIELD',
                'element': 'value',
                'attribute': '#text',
                'order': 202
            },
        }

        error_fields_set = set()
        pk_dict={}
        print(f"\nstarting text {len(root_element_list)}")
        for root_element in root_element_list:
            DDP.do_basic_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, pk_dict)
            print(f"dict: {output_dict}")
            assert output_dict['text_field'] == the_text
        print('done')

    except Exception as e:
        print(e)
        assert False
        raise e



def test_quant_field():
    ns = {
        # '': 'urn:hl7-org:v3',  # default namespace
        'hl7': 'urn:hl7-org:v3',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'sdtc': 'urn:hl7-org:sdtc'
    }
                # <hl7:value xsi:type="PQ" value="93" />
    XML_text="""
        <grouping_element>
            <quantity_element>
                <value type="PQ" value="93" />
            </quantity_element>
        </grouping_element>
    """

    output_dict = {}
    config_name="TEST"
    root_path="quantity_element" 

    try:
        tree=ET.fromstring(XML_text)
        root_element_list = tree.xpath(root_path, namespaces=ns)

                # 'element': 'hl7:value',
        config_dict = {
            'text_field': { 
                'config_type': 'FIELD',
                'element': 'value',
                'attribute': 'value',
                'order': 202
            },
        }

        error_fields_set = set()
        pk_dict={}
        print(f"\nstarting quant {len(root_element_list)}")
        for root_element in root_element_list:
            DDP.do_basic_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, pk_dict)
            print(f"dict: {output_dict}")
            assert output_dict['text_field'] == '93'
        print("done")

    except Exception as e:
        print(e)
        assert False
        raise e
        
def test_string_truncation_default():
    ns = {'hl7': 'urn:hl7-org:v3'}
    # 60 characters total
    long_string = "A" * 60 
    XML_text=f"""
        <grouping_element>
            <simple_element>
                <value value="{long_string}" />
            </simple_element>
        </grouping_element>
    """
    output_dict = {}
    config_dict = {
        'text_field': { 
            'config_type': 'FIELD',
            'element': 'value',
            'attribute': 'value',
            'order': 1
        },
    }
    
    tree = ET.fromstring(XML_text)
    root_element = tree.xpath("./simple_element", namespaces=ns)[0]
    
    # DDP.do_basic_fields should now truncate this to 50 by default
    DDP.do_basic_fields(output_dict, root_element, "./simple_element", "TEST", config_dict, set(), {})
    
    assert len(output_dict['text_field']) == 50
    assert output_dict['text_field'] == "A" * 50

def test_string_truncation_explicit():
    ns = {'hl7': 'urn:hl7-org:v3'}
    # 40 characters total
    long_string = "B" * 40 
    XML_text=f"""
        <grouping_element>
            <stop_reason_element>
                <value value="{long_string}" />
            </stop_reason_element>
        </grouping_element>
    """
    output_dict = {}
    config_dict = {
        'stop_reason': { 
            'config_type': 'FIELD',
            'element': 'value',
            'attribute': 'value',
            'length': 20, # Explicit override for stop_reason
            'order': 1
        },
    }
    
    tree = ET.fromstring(XML_text)
    root_element = tree.xpath("./stop_reason_element", namespaces=ns)[0]
    
    DDP.do_basic_fields(output_dict, root_element, "./stop_reason_element", "TEST", config_dict, set(), {})
    
    assert len(output_dict['stop_reason']) == 20
    assert output_dict['stop_reason'] == "B" * 20

def test_constant_truncation():
    # 60 character constant, target length 20 (e.g., stop_reason)
    long_const = "C" * 60
    config_dict = {
        'stop_reason': { 
            'config_type': 'CONSTANT',
            'constant_value': long_const,
            'length': 20,
            'order': 1
        },
    }
    output_dict = {}
    DDP.do_constant_fields(output_dict, None, "", "TEST", config_dict, set())
    
    assert len(output_dict['stop_reason']) == 20
    assert output_dict['stop_reason'] == "C" * 20

def test_derived_truncation():
    config_dict = {
        'value_as_string': {
            'config_type': 'DERIVED',
            'FUNCTION': VT.concat_fields,
            'argument_names': {
                'first_field': 'f1',
                'second_field': 'f2'
            },
            'length': 10,
            'order': 8 
        },
    }
    output_dict = {'f1': 'PART_ONE_', 'f2': 'PART_TWO_'} 
    pk_dict = {'value_as_string': []}
    
    DDP.do_derived_fields(output_dict, None, "", "TEST", config_dict, set(), pk_dict)
    
    assert output_dict['value_as_string'] == "PART_ONE_|"
 