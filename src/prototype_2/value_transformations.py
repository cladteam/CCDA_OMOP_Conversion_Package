from typeguard import typechecked
from numpy import int32
import pandas as pd

import logging
logging.basicConfig(
        filename="layer_datasets.log",
        filemode="w",
        level=logging.INFO ,
        format='%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d %(message)s')
        
"""
    Functions for use in DERVIED fields.
    The configuration for this type of field is:
        <new field name>: {
    	    'config_type': 'DERIVED',
    	    'FUNCTION': VT.<function_name>
    	    'argument_names': {
    		    <arg_name_1>: <field_name_1>
                ...
       		    <arg_name_n>: <field_name_n>
                'default': <default_value>
    	    }
        }
    The config links argument names to functions defined here to field names
    for the values. The code that calls these functions does the value lookup,
    so they operate on values, not field names or keys.
"""    

logger = logging.getLogger(__name__)

# --- Start of Moved Code from __init__.py ---
# These dictionaries are now defined and handled here.
codemap_dict = None
valueset_dict = None
visitmap_dict = None


def set_codemap_dict(map):
    if map is not None:
        logger.info(f"set_codemap_dict {len(map)}")
    else:
        logger.info("set_codemap_dict None map")
    global codemap_dict
    codemap_dict = map

def get_codemap_dict():
    return codemap_dict


def set_valueset_dict(map):
    if map is not None:
        logger.info(f"set_valueset_dict {len(map)}")
    else:
        logger.info("set_valueset_dict None map")
    global valueset_dict
    valueset_dict = map 

def get_valueset_dict():
    return valueset_dict


def set_visitmap_dict(map):
    if map is not None:
        logger.info(f"set_visitmap_dict {len(map)}")
    else:
        logger.info("set_visitmap_dict None map")
    global visitmap_dict
    visitmap_dict = map

def get_visitmap_dict():
    return visitmap_dict


def cast_as_string(args_dict):
    string_value = args_dict['input']
    type_value = args_dict['type']
    if type_value == 'ST':
        return str(string_value)
    else:
        return None


def cast_as_number(args_dict):
    string_value = args_dict['input']
    type_value = args_dict['type']
    if type_value == 'PQ':
        return int(string_value)
    else:
        return None


def cast_as_concept_id(args_dict):  # TBD FIX TODO
    raise Exception("cast_as_concept not implemented")

    string_value = args_dict['input']
    type_value = args_dict['type']
    if type_value == 'CD' or type_value == 'CE':
        return string_value
    else:
        return None

    return ""



    
############################################################################
"""
    table: codemap_xwalk
    functions: codemap_xwalk...
"""

def codemap_xwalk_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: concept_id AS INTEGER (because that's what's in the table), not necessarily standard
        throws/raises when codemap_xwalk is None
    """
    id_value = _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

    if id_value is not None:
        logger.debug(f"codemap_xwalk_concept_id concept_id is {id_value}  for {args_dict}")
        return int32(id_value)
    else:
        logger.error(f"codemap_xwalk_concept_id concept_id is None  for {args_dict}")
        return None


def codemap_xwalk_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: domain_id
        throws/raises when codemap_xwalk is None
    """
    id_value = _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_domain_id', args_dict['default']) 

    if id_value is not None:
        return str(id_value)
    else:
        return None


def codemap_xwalk_source_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: unmapped concept_id AS INTEGER (because that's what's in the table), not necessarily standard
        throws/raises when codemap_xwalk is None
    """
    id_value =  _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 

    if id_value is not None:
        return int32(id_value)
    else:
        return None


def _codemap_xwalk(vocabulary_oid, concept_code, column_name, default):

    if get_codemap_dict() is None:
        logger.error("codemap_dict is not initialized in prototype_2/value_transformations.py for value_transformations.py")
        raise Exception("codemap_dict is not initialized in prototype_2/value_transformations.py for value_transformations.py")
    codemap_xwalk_mapping_dict= get_codemap_dict()

    if (vocabulary_oid, concept_code) in codemap_xwalk_mapping_dict:
        mapping_rows = codemap_xwalk_mapping_dict[(vocabulary_oid, concept_code)]
    else:
        logger.error(f"value_transformations.py _codemap_xwalk vocabulary_id:\"{vocabulary_oid}\" ,{type(vocabulary_oid)}, code:\"{concept_code}\", {type(concept_code)}  not present or not found")
        return default

    if mapping_rows is None:
        logger.error(f"codemap_dict mapping_rows is None  for vocab:{vocabulary_oid} code:{concept_code} column_name:{column_name} default:{default}")
        return default

    if len(mapping_rows) < 1:
        logger.error(f"codemap_dict mapping_rows is <1 for vocab:{vocabulary_oid} code:{concept_code} column_name:{column_name} default:{default}")
        return default

    if len(mapping_rows) > 1:
        logger.error(f"_codemap_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

    if column_name in mapping_rows[0]:
        column_value = mapping_rows[0][column_name]
    else:
        logger.error(f"value_transformations.py _codemap_xwalk doens't have the column{column_name}....{mapping_rows[0]}")
        logger.error("f (cont) {mapping_rows}")
    return column_value


############################################################################
"""
    table: visit_concept_xwalk_mapping
    functions: visit_xwalk...
""" 

def visit_xwalk_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: concept_id AS INTEGER (because that's what's in the table), not necessarily standard
    """
    id_value = _visit_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

    if args_dict['vocabulary_oid'] == '2.16.840.1.113883.5.4' and \
       args_dict['concept_code'] == 'AMB':
        if id_value is None:
            raise Exception(f"AMB not mapped {args_dict} ")
        elif id_value == 0:
            raise Exception(f"AMB mapped to No Matching Concept {args_dict} \"{id_value}\" ")
        elif id_value != 9202 and id_value != '9202':
            raise Exception(f"AMB not mapped correctly {args_dict} \"{id_value}\" type:{type(id_value)}")

    if args_dict['vocabulary_oid'] == '2.16.840.1.113883.6.12' and \
        args_dict['concept_code'] == 'AMB':
        if id_value is None:
            raise Exception(f"AMB not mapped {args_dict} ")
        elif id_value == 0:
            raise Exception(f"AMB mapped to No Matching Concept {args_dict} \"{id_value}\" ")
        elif id_value != 9202 and id_value != '9202':
            raise Exception(f"AMB not mapped correctly {args_dict} \"{id_value}\" type:{type(id_value)}")

    if id_value is not None:
        logger.debug(f"visit_xwalk_concept_id concept_id is {id_value}  for {args_dict}")
        return int32(id_value)
    else:
        logger.error(f"visit_xwalk_concept_id concept_id is None  for {args_dict}")
        return None


def visit_xwalk_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: domain_id
    """
    id_value = _visit_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_domain_id', args_dict['default']) 

    if id_value is not None:
        return str(id_value)
    else:
        return None


def visit_xwalk_source_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: unmapped concept_id AS INTEGER (because that's what's in the table), not necessarily standard
    """ 
    id_value = _visit_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 
    if id_value is not None:
        return int32(id_value)
    else:
        return None


def _visit_xwalk(vocabulary_oid, concept_code, column_name, default):
    visitmap_dict =  get_visitmap_dict()
    if visitmap_dict is None:
        raise Exception("visitmap_dict is not initialized in prototype_2/value_transformations.py for value_transformations.py")

    if len(visitmap_dict) < 1:
        raise Exception("visitmap_dict has zero length prototype_2/value_transformations.py for value_transformations.py")

    mapping_rows = visitmap_dict[(vocabulary_oid, concept_code)]
    if mapping_rows is None:
        logger.error(f"visitmap_dict mapping_rows is None  for vocab:{vocabulary_oid} code:{concept_code} column_name:{column_name} default:{default}")
        return default

    if len(mapping_rows) < 1:
        logger.error(f"visitmap_dict mapping_rows is <1  for vocab:{vocabulary_oid} code:{concept_code} column_name:{column_name} default:{default}")
        return default

    if len(mapping_rows) > 1:
       logger.warn(f"_visit_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

    return mapping_rows[0][column_name]


    
    
############################################################################
"""
    table: ccda_value_set_mapping_table
    functions: valueset_xwalk...
"""    

def valueset_xwalk_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: concept_id AS INTEGER
    """

    id_value = _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'],
                'target_concept_id', args_dict['default'])


    if args_dict['vocabulary_oid'] == '2.16.840.1.113883.5.1' and \
       args_dict['concept_code'] == 'F':
        if id_value is None:
            raise Exception(f"F not mapped {args_dict} ")
        elif id_value == 0:
            raise Exception(f"F mapped to No Matching Concept {args_dict} \"{id_value}\" ")
        elif id_value != 8532 and id_value != "8532":
            raise Exception(f"F not mapped correctly {args_dict} \"{id_value}\" type:{type(id_value)} ")
        ##elif id_value == 8532 or id_value == "8532":
        ##    raise Exception(f"F MAPPED CORRECTLY {args_dict} \"{id_value}\" type:{type(id_value)} ")

    if id_value is not None:
        return int32(id_value)
    else:
        return None       
    
def valueset_xwalk_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: domain_id
    """
    id_value =  _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_domain_id', args_dict['default']) 
    
    if id_value is not None:
        return str(id_value)
    else:
        return None

    
def valueset_xwalk_source_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: unmapped concept_id AS INTEGER not necessarily standard
    """
    
    id_value =  _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 
    if id_value is not None:
        return int32(id_value)
    else:
        return None


def _valueset_xwalk(vocabulary_oid, concept_code, column_name, default):
    # Check Dict
    if get_valueset_dict() is None:
        logger.error("valueset_dict is not initialized in prototype_2/value_transformations.py for value_transformations.py _valueset_xwalk_DICT()")
        raise Exception("valueset_dict is not initialized in prototype_2/value_transformations.py for value_transformations.py _valueset_xwalk_DICT()")

    valueset_dict =  get_valueset_dict()
    if len(valueset_dict) < 1:
        raise Exception("valueset_dict has zero length in prototype_2/value_transformations.py for value_transformations.py _valueset_xwalk_DICT()")

    # Get and Check results
    mapping_rows = valueset_dict[(vocabulary_oid, concept_code)]
    if mapping_rows is None:
        logger.error(f"valueset_xwalk_dict mapping_rows is None  for vocab:{vocabulary_oid} code:{concept_code} column_name:{column_name} default:{default}")
        return default
    if len(mapping_rows) < 1 :
        if  vocabulary_oid is not None and concept_code is not None:
           logger.error(f"valueset_xwalk_dict mapping_rows is <1  for vocab:{vocabulary_oid} code:{concept_code} column_name:{column_name} default:{default}")
        return default

    if len(mapping_rows) > 1:
       logger.warn(f"_valueset_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")
    return mapping_rows[0][column_name]



############################################################################

    
@typechecked
def extract_day_of_birth(args_dict : dict[str, any]) -> int32:
    # assumes input is a datetime
    date_object = args_dict['date_object']
    if date_object is not None:
        return int32(date_object.day)
    return None


@typechecked
def extract_month_of_birth(args_dict : dict[str, any]) -> int32:
    # assumes input is a datetime
    date_object = args_dict['date_object']
    if date_object is not None:
        return int32(date_object.month)
    return None


@typechecked
def extract_year_of_birth(args_dict : dict[str, any]) -> int32:
    # assumes input is a datetime
    date_object = args_dict['date_object']
    if date_object is not None:
        return int32(date_object.year)
    return None


def concat_field_list_names(args_dict, data_dict):
    '''
        A DERIVED2 style function.
        Looks for a argument with the name of 'args' under the 'argument_list'
        brought in from the parse configuration. That list is a list of keys
        whose data we're interested in fetching from the data_dict.

        args_dict: the field's paragraph in the parse configuration
        data_dict: the dictionary of values being built up for an OMOP row 
           by the parse configuration where all this comes from.

        Returns: a joined list of the keys. The data_dict is unused.
    '''
    args_key = 'argument_list'
    if not args_dict:
        return f"no args dict \"{args_dict}\" "
    if args_key not in args_dict.keys():
        return f"no  \"{args_key}\" in args dict {args_dict}"
    if 'key_list' not in args_dict['argument_list'].keys():
        return f"no  \"key_list\" in args dict {args_dict}"

    return  "|".join(args_dict['argument_list']['key_list'])


def concat_field_list_values(args_dict, data_dict):
    '''
        A DERIVED2 style function.
        Looks for a argument with the name of 'args' under the 'argument_list'
        brought in from the parse configuration. That list is a list of keys
        whose data we're interested in fetching from the data_dict.

        args_dict: the field's paragraph in the parse configuration
        data_dict: the dictionary of values being built up for an OMOP row 
           by the parse configuration where all this comes from.

        Returns: a joined list of the data values associated with those keys.
    '''
    if not args_dict:
        return f"no args dict \"{args_dict}\" "
    if 'argument_list' not in args_dict.keys():
        return f"no  argument_list in args dict {args_dict}"
    if 'key_list' not in args_dict['argument_list'].keys():
        return f"no  \"key_list\" in args dict {args_dict}"

    return "|".join(map(str, map(lambda x: data_dict[x], args_dict['argument_list']['key_list'] )))




def concat_fields(args_dict):
    """
      A DERIVED style function.
      input key "delimiter" is a character to use to separate the fields
      following items in dict are the names of keys in the values to concat
      
      returns one string, the concatenation of values corresponding to args 2-n, using arg 1 as a delimieter
    """
    delimiter = '|'

        
    if (args_dict['first_field'] is None) & (args_dict['second_field'] is None):
        return ''
    
    elif (args_dict['first_field'] is None) & (args_dict['second_field'] is not None):
        return args_dict['second_field']
    
    elif (args_dict['first_field'] is not None) & (args_dict['second_field'] is None):
        return args_dict['first_field']
    else :
        values_to_concat = [ args_dict['first_field'], args_dict['second_field'] ]
        return delimiter.join(values_to_concat)
    
####################################################################################################

partner_map = None
mspi_lookup_map = None 

def set_partner_map(m):
    """Initializes the partner map on the executor."""
    global partner_map
    partner_map = m

def get_partner_map():
    return partner_map

def get_data_partner_id(args_dict):
    """
    DERIVED function: Returns Data Partner ID for a given filename.
    Expects 'filename' and 'default' in args_dict.
    """
    fname = args_dict.get('filename')
    default = args_dict.get('default', 0)
    
    mapping = get_partner_map()
    
    if mapping and fname in mapping:
        try:
            partner_id = mapping[fname]
            if partner_id is not None:
                return int32(partner_id)
        except (ValueError, TypeError) as e:
            logger.error(f"get_data_partner_id: Conversion error for {fname}: {e}")
            return int32(default)
            
    return int32(default)

def set_mspi_map(m):
    """Initializes the MSPI (person_id) map on the executor."""
    global mspi_lookup_map
    mspi_lookup_map = m

def get_mspi_map():
    return mspi_lookup_map

def map_filename_to_mspi(args_dict):
    """
    DERIVED function: Returns MSPI for a given filename to be used as person_id.
    Expects 'filename' and 'default' in args_dict.
    """
    fname = args_dict.get('filename')
    default = args_dict.get('default')
    
    mapping = get_mspi_map()
    
    if mapping and fname in mapping:
        try:
            
            mspi_value = mapping[fname]
            if mspi_value is not None:
                return int(str(mspi_value))
        except (ValueError, TypeError) as e:
            logger.error(f"map_filename_to_mspi: Conversion error for {fname}: {e}")
            return default
    
    logger.warning(f"map_filename_to_mspi: filename {fname} not found in metadata map. Returning default.")
    return default