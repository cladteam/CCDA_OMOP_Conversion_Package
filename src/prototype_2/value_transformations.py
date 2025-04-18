import logging
#from . import get_codemap_xwalk
#from . import get_ccda_value_set_mapping_table
#from . import get_visit_concept_xwalk_mapping
from . import get_codemap_xwalk_dict
from . import get_ccda_value_set_mapping_table_dict
from . import get_visit_concept_xwalk_mapping_dict
from typeguard import typechecked
from numpy import int32

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
    id_value =  _codemap_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

    if id_value is not None:
        return int32(id_value)
    else:
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
    return _codemap_xwalk_DICT(vocabulary_oid, concept_code, column_name, default)


def _codemap_xwalk_DICT(vocabulary_oid, concept_code, column_name, default):
    if get_codemap_xwalk_dict() is None:
        raise Exception("visit_concept_xwalk_mapping_dict is not initialized in prototype_2/__init__.py for value_transformations.py")

    codemap_xwalk_mapping_dict= get_visit_concept_xwalk_mapping_dict()
    map_dict = codemap_xwalk_mapping_dict[(vocabulary_oid, concept_code)]

    if map_dict is None:
        return default

    if len(map_dict) < 1:
        return default

    if len(map_dict) > 1:
       logger.warning(f"_visit_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

    return map_dict[column_name]


#def _codemap_xwalk_DATASET(vocabulary_oid, concept_code, column_name, default):
    #df = codemap_xwalk[ (codemap_xwalk['vocab_oid'] == vocabulary_oid) & (codemap_xwalk['src_code']  == concept_code) ]
    # 2025-03-04 new version of codemap schema:
    #df = codemap_xwalk[ (codemap_xwalk['src_vocab_code_system'] == vocabulary_oid) & (codemap_xwalk['src_code']  == concept_code) ]
    #if len(df) < 1:
    #if df.count() < 1:
    #   return default
    #if len(df) > 1: 
    #if df.count() > 1:
    #    logger.warning(f"_codemap_xwalk(): more than one  value for column \"{column_name}\" from \"{vocabulary_oid}\" \"{concept_code}\", chose the first")
    #if df is None:
    #    return default
    #return df[column_name].iloc[0]  # pandas?
    #$return df.first()[column_name]

    
    
############################################################################
"""
    table: visit_concept_xwalk_mapping_dataset
    functions: visit_xwalk...
""" 

def visit_xwalk_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: concept_id AS INTEGER (because that's what's in the table), not necessarily standard
        throws/raises when visit_concept_xwalk_mapping_dataset is None
    """
    id_value = _visit_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

    if id_value is not None:
        return int32(id_value)
    else:
        return None

    
def visit_xwalk_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: domain_id
        throws/raises when visit_concept_xwalk_mapping_dataset is None
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
        throws/raises when visit_concept_xwalk_mapping_dataset is None
    """ 
    id_value = _visit_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 
    if id_value is not None:
        return int32(id_value)
    else:
        return None
    

def _visit_xwalk(vocabulary_oid, concept_code, column_name, default):
    return _visit_xwalk_DICTIONARY(vocabulary_oid, concept_code, column_name, default)


def _visit_xwalk_DICTIONARY(vocabulary_oid, concept_code, column_name, default):
    """ expects: vocabulary_oid, concept_code
        throws/raises when visit_concept_xwalk_mapping_dataset is None
    """
    if get_visit_concept_xwalk_mapping_dict() is None:
        raise Exception("visit_concept_xwalk_mapping_dict is not initialized in prototype_2/__init__.py for value_transformations.py")

    visit_concept_xwalk_mapping_dict =  get_visit_concept_xwalk_mapping_dict()
    map_dict = visit_concept_xwalk_mapping_dict[(vocabulary_oid, concept_code)]

    if map_dict is None:
        return default

    if len(map_dict) < 1:
        return default

    if len(map_dict) > 1:
       logger.warning(f"_visit_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

    return map_dict[column_name]


#def _visit_xwalk_DATASET(vocabulary_oid, concept_code, column_name, default):
#    if get_visit_concept_xwalk_mapping_dataset() is None:
#        raise Exception("visit_concept_xwalk_mapping_dataset is not initialized in prototype_2/__init__.py for value_transformations.py")
#    visit_concept_xwalk_mapping_dataset =  get_visit_concept_xwalk_mapping_dataset()
#    df = visit_concept_xwalk_mapping_dataset[ 
#        (visit_concept_xwalk_mapping_dataset['codeSystem'] == vocabulary_oid) &
#        (visit_concept_xwalk_mapping_dataset['src_cd']  == concept_code) ]
#    #if len(df) < 1:
#    if df.count() < 1:
#        return default
#    #if len(df) > 1:
#    if df.count() > 1:
#       logger.warning(f"_visit_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")
#    if df is None:
#        return default
#    #return df[column_name].iloc[0]
    
    
    
############################################################################
"""
    table: ccda_value_set_mapping_table_dataset
    functions: valueset_xwalk...
"""    

def valueset_xwalk_concept_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: concept_id AS INTEGER
        throws/raises when ccda_value_set_mapping_table_dataset is None
    """
    id_value = _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'target_concept_id', args_dict['default']) 

    if id_value is not None:
        return int32(id_value)
    else:
        return None
    
    
def valueset_xwalk_domain_id(args_dict):
    """ expects: vocabulary_oid, concept_code
        returns: domain_id
        throws/raises when ccda_value_set_mapping_table_dataset is None
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
        throws/raises when ccda_value_set_mapping_table_dataset is None
    """
    
    id_value =  _valueset_xwalk(args_dict['vocabulary_oid'], args_dict['concept_code'], 
                'source_concept_id', args_dict['default']) 
    if id_value is not None:
        return int32(id_value)
    else:
        return None
   

def _valueset_xwalk(vocabulary_oid, concept_code, column_name, default):
    return _valueset_xwalk_DICT(vocabulary_oid, concept_code, column_name, default)


def _valueset_xwalk_DICT(vocabulary_oid, concept_code, column_name, default):

    if get_ccda_value_set_mapping_table_dict() is None:
        raise Exception("ccda_value_set_mapping_talbe_dict is not initialized in prototype_2/__init__.py for value_transformations.py")

    ccda_value_set_mapping_dict =  get_ccda_value_set_mapping_table_dict()
    map_dict = ccda_value_set_mapping_dict[(vocabulary_oid, concept_code)]

    if map_dict is None:
        return default

    if len(map_dict) < 1:
        return default

    if len(map_dict) > 1:
       logger.warning(f"_valueset_xwalk(): more than one  concept for  \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")

    return map_dict[column_name]


#def _valueset_xwalk_DATASET(vocabulary_oid, concept_code, column_name, default):
#    if get_ccda_value_set_mapping_table_dataset() is None:
#        raise Exception("ccda_value_set_mapping_table_dataset is not initialized in prototype_2/__init__.py for value_transformations.py")
#
#    ccda_value_set_mapping_table_dataset =  get_ccda_value_set_mapping_table_dataset()
#    df = ccda_value_set_mapping_table_dataset[ (ccda_value_set_mapping_table_dataset['codeSystem'] == vocabulary_oid) &
#                                                   (ccda_value_set_mapping_table_dataset['src_cd']  == concept_code) ]
#    #if len(df) < 1:
#    if df.count() < 1:
#        return default
#
#    #if len(df) > 1:
#    if df.count() > 1:
#        logger.warning(f"_valueset_xwalk(): more than one  value for column \"{column_name}\" from  \"{vocabulary_oid}\" \"{concept_code}\", chose the first")
#
#    if df is None:
#        return default
#    #return df[column_name].iloc[0]
#    return df.first()[column_name]

############################################################################

def map_valuesets_to_omop(args_dict):
    """ expects: vocabulary_oid, concept_code
    """
    vocab_oid = args_dict['vocabulary_oid']
    concept_code = args_dict['concept_code']
    ##codemap_xwalk

    
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


def concat_fields(args_dict):
    """
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
    

