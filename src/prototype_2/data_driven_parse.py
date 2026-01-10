
""" Table-Driven ElementTree parsing in Python

 This version puts the paths into a data structure and explores using
 one function driven by the data.
 - The mapping_dict is hard-coded here. An next step would be to read that in from a file.
 - Value transformation is stubbed out waiting for vocabularies to be loaded, and to
   figure out how to use them once there.

  - Deterministic hashing in Python3 https://stackoverflow.com/questions/27954892/deterministic-hashing-in-python-3
  - https://stackoverflow.com/questions/16008670/how-to-hash-a-string-into-8-digits 

 Chris Roeder

    Call Graph:
    - main
      - process_file
        - parse_doc
          -  parse_configuration_from_file
            - parse_config_from_single_root
              - do_none_fields
              - do_constant_fields
              - do_basic_fields
              - do_derived_fields
              - do_domain_fields
              - do_hash_fields
              - do_priority_fields


    Config dictionary structure: dict[str, dict[str, dict[str, str ] ] ]
    metadata = {
        config_dict = {
            field_details_dict = {
               attribute: value 
            }
        }
    }
    So there are many config_dicts, each roughly for a domain. You may
    have more than one per domain when there are more than a single
    location for a domain.
    Each config_dict is made up of many fields for the OMOP table it 
    creates. There are non-output fields used as input to derived 
    fields, like the vocabulary and code used to find the concept_id.
    Each field_spec. has multiple attributes driving that field's
    retrieval or derivation.

    PK_dict :dict[str, any]
    key is the field_name, any is the value. Value can be a string, int, None or a list of same.

    output_dict :dict[str, any]
    omop_dict : dict[str, list[any] for each config you have a list of records



    XML terms used specifically:
    - element is a thing in a document inside angle brackets like <code code="1234-5" codeSystem="LOINC"/
    - attributes are code and codeSystem in the above example
    - text is when there are both start and end parts to the element like <text>foobar</text>. "foobar" is
       the text in an element that has a tag = 'text'
    - tag see above

"""


import argparse
import datetime
from dateutil.parser import parse
import hashlib
import logging
import math
import os
import pandas as pd
import sys
import traceback
import zlib

from numpy import int32
from numpy import int64
from collections import defaultdict
from lxml import etree as ET
from lxml.etree import XPathEvalError
from typeguard import typechecked

from prototype_2 import value_transformations as VT
from prototype_2.metadata import get_meta_dict
from prototype_2 import ddl as DDL

from prototype_2 import visit_reconcilliation as VR

logger = logging.getLogger(__name__)

DO_VISIT_DETAIL = False

ns = {
   # '': 'urn:hl7-org:v3',  # default namespace
   'hl7': 'urn:hl7-org:v3',
   'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
   'sdtc': 'urn:hl7-org:sdtc'
}


#from foundry.transforms import Dataset
#concept_xwalk = Dataset.get("concept_xwalk")
#concept_xwalk_files = concept_xwalk.files().download()

#def create_8_byte_hash(input_string):
#    hash_value = hashlib.md5(input_string.encode('utf-8'))
#    int_hash_value = int(hash_value.hexdigest(), 16)
#    bigint_hash_value = ctypes.c_int64(int_hash_value % 2**64).value
#    return bigint_hash_value

#@typechecked
def create_hash(input_string) -> int64 | None:
    """ matches common SQL code when that code also truncates to 13 characters
        SQL: cast(conv(substr(md5(test_string), 1, 15), 16, 10) as bigint) as hashed_value
        32 bit
    """
    if input_string == '':
        return None
    
    hash_value = hashlib.md5(input_string.encode('utf-8').upper())
    truncated_hash = hash_value.hexdigest()[0:13]
    int_trunc_hash_value = int(truncated_hash, 16)
    return int64(int_trunc_hash_value)

def create_hash_too_long(input_string):
    # 64 bit is 16 hex characters, output is way longer...
    if input_string == '':
        return None
    hash_value = hashlib.md5(input_string.encode('utf-8').upper())
    hash_digest = hash_value.hexdigest()[0:15]
    long_hash_value = int(hash_digest, 31)
    return long_hash_value



@typechecked
def cast_to_date(string_value) ->  datetime.date | None:
    # TODO does CCDA always do dates as YYYYMMDD ?
    # https://build.fhir.org/ig/HL7/CDA-ccda/StructureDefinition-USRealmDateTimeInterval-definitions.html
    # doc says YYYMMDD... examples show ISO-8601. Should use a regex and detect parse failure.
    # TODO  when  is it date and when datetime

    try:
        datetime_val = parse(string_value)
        return datetime_val.date()
    except Exception as x:
        print(f"ERROR couldn't parse {string_value} as date. Exception:{x}")
        #return None
        return  datetime.date.fromisoformat("1970-01-01")
    except ValueError as ve:
        print(f"ERROR couldn't parse {string_value} as date. ValueError:{ve}")
        #return None
        return  datetime.date.fromisoformat("1970-01-01")

def cast_to_datetime(string_value) -> datetime.datetime | None:
    try:
        datetime_val = parse(string_value)
        return datetime_val
    except Exception as x:
        print(f"ERROR couldn't parse {string_value} as datetime. {x}")
        #return None
        return  datetime.date.fromisoformat("1970-01-01T00:00:00")


@typechecked
def parse_field_from_dict(field_details_dict :dict[str, str], root_element, 
        config_name, field_tag, root_path) ->  None | str | float | int | int32 | int64 | datetime.datetime | datetime.date | list:
    """ Retrieves a value for the field descrbied in field_details_dict that lies below
        the root_element.
        Domain and field_tag are here for error messages.
    """

    if 'element' not in field_details_dict:
#        logger.error(("FIELD could find key 'element' in the field_details_dict:"
#                     f" {field_details_dict} root:{root_path}"))
        return None

    logger.info(f"    FIELD {field_details_dict['element']} for {config_name}/{field_tag}")
    field_element = None
    try:
        field_element = root_element.xpath(field_details_dict['element'], namespaces=ns)
    except XPathEvalError as p:
        pass
#        logger.error(f"ERROR (often inconsequential) {field_details_dict['element']} {p}")
        ###print(f"FAILED often inconsequential  {field_details_dict['element']} {p}")
    if field_element is None:
##        logger.error((f"FIELD could not find field element {field_details_dict['element']}"
##                      f" for {config_name}/{field_tag} root:{root_path} {field_details_dict} "))
        return None

    if 'attribute' not in field_details_dict:
##        logger.error((f"FIELD could not find key 'attribute' in the field_details_dict:"
##                     f" {field_details_dict} root:{root_path}"))
        return None

    logger.info((f"       ATTRIBUTE   {field_details_dict['attribute']} "
                 f"for {config_name}/{field_tag} {field_details_dict['element']} "))
    attribute_value = None
    if len(field_element) > 0:
        attribute_value = field_element[0].get(field_details_dict['attribute'])
        if field_details_dict['attribute'] == "#text":
            try:
                attribute_value = ''.join(field_element[0].itertext())
            except Exception as e:
                logger.error((f"no text elemeent for field element {field_element} "
                        f"for {config_name}/{field_tag} root:{root_path} "
                        f" dict: {field_element[0].attrib} EXCEPTION:{e}"))
        if attribute_value is None:
            logger.warning((f"no value for field element {field_details_dict['element']} "
                        f"for {config_name}/{field_tag} root:{root_path} "
                        f" dict: {field_element[0].attrib}"))
    else:
        logger.warning((f"no element at path {field_details_dict['element']} "
                        f"for {config_name}/{field_tag} root:{root_path} "))

    # Do data-type conversions
    if 'data_type' in field_details_dict:
        if attribute_value is not None and attribute_value == attribute_value:
            if field_details_dict['data_type'] == 'DATE':
                #attribute_value = cast_to_date(attribute_value)
                try:
                    attribute_value = cast_to_date(attribute_value)
                    if attribute_value != attribute_value:
                        attribute_value = datetime.date.fromisoformat("1970-01-01")
                except Exception as e:
                    attribute_value = datetime.date.fromisoformat("1970-01-01")
                    print(f"cast to date failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to date failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'DATETIME':
                try:
                    attribute_value = cast_to_datetime(attribute_value)
                    if attribute_value != attribute_value:
                        attribute_value = datetime.datetime.fromisoformat("1970-01-01T00:00:00")
                except Exception as e:
                    attribute_value = datetime.datetime.fromisoformat("1970-01-01T00:00:00")
                    print(f"cast to datetime failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to datetime failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'LONG':
                try:
                    attribute_value = int64(attribute_value)
                except Exception as e:
                    print(f"cast to int64 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to int64 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'INTEGER':
                try:
                    attribute_value = int32(attribute_value)
                except Exception as e:
                    print(f"cast to int32 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to int32 failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'BIGINTHASH':
                try:
                    attribute_value = create_hash(attribute_value)
                except Exception as e:
                    print(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'TEXT':
                try:
                    attribute_value = str(attribute_value)
                except Exception as e:
                    print(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to hash failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            elif field_details_dict['data_type'] == 'FLOAT':
                try:
                    attribute_value = float(attribute_value)
                except Exception as e:
                    print(f"cast to float failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
                    logger.error(f"cast to float failed for config:{config_name} field:{field_tag} val:{attribute_value}") 
            else:
                print(f" UNKNOWN DATA TYPE: {field_details_dict['data_type']} {config_name} {field_tag}")
                logger.error(f" UNKNOWN DATA TYPE: {field_details_dict['data_type']} {config_name} {field_tag}")

            #if attribute_value is None or attribute_value != attribute_value:
            if attribute_value != attribute_value: # checking for NaN or NaT, but not None
                #raise Exception(f"No Nones, N/As, NaNs or NaTs allowed(2)! {config_name} {field_tag}")
                wth = f"No  NaNs or NaTs allowed(2)! {config_name} {field_tag}" 
                raise Exception(wth)
            return attribute_value

        else:
            print(f" no value: {field_details_dict['data_type']} {config_name} {field_tag}")
            logger.error(f" no value: {field_details_dict['data_type']} {config_name} {field_tag}")

        #if attribute_value is None or attribute_value != attribute_value:
        if attribute_value != attribute_value: # checking for NaN or NaT, but not None
            if field_details_dict['data_type'] == 'DATETIME' or field_details_dict['data_type'] == 'DATE':
                attribute_value=datetime.date.fromisoformat("1970-01-01")
                return attribute_value
            else:
                #raise Exception(f"No Nones, N/As, NaNs or NaTs allowed(1)! {config_name} {field_tag}")
                wth = f"No NaNs or NaTs allowed(1)! {config_name} {field_tag}" 
                raise Exception(wth)
                return None
    else:
        #if attribute_value is None or attribute_value != attribute_value:
        if attribute_value != attribute_value:
            #raise Exception(f"No Nones, N/As, NaNs or NaTs allowed(3)! {config_name} {field_tag}")
            wth = f"No  NaNs or NaTs allowed(3)! {config_name} {field_tag}" 
            raise Exception(wth)
        return attribute_value


@typechecked
def do_none_fields(output_dict :dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date ],
                   root_element, root_path, config_name,  
                   config_dict :dict[str, dict[str, str | None]], 
                   error_fields_set :set[str]):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     NONE FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag is None:
            output_dict[field_tag] = None

            
@typechecked
def do_constant_fields(output_dict :dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date], 
                       root_element, root_path, config_name,  
                       config_dict :dict[str, dict[str, str | None]], 
                       error_fields_set :set[str]):

    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     CONSTANT FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag == 'CONSTANT':
            constant_value = field_details_dict['constant_value']
            output_dict[field_tag] = constant_value

            
@typechecked
def do_filename_fields(output_dict :dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date], 
                       root_element, root_path, config_name,  
                       config_dict :dict[str, dict[str, str | None]], 
                       error_fields_set :set[str],
                       filename :str):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FILENAME FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        config_type_tag = field_details_dict['config_type']
        if config_type_tag == 'FILENAME':
            output_dict[field_tag] = filename

            
@typechecked
def do_basic_fields(output_dict :dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date], 
                    root_element, root_path, config_name,  
                    config_dict :dict[str, dict[str, str | None] ], 
                    error_fields_set :set[str], 
                    pk_dict :dict[str, list[any]] ):
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FIELD config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        type_tag = field_details_dict['config_type']
        if type_tag == 'FIELD':
            try:
                attribute_value = parse_field_from_dict(field_details_dict, root_element,
                                                    config_name, field_tag, root_path)
                if attribute_value is str:
                    output_dict[field_tag] = attribute_value.strip()
                else:
                    output_dict[field_tag] = attribute_value
                logger.info(f"     FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
            except KeyError as ke:
                logger.error(f"key erorr: {ke}")
                logger.error(f"  {field_details_dict}")
                logger.error(f"  FIELD for {config_name}/{field_tag} \"{attribute_value}\"")
                raise

        elif type_tag == 'PK':
            # PK fields are basically regular FIELDs that go into the pk_dict
            # NB. so do HASH fields.
            logger.info(f"     PK for {config_name}/{field_tag}")
            attribute_value = parse_field_from_dict(field_details_dict, root_element,
                                                    config_name, field_tag, root_path)
            if attribute_value is str:
                output_dict[field_tag] = attribute_value.strip()
            else:
                output_dict[field_tag] = attribute_value
            pk_dict[field_tag].append(attribute_value)
            logger.info("PK {config_name}/{field_tag} {type(attribute_value)} {attribute_value}")
            

@typechecked 
def do_foreign_key_fields(output_dict :dict[str, None | str | float | int | int32 | int64 |datetime.datetime | datetime.date], 
                    root_element, root_path, config_name,  
                    config_dict :dict[str, dict[str, str | None] ], 
                    error_fields_set :set[str], 
                    pk_dict :dict[str, list[any]] ):
    """
        When a configuration has an FK field, it uses the tag in that configuration
        to find corresponding values from PK fields.  This mechanism is intended for
        PKs uniquely identified in a CCDA document header for any places in the sections
        it would be used as an FK. This is typically true for person_id and visit_occurrence_id, 
        but there are exceptions. In particular, some documents have multiple encounters, so
        you can't just naively choose the only visit_id because there are many.
        
        Choosing the visit is more complicated, because it requires a join (on date ranges)
        between the domain table and the encounters table, or portion of the header that
        has encompassingEncounters in it. This code, the do_foreign_key_fields() function
        operates in too narrow a context for that join. These functions are scoped down
        to processing a single config entry for a particular OMOP domain. The output_dict, 
        parameter is just for that one domain. It wouldn't include the encounters.
        For example, the measurement_results.py file has a configuration for parsing OMOP 
        measurement rows out of an XML file. The visit.py would have been previosly processed
        and it's rows stashed away elsewhere in the parse_doc() function whose scope is large
        enough to consider all the configurations. So the visit choice/reconcilliation
        must happen from there.
        
        TL;DR not all foreign keys are resolved here. In particular, domain FK references,
        visit_occurrence_id, in cases where more than a single encounter has previously been
        parsed, are not, can not, be resolved here. See the parse_doc() function for how
        it is handled there.
        
    """
    for (field_tag, field_details_dict) in config_dict.items():
        logger.info((f"     FK config:'{config_name}' field_tag:'{field_tag}'"
                     f" {field_details_dict}"))
        type_tag = field_details_dict['config_type']
        
        if type_tag == 'FK':
            logger.info(f"     FK for {config_name}/{field_tag}")
            if field_tag in pk_dict:
                if len(pk_dict[field_tag]) == 1:
                    output_dict[field_tag] = pk_dict[field_tag][0]
                else:
                    # can't really choose the correct value here. Is attempted in reconcile_visit_FK_with_specific_domain() later, below.
                    ###print(f"WARNING FK has more than one value {field_tag}, tagging with 'RECONCILE FK' ")
                    logger.info(f"WARNING FK has more than one value {field_tag}, tagging with 'RECONCILE FK'")
                    # original hack:
                    output_dict[field_tag] = None;

            else:
                path = root_path + "/"
                if 'element' in field_details_dict:
                    path = path + field_details_dict['element'] + "/@"
                else:
                    path = path + "no element/"
                if 'attribute' in field_details_dict:
                    path = path + field_details_dict['attribute']
                else:
                    path = path + "no attribute/"

##                if field_tag in pk_dict and len(pk_dict[field_tag]) == 0:
###                    logger.error(f"FK no value for {field_tag}  in pk_dict for {config_name}/{field_tag}")
##                else:
##                    logger.error(f"FK could not find {field_tag}  in pk_dict for {config_name}/{field_tag}")
                output_dict[field_tag] = None
                error_fields_set.add(field_tag)

@typechecked
def do_derived_fields(output_dict: dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date],
                      root_element, root_path, config_name,
                      config_dict: dict[str, dict[str, str | None]],
                      error_fields_set: set[str],
                      pk_dict: dict[str, list[any]]):
    """ Do/compute derived values now that their inputs should be available in the output_dict
        Except for a special argument named 'default', when the value is what is other wise the field to look up in the output dict.

        This set-up is for functions that expect explicit named arguments. This code here adds values for those arguments to the
        the dictionary passed to the function.
        It's tempting to want to pass a list of arguments, but that's not how this function works.

        Also a PK
    """
    for (field_tag, field_details_dict) in config_dict.items():
        if field_details_dict['config_type'] == 'DERIVED':
            logger.info(f"     DERIVING {field_tag}, {field_details_dict}")
            # NB Using an explicit dict here instead of kwargs because this code here
            # doesn't know what the keywords are at 'compile' time.
            args_dict = {}
            for arg_name, field_name in field_details_dict['argument_names'].items():
                if arg_name == 'default':
                        args_dict[arg_name] = field_name
                else:
                    logger.info(f"     -- {field_tag}, arg_name:{arg_name} field_name:{field_name}")
                    try:
                        if field_name not in output_dict:
                            error_fields_set.add(field_tag)
                            logger.error((f"DERIVED config:{config_name} field:{field_tag} could not "
                                      f"find {field_name} in {output_dict}"))
                        try:
                            args_dict[arg_name] = output_dict[field_name]
                        except Exception as e:
                            print(f"-------error field_name:{field_name}  arg_name:{arg_name}  {e}")
                            print(traceback.format_exc(e))
                            error_fields_set.add(field_tag)
                            logger.error((f"DERIVED {field_tag} arg_name: {arg_name} field_name:{field_name}"
                                        f" args_dict:{args_dict} output_dict:{output_dict}"))
                            logger.error(f"DERIVED exception {e}")
                    except TypeError as te:
                        print(f"-------error field_name:{field_name}  arg_name:{arg_name}  {te}")
                        print(traceback.format_exc(te))

            try:
                function_value = field_details_dict['FUNCTION'](args_dict)
                output_dict[field_tag] = function_value
                logger.info((f"     DERIVED {function_value} for "
                                f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))
                # Treat derived fields (like person_id) as Primary Keys (PKs)
                # and stash the value so that FK fields in subsequent domains can find it.
                if function_value is not None:
                    if function_value not in pk_dict[field_tag]:
                        pk_dict[field_tag].append(function_value)
            except KeyError as e:
                #print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                print(f"DERIVED key error on: {e}")
                logger.error(f"DERIVED key error on: {e}")
                logger.error(f"DERIVED KeyError {field_tag} function can't find key it expects in {args_dict}")
                output_dict[field_tag] = None
            except TypeError as e:
                #print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                print(f"DERIVED type error exception: {e}")
                logger.error(f"DERIVED type error exception: {e}")
                logger.error((f"DERIVED TypeError {field_tag} possibly calling something that isn't a function"
                              " or that function was passed a null value." 
                              f" {field_details_dict['FUNCTION']}. You may have quotes "
                              "around it in  a python mapping structure if this is a "
                              f"string: {type(field_details_dict['FUNCTION'])}"))
                output_dict[field_tag] = None
            except Exception as e:
                print(f"DERIVED exception: {e}")
                logger.error(f"DERIVED exception: {e}")
                output_dict[field_tag] = None


@typechecked
def do_derived2_fields(output_dict :dict[str, list | None | str | float | int | int32 | int64 | datetime.datetime | datetime.date], 
                      root_element, root_path, config_name,
                      config_dict :dict[str, dict[str, str | None | list]],
                      error_fields_set :set[str]):
    '''
    This version is for functions that are smart enough to mine the output_dict with keys passed in.
    It allows for a list of arguments, but requires looking the value up explicitly
    '''


    for (field_tag, field_details_dict) in config_dict.items():
        #output_dict[field_tag] = f"XX:\"{field_tag}\   \"{field_details_dict}\" "
        if field_details_dict['config_type'] == 'DERIVED2':
            output_dict[field_tag] = None
            try:
                function_value = field_details_dict['FUNCTION'](field_details_dict, output_dict)
                output_dict[field_tag] = function_value
            except Exception as e:
                print(f"Error in do_derived2_fields {config_name} {field_tag}")
                print(traceback.format_exc(e))



                
@typechecked
def do_hash_fields(output_dict: dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date],
                   root_element, root_path, config_name,
                   config_dict: dict[str, dict[str, str | None]],
                   error_fields_set: set[str],
                   pk_dict: dict[str, list[any]]):
    """ These are basically derived, but the argument is a lsit of field names, instead of
        a fixed number of individually named fields.
        Dubiously useful in an environment where IDs are  32 bit integers.
        See the code above for converting according to the data_type attribute
        where a different kind of hash is beat into an integer.

        ALSO A PK
    """
    for (field_tag, field_details_dict) in config_dict.items():
        if field_details_dict['config_type'] == 'HASH':
            value_list = []
            if 'fields' not in field_details_dict:
                logger.error(f"HASH field {field_tag} is missing 'fields' attributes in config:{config_name}")
            for field_name in field_details_dict['fields'] :
                if field_name in output_dict:
                    value_list.append(output_dict[field_name])
                else:
                    logger.error("unknown HASH field  {field_name}")
                    print("unknown HASH field  {field_name}")
            hash_input =  "|".join(map(str, value_list))
            #hash_value = create_hash_part1(hash_input)
            hash_value = create_hash(hash_input)
            output_dict[field_tag] = hash_value
            # treat as PK and include in that dictionary
            pk_dict[field_tag].append(hash_value)
            logger.info((f"     HASH (PK) {hash_value} for "
                         f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))

            
@typechecked
def do_priority_fields(output_dict: dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date],
                       root_element, root_path, config_name,
                       config_dict: dict[str, dict[str, str | None]],
                       error_fields_set: set[str],
                       pk_dict: dict[str, list[any]]) -> dict[str, list]:
    """
        ARGS expected in config: 
       	    'config_type': 'PRIORITY',
            'defult': 0, in case there is no non-null value in the priority change and we don't want a null value in the end.
            'order': 17
        Returns the list of  priority_names so the chosen one (first non-null) can be 
        added to output fields Also, adds this field to the PK list?
        This is basically what SQL calls a coalesce.

        Within the config_dict, find all fields tagged with priority and group
        them by their priority names in a dictionary keyed by that name
        Ex. { 'person_id': [ ('person_id_ssn', 1), ('person_id_unknown', 2) ]
        Sort them, choose the first one that is not None.

        NB now there is a separate config_type PRIORITY to compliment the priority attribute.
        So you might have person_id_npi, person_id_ssn and person_id_hash tagged with priority
        attributes to create a field person_id, but then also another field, just plain person_id.
        The point of it is to have a unique place to put that field's order attribute. The code
        here (and in the ordering code later) must be aware of a  that field in the
        config_dict (where it isn't used) ...and not clobber it. It's an issue over in the
        sorting/ordering.
    """

    # Create Ref Data
    # for each new field, create a list of source fields and their priority:
    # Ex. [('person_id_other', 2), ('person_id_ssn', 1)]
    priority_fields = {}
    for field_key, config_parts in config_dict.items():
        if 'priority' in config_parts:
            new_field_name = config_parts['priority'][0]
            if new_field_name in priority_fields:
                priority_fields[new_field_name].append( (field_key, config_parts['priority'][1]))
            else:
                priority_fields[new_field_name] = [ (field_key, config_parts['priority'][1]) ]

    # Choose Fields
    # first field in each set with a non-null value in the output_dict adds that value to the dict with it's priority_name
    for priority_name, priority_contents in priority_fields.items():
        sorted_contents = sorted(priority_contents, key=lambda x: x[1])
        # Ex. [('person_id_ssn', 1), ('person_id_other, 2)]

        found=False
        for value_field_pair in sorted_contents: 
            if value_field_pair[0] in output_dict and output_dict[value_field_pair[0]] is not None:
                output_dict[priority_name] = output_dict[value_field_pair[0]]
                pk_dict[priority_name].append(output_dict[value_field_pair[0]])
                found=True
                break

        if not found:
            # relent and put a None if we didn't find anything
            # unless we have a default value
            default_value = None
            if 'default' in config_dict:
                default_value = config_dict['default']
            output_dict[priority_name] = default_value
            pk_dict[priority_name].append(default_value)

    return priority_fields
    
    
@typechecked
def get_extract_order_fn(dict):
    def get_order_from_dict(field_key):
        if 'order' in dict[field_key]:
            logger.info(f"{field_key} {dict[field_key]['order']}")
            return int(dict[field_key]['order'])
        else:
            logger.info(f"extract_order_fn, no order in {field_key}")
            return int(sys.maxsize)

    return get_order_from_dict


@typechecked
def get_filter_fn(dict):
    def has_order_attribute(key):
        return 'order' in dict[key] and dict[key]['order'] is not None
    return has_order_attribute


@typechecked
def sort_output_and_omit_dict(output_dict :dict[str, None | str | float | int | int64], 
                     config_dict :dict[str, dict[str, str | None]], config_name):
    """ Sorts the ouput_dict by the value of the 'order' fields in the associated
        config_dict. Fields without a value, or without an entry used to 
        come last, now are omitted.
    """
    ordered_output_dict = {}

    sort_function = get_extract_order_fn(config_dict) # curry in the config_dict arg.
    ordered_keys = sorted(config_dict.keys(), key=sort_function)

    filter_function = get_filter_fn(config_dict)
    filtered_ordered_keys = filter(filter_function, ordered_keys)

    for key in filtered_ordered_keys:
        if key in output_dict:
            ordered_output_dict[key] = output_dict[key]

    return ordered_output_dict


@typechecked
def parse_config_for_single_root(root_element, root_path, config_name, 
                                 config_dict :dict[str, dict[str, str | None]], 
                                 error_fields_set : set[str], 
                                 pk_dict :dict[str, list[any]],
                                 filename :str) -> dict[str,  None | str | float | int | int64 |  datetime.datetime | datetime.date] | None:

    """  Parses for each field in the metadata for a config out of the root_element passed in.
         You may have more than one such root element, each making for a row in the output.

        If the configuration includes a field of config_type DOMAIN, the value it generates
        will be compared to the domain specified in the config in expected_domain_id. If they are different, null is returned.
        This is how  OMOP "domain routing" is implemented here. 


         Returns output_dict, a record, a single row for the domain involved.
    """
    output_dict = {} #  :dict[str, any]  a record, a single row for a given domain.
    domain_id = None
    logger.info((f"DDP.parse_config_for_single_root()  ROOT for config:{config_name}, we have tag:{root_element.tag}"
                 f" attributes:{root_element.attrib}"))

    do_none_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_constant_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_filename_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, filename)
    do_basic_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    do_derived_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    do_derived2_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_foreign_key_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)

    # NOTE: Order of operations is important here. do_priority_fields() must run BEFORE do_hash_fields().
    # Many hash fields (e.g., *_ids) depend on values that are resolved through priority logic.
    # This means that a priority chain should not include any hash fields.
    do_priority_fields(output_dict, root_element, root_path, config_name,  config_dict,
                                              error_fields_set, pk_dict)
    do_hash_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)

    logger.info((f"DDP.parse_config_for_single_root()  ROOT for config:{config_name}, "
                 f"we have tag:{root_element.tag}"
                 f" attributes:{root_element.attrib}"))

    if 'domain_id' not in output_dict:
        logger.error("'domain_id' mising from output dict when testing expected_domain_id. Check your "
            "parse configuration \"{config_name}\" for a field called 'domain_id'. If you don't have one, add it."
            "If you do, check the spelling. Your row will be REJECTED or DENY/DENIED.")        
    domain_id = output_dict.get('domain_id', None) # fetch this before it gets omitted
    output_dict = sort_output_and_omit_dict(output_dict, config_dict, config_name)
    expected_domain_id = config_dict['root']['expected_domain_id']

    # Strict: null domain_id is not good, but don't expect a domain id from non-domain tables
    if (expected_domain_id == domain_id
        or expected_domain_id in ['Person', 'Location', 'Care_Site', 'Provider', 'Visit']):
 
        if expected_domain_id == "Observation":
            logger.warning((f"ACCEPTING {domain_id} in config: {config_name} "
                            f"row id:{output_dict['observation_id']} "
                            f"concept code:{output_dict['observation_concept_id']}" ) )
        elif expected_domain_id == "Measurement":
            logger.warning((f"ACCEPTING {domain_id} in config: {config_name} "
                            f"row id:{output_dict['measurement_id']} "
                            f"concept code:{output_dict['measurement_concept_id']}") )
        elif expected_domain_id == "Procedure":
            logger.warning((f"ACCEPTING {domain_id} in config: {config_name} "
                            f"row id:{output_dict['procedure_occurrence_id']} "
                            f"concept code:{output_dict['procedure_concept_id']}") )
        elif expected_domain_id == "Device":
            logger.warning((f"ACCEPTING {domain_id} in config: {config_name} "
                            f"row id:{output_dict['device_exposure_id']} "
                            f"concept code:{output_dict['device_concept_id']}") )
        return output_dict
    else:
        if expected_domain_id == "Observation":
            logger.warning((f"DENYING/REJECTING have:{domain_id} domain:{expected_domain_id} in config: {config_name} "
                            f"row id:{output_dict['observation_id']} "
                            f"concept code:{output_dict['observation_concept_id']}" ))
        elif expected_domain_id == "Measurement":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} in config: {config_name} "
                              f"row id:{output_dict['measurement_id']} "
                              f"concept code:{output_dict['measurement_concept_id']}") )
        elif expected_domain_id == "Procedure":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} in config: {config_name} "
                              f"row id:{output_dict['procedure_occurrence_id']} "
                              f"concept code:{output_dict['procedure_concept_id']}") )
        elif expected_domain_id == "Drug":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} in config: {config_name} "
                              f"row id:{output_dict['drug_exposure_id']} "
                              f"concept code:{output_dict['drug_concept_id']}") )
        elif expected_domain_id == "Device":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} in config: {config_name} "
                              f"row id:{output_dict['device_exposure_id']} "
                              f"concept code:{output_dict['device_concept_id']}") )
        elif expected_domain_id == "Condition":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} in config: {config_name} "
                              f"row id:{output_dict['condition_occurrence_id']} "
                              f"concept code:{output_dict['condition_concept_id']}") )
        else:
            logger.warning((f"DENYING/REJECTING have:{domain_id} domain:{expected_domain_id} in config: {config_name} "))
        return None


def make_distinct(rows):
    """ rows is a list of records/dictionaries
        returns another such list, but uniqued
    """
    # make a key of each field, and add to a set
    seen_tuples = set()
    unique_rows = []
    for row in rows:
        row_tuple = tuple(sorted(row.items()))
        if row_tuple not in seen_tuples:
            seen_tuples.add(row_tuple)
            unique_rows.append(row)
    return unique_rows



@typechecked
def parse_config_from_xml_file(tree, config_name, 
                           config_dict :dict[str, dict[str, str | None]], filename, 
                           pk_dict :dict[str, list[any]]) -> list[ dict[str,  None | str | float | int | int64 | datetime.datetime | datetime.date] | None  ] | None:
                                                                   
    """ 
    Basically returns a list of rows for one domain that a parse configuration, config_name, creates.

        The main logic is here.
        Given a tree from ElementTree representing a CCDA document
        (ClinicalDocument, not just file),
        parse the different domains out of it (1 config each), linking PK and FKs between them.

        Returns a list, output_list, of dictionaries, output_dict, keyed by field name,
        containing a list of the value and the path to it:
            [ { field_1 : (value, path), field_2: (value, path)},
              { field_1: (value, path)}, {field_2: (value, path)} ]
        It's a list of because you might have more than one instance of the root path, like when you
        get many observations.
        
        arg: tree, this is the lxml.etree parse of the XML file
        arg: config_name, this is a key into the first level of the metadata, an often a OMOP domain name
        arg: config_dict, this is the value of that key in the dict
        arg: filename, the name of the XML file, for logging
        arg: pk_dict, a dictionary for Primary Keys, the keys here are field names and 
             their values are their values. It's a sort of global space for carrying PKs 
             to other parts of processing where they will be used as FKs. This is useful
             for things like the main person_id that is part of the context the document creates.


    """

    # log to a file per file/config
#    base_name = os.path.basename(filename)
#    logging.basicConfig(
#        format='%(levelname)s: %(message)s',
##        filename=f"logs/log_config_{base_name}_{config_name}.log",
#        #force=True, level=logging.WARNING)
#        force=True, level=logging.ERROR)

    # Find root
    if 'root' not in config_dict:
        logger.error(f"CONFIG {config_dict} lacks a root element.")
        return None

    if 'element' not in config_dict['root']:
        logger.error(f"CONFIG {config_dict} root lacks an 'element' key.")
        return None

    root_path = config_dict['root']['element']
    logger.info((f"CONFIG >>  config:{config_name} root:{config_dict['root']['element']}"
                 f"   ROOT path:{root_path}"))
    #root_element_list = tree.findall(config_dict['root']['element'], ns)
    root_element_list = None
    try:
        root_element_list = tree.xpath(config_dict['root']['element'], namespaces=ns)
    except Exception as e:
        logger.error(f" {config_dict['root']['element']}   {e}")
        
    if root_element_list is None or len(root_element_list) == 0:
        logger.info((f"CONFIG couldn't find root element for {config_name}"
                      f" with {config_dict['root']['element']}"))
        return None

    output_list = []
    error_fields_set = set()
    logger.info(f"NUM ROOTS {config_name} {len(root_element_list)}")
    for root_element in root_element_list:
        output_dict = parse_config_for_single_root(root_element, root_path, 
                config_name, config_dict, error_fields_set, pk_dict, filename)
        if output_dict is not None:
            output_list.append(output_dict)

    # report fields with errors
    if len(error_fields_set) > 0:
        print(f"DOMAIN Fields with errors in config {config_name} {error_fields_set}")
        logger.error(f"DOMAIN Fields with errors in config {config_name} {error_fields_set}")

    # distinct: gack, Pandas munges the types
    #output_list=pd.DataFrame(output_list).drop_duplicates().to_dict('records')
    output_list = make_distinct(output_list)

    return output_list



@typechecked
def parse_string(ccda_string, file_path,
              metadata :dict[str, dict[str, dict[str, str]]]) -> dict[str, 
                      list[ dict[str,  None | str | float | int | int64 ] | None  ] | None]:
    """ 
        Parses many meta configs from a string instead of a single file, 
        collects them in omop_dict.

        Returns omop_dict, a  dict keyed by configuration names, 
        each a list of record/row dictionaries.
    """
    omop_dict = {}
    pk_dict = defaultdict(list)
    tree = ET.fromstring(ccda_string)
    base_name = os.path.basename(file_path)
    for config_name, config_dict in metadata.items():
        data_dict_list = parse_config_from_xml_file(tree, config_name, config_dict, base_name, pk_dict)
        if data_dict_list is not None:
            logger.info(f"DDP.py {config_name} {len(data_dict_list)}")
        else:
            logger.info(f"DDP.py {config_name} has None data_dict_list")
        if config_name in omop_dict:
            omop_dict[config_name] = omop_dict[config_name].extend(data_dict_list)
        else:
            omop_dict[config_name] = data_dict_list

    for config_name, config_dict in omop_dict.items():
        if config_dict is not None:
            logger.info(f"DDP.py resulting omop_dict {config_name} {len(config_dict)}")
        else:
            logger.info(f"DDP.py resulting omop_dict {config_name} empty")

    if DO_VISIT_DETAIL:
        omop_dict = VR.reclassify_nested_visit_occurrences_as_detail(omop_dict)

    return omop_dict


@typechecked
def parse_doc(file_path, 
              metadata :dict[str, dict[str, dict[str, str]]]) -> dict[str, 
                      list[ dict[str,  None | str | float | int | int64] | None  ] | None]:
    """ Parses many meta configs from a single file, collects them in omop_dict.
        Returns omop_dict, a  dict keyed by configuration names, 
          each a list of record/row dictionaries.
    """
    omop_dict = {}
    pk_dict = defaultdict(list) 
    tree = ET.parse(file_path)
    base_name = os.path.basename(file_path)
    for config_name, config_dict in metadata.items():
#        print(f" {base_name} {config_name}")
        data_dict_list = parse_config_from_xml_file(tree, config_name, config_dict, base_name, pk_dict)
        if config_name in omop_dict: 
            omop_dict[config_name] = omop_dict[config_name].extend(data_dict_list)
        else:
            omop_dict[config_name] = data_dict_list
            

    # Try:
    if DO_VISIT_DETAIL:
        omop_dict = VR.reclassify_nested_visit_occurrences_as_detail(omop_dict)
    # No. Crash loudly and publicly if this threw an exception that is so mysterious
    # we catch with something as broad as Exception.
    # We are not operating on such a tight schedule that glossing over an unknown
    # here, and hoping the error is noticed in the logs.  There is no automated
    # process for scanning them. It's a human effort that never happens in
    # production.
    #except Exception as e:
    #    logger.error(f"Error processing visit hierarchy: {e}")
    #    logger.error(traceback.format_exc())
    #    # Continue with original data if hierarchy processing fails

    return omop_dict


@typechecked
def print_omop_structure(omop :dict[str, list[ dict[str, None | str | float | int | int64 ] ] ], 
                         metadata :dict[str, dict[str, dict[str, str ] ] ] ):
    
    """ prints a dict of parsed domains as returned from parse_doc()
        or parse_domain_from_dict()
    """
    for domain, domain_list in omop.items():
        if domain_list is None:
            logger.warning(f"no data for domain {domain}")
        else:
            for domain_data_dict in domain_list:
                n = 0
                if domain_data_dict is None:
                    print(f"\n\nERROR DOMAIN: {domain} is NONE")
                else:
                    print(f"\n\nDOMAIN: {domain} {domain_data_dict.keys()} ")
                    for field, parts in domain_data_dict.items():
                        print(f"    FIELD:{field}")
                        #print(f"        parts type {type(parts[0])}")
                        #print(f"        parts type {type(parts[1])}")
                        print(f"        parts type {type(parts)}")
                        print(f"        VALUE:{parts}")
                        #print(f"        VALUE:{parts[0]}")
                        #print(f"        PATH:{parts[1]}")
                        print(f"        ORDER: {metadata[domain][field]['order']}")
                        n = n+1
                    print(f"\n\nDOMAIN: {domain} {n}\n\n")

                    
@typechecked
def process_file(filepath :str, print_output: bool):
    """ Process each configuration in the metadata for one file.
        Returns nothing.
        Prints the omop_data. See better functions in layer_datasets.puy
    """
    print(f"PROCESSING {filepath} ")
    logger.info(f"PROCESSING {filepath} ")
#    base_name = os.path.basename(filepath)
#    logging.basicConfig(
#        format='%(levelname)s: %(message)s',
##        filename=f"logs/log_file_{base_name}.log",
##        force=True,
#        # level=logging.ERROR
#        level=logging.WARNING
#        # level=logging.INFO
#        # level=logging.DEBUG
#    )

    metadata = get_meta_dict()

    print(f"    {filepath} parse_doc() ")
    omop_data = parse_doc(filepath, metadata)
    print(f"    {filepath} reconcile_visit()() ")
    VR.assign_visit_occurrence_ids_to_events(omop_data)
    VR.assign_visit_detail_ids_to_events(omop_data)
    if print_output and (omop_data is not None or len(omop_data) < 1):
        print_omop_structure(omop_data, metadata)
    else:
        logger.error(f"FILE no data from {filepath} (or printing turned off)")

    print(f"done PROCESSING {filepath} ")


# for argparse
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main() :
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP Code Snooper',
        description="finds all code elements and shows what concepts the represent",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    parser.add_argument('-p', '--print_output', 
            type=str2bool, const=True, default=True,  nargs="?",
            help="print out the output values, -p False to have it not print")
    args = parser.parse_args()

    if args.filename is not None:
        process_file(args.filename, args.print_output)
    elif args.directory is not None:
        only_files = [f for f in os.listdir(args.directory) if os.path.isfile(os.path.join(args.directory, f))]
        for file in (only_files):
            if file.endswith(".xml"):
            	process_file(os.path.join(args.directory, file), args.print_output)
    else:
        logger.error("Did args parse let us  down? Have neither a file, nor a directory.")


if __name__ == '__main__':
    main()
