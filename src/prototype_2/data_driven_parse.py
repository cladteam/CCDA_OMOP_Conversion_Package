#!/usr/bin/env python3

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


logger = logging.getLogger(__name__)


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
            if field_tag in pk_dict and  len(pk_dict[field_tag]) > 0:
                if len(pk_dict[field_tag]) == 1:
                    output_dict[field_tag] = pk_dict[field_tag][0]
                else:
                    # can't really choose the correct value here. Is attempted in reconcile_visit_FK_with_specific_domain() later, below.
                    ###print(f"WARNING FK has more than one value {field_tag}, tagging with 'RECONCILE FK' ")
                    logger.info(f"WARNING FK has more than one value {field_tag}, tagging with 'RECONCILE FK'")
                    # original hack:
                    output_dict[field_tag] = -1  #### 'RECONCILE FK'
                
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
def do_derived_fields(output_dict :dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date], 
                      root_element, root_path, config_name,
                      config_dict :dict[str, dict[str, str | None]],
                      error_fields_set :set[str]):
    """ Do/compute derived values now that their inputs should be available in the output_dict
        Except for a special argument named 'default', when the value is what is other wise the field to look up in the output dict.
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
                    if field_name not in output_dict:
                        error_fields_set.add(field_tag)
                        logger.error((f"DERIVED config:{config_name} field:{field_tag} could not "
                                      f"find {field_name} in {output_dict}"))
                    try:
                        args_dict[arg_name] = output_dict[field_name]
                    except Exception as e:
                        #print(traceback.format_exc(e))
                        error_fields_set.add(field_tag)
                        logger.error((f"DERIVED {field_tag} arg_name: {arg_name} field_name:{field_name}"
                                      f" args_dict:{args_dict} output_dict:{output_dict}"))
                        logger.error(f"DERIVED exception {e}")

            try:
                function_reference = field_details_dict['FUNCTION']
                function_value = field_details_dict['FUNCTION'](args_dict)
#                if function_reference != VT.concat_fields and function_value is None:
#                    logger.error((f"do_derived_fields(): No mapping back for {config_name} {field_tag}"
 #                                 f" from {field_details_dict['FUNCTION']}  {args_dict}   {config_dict[field_tag]}  "
#                                  "If this is from a value_as_concept/code field, it may not be an error, but "
 #                                 "an artificat of data that doesn't have a value or one that is not "
#                                  "meant as a concept id"))
                output_dict[field_tag] = function_value
                logger.info((f"     DERIVED {function_value} for "
                                f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))
            except KeyError as e:
                #print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED key error on: {e}")
                logger.error(f"DERIVED KeyError {field_tag} function can't find key it expects in {args_dict}")
                output_dict[field_tag] = None
            except TypeError as e:
                #print(traceback.format_exc(e))
                error_fields_set.add(field_tag)
                logger.error(f"DERIVED type error exception: {e}")
                logger.error((f"DERIVED TypeError {field_tag} possibly calling something that isn't a function"
                              " or that function was passed a null value." 
                              f" {field_details_dict['FUNCTION']}. You may have quotes "
                              "around it in  a python mapping structure if this is a "
                              f"string: {type(field_details_dict['FUNCTION'])}"))
                output_dict[field_tag] = None
            except Exception as e:
                logger.error(f"DERIVED exception: {e}")
                output_dict[field_tag] = None
            except: # Error as er:
#                logger.error(f"DERIVED error: {er}")
                output_dict[field_tag] = None

                
@typechecked
def do_hash_fields(output_dict :dict[str, None | str | float | int | int32 | int64 | datetime.datetime | datetime.date], 
                   root_element, root_path, config_name,  
                   config_dict :dict[str, dict[str, str | None]], 
                   error_fields_set :set[str], 
                   pk_dict :dict[str, list[any]]):
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
            hash_input =  "|".join(map(str, value_list))
            hash_value = create_hash(hash_input)
            output_dict[field_tag] = hash_value
            # treat as PK and include in that dictionary
            pk_dict[field_tag].append(hash_value)
            logger.info((f"     HASH (PK) {hash_value} for "
                         f"{field_tag}, {field_details_dict} {output_dict[field_tag]}"))

            
@typechecked
def do_priority_fields(output_dict :dict[str, None | str | float | int | int32 | int64 |  datetime.datetime | datetime.date], 
                       root_element, root_path, config,  
                       config_dict :dict[str, dict[str, str | None]], 
                       error_fields_set :set[str], 
                       pk_dict :dict[str, list[any]]) -> dict[str, list]:
    """
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
        if  'priority' in config_parts:
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
            output_dict[priority_name] = None
            pk_dict[priority_name].append(None)

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
        will be compared to the domain specified in the config in. If they are different, null is returned.
        This is how  OMOP "domain routing" is implemented here.


         Returns output_dict, a record
    """
    output_dict = {} #  :dict[str, any]  a record
    domain_id = None
    logger.info((f"DDP.parse_config_for_single_root()  ROOT for config:{config_name}, we have tag:{root_element.tag}"
                 f" attributes:{root_element.attrib}"))

    do_none_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_constant_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
    do_filename_fields(output_dict, root_element, root_path, config_name, config_dict, error_fields_set, filename)
    do_basic_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set, pk_dict)
    do_derived_fields(output_dict, root_element, root_path, config_name,  config_dict, error_fields_set)
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

    domain_id = output_dict.get('domain_id', None) # fetch this before it gets omitted
    output_dict = sort_output_and_omit_dict(output_dict, config_dict, config_name)
    expected_domain_id = config_dict['root']['expected_domain_id']

    # Strict: null domain_id is not good, but don't expect a domain id from non-domain tables
    if (expected_domain_id == domain_id
        or expected_domain_id in ['Person', 'Location', 'Care_Site', 'Provider', 'Visit']):

        if expected_domain_id == "Observation":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['observation_id']} "
                            f"cpt:{output_dict['observation_concept_id']}" ) )
        elif expected_domain_id == "Measurement":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['measurement_id']} "
                            f"cpt:{output_dict['measurement_concept_id']}") )
        elif expected_domain_id == "Procedure":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['procedure_occurrence_id']} "
                            f"cpt:{output_dict['procedure_concept_id']}") )
        elif expected_domain_id == "Device":
            logger.warning((f"ACCEPTING {domain_id} "
                            f"id:{output_dict['device_exposure_id']} "
                            f"cpt:{output_dict['device_concept_id']}") )
        return output_dict
    else:
        if expected_domain_id == "Observation":
            logger.warning((f"DENYING/REJECTING have:{domain_id} domain:{expected_domain_id} "
                            f"id:{output_dict['observation_id']} "
                            f"cpt:{output_dict['observation_concept_id']}" ))
        elif expected_domain_id == "Measurement":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['measurement_id']} "
                              f"cpt:{output_dict['measurement_concept_id']}") )
        elif expected_domain_id == "Procedure":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['procedure_occurrence_id']} "
                              f"cpt:{output_dict['procedure_concept_id']}") )
        elif expected_domain_id == "Drug":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['drug_exposure_id']} "
                              f"cpt:{output_dict['drug_concept_id']}") )
        elif expected_domain_id == "Device":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['device_exposure_id']} "
                              f"cpt:{output_dict['device_concept_id']}") )
        elif expected_domain_id == "Condition":
            logger.warning( ( f"DENYING/REJECTING have:{domain_id} expect:{expected_domain_id} "
                              f"id:{output_dict['condition_occurrence_id']} "
                              f"cpt:{output_dict['condition_concept_id']}") )
        else:
            logger.warning((f"DENYING/REJECTING have:{domain_id} domain:{expected_domain_id} "))
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
                                                                   
    """ The main logic is here.
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



#
##################################################
#


"""

    The following part processes visit data to create a hierarchical structure
    where inpatient parent visits (<= 1 year duration) are kept in visit_occurrence
    and their nested child visits are moved to visit_detail.

    Process:
    1. Identify inpatient parent visits with duration <= 1 year
    2. Find visits temporally nested within each parent
    3. Create visit_detail records for nested children
    4. Return updated visit_occurrence (parents + standalone) and new visit_detail list

"""

# Type alias for OMOP record dictionaries
OMOPRecord = dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date]

# OMOP standard concept IDs for inpatient visits
INPATIENT_CONCEPT_IDS = {
    9201   # Inpatient Visit
}

# Maximum duration for a valid inpatient parent (in days)
MAX_PARENT_DURATION_DAYS = 367


def get_visit_duration_days(visit_dict: OMOPRecord) -> float | None:
    """
    Calculate visit duration in days.

    Args:
        visit_dict: Dictionary containing visit record

    Returns:
        Duration in days, or None if dates are missing
    """
    # Try datetime columns first, fall back to date columns
    if 'visit_start_datetime' in visit_dict and 'visit_end_datetime' in visit_dict:
        start_key = 'visit_start_datetime'
        end_key = 'visit_end_datetime'
    else:
        start_key = 'visit_start_date'
        end_key = 'visit_end_date'

    start = visit_dict.get(start_key)
    end = visit_dict.get(end_key)

    if start is None or end is None:
        return None

    # Handle both datetime and date objects
    if isinstance(start, datetime.datetime) and isinstance(end, datetime.datetime):
        return (end - start).total_seconds() / 86400
    elif isinstance(start, datetime.date) and isinstance(end, datetime.date):
        return float((end - start).days)
    else:
        return None


def identify_inpatient_parents(visit_list: list[OMOPRecord]) -> list[OMOPRecord]:
    """
    Identify inpatient parent visits that are meaningful and time-bounded.

    Criteria:
    - visit_concept_id is in INPATIENT_CONCEPT_IDS
    - Duration between start and end is <= 1 year
    - Has valid start and end datetimes

    Args:
        visit_list: List of visit record dictionaries

    Returns:
        List containing only eligible inpatient parent visits
    """
    if not visit_list:
        logger.info("No visits to process for parent identification")
        return []

    eligible_parents = []

    for visit in visit_list:
        # Check if inpatient concept
        concept_id = visit.get('visit_concept_id')
        if concept_id in INPATIENT_CONCEPT_IDS:
            # Calculate duration
            duration_days = get_visit_duration_days(visit)
            if duration_days is not None:
                # Check duration threshold
                if duration_days < MAX_PARENT_DURATION_DAYS:
                    eligible_parents.append(visit)

    logger.info(f"Identified {len(eligible_parents)} inpatient parent visits from {len(visit_list)} total visits")

    return eligible_parents


def is_temporally_contained(child_dict: OMOPRecord, parent_dict: OMOPRecord) -> bool:
    """
    Check if child visit is temporally contained within parent visit.

    Args:
        child_dict: Child visit record
        parent_dict: Parent visit record

    Returns:
        True if child is fully contained within parent timeframe
    """
    # Determine which date columns to use
    if 'visit_start_datetime' in child_dict and 'visit_end_datetime' in child_dict:
        start_key = 'visit_start_datetime'
        end_key = 'visit_end_datetime'
    else:
        start_key = 'visit_start_date'
        end_key = 'visit_end_date'

    child_start = child_dict.get(start_key)
    child_end = child_dict.get(end_key)
    parent_start = parent_dict.get(start_key)
    parent_end = parent_dict.get(end_key)

    if any(x is None for x in [child_start, child_end, parent_start, parent_end]):
        return False

    # Check temporal containment
    return parent_start <= child_start and parent_end >= child_end


def find_most_specific_parent(child_dict: OMOPRecord,
                              potential_parents: list[OMOPRecord]) -> int64 | None:
    """
    Find the most specific (shortest duration, most immediate) parent for a child visit.

    When multiple parents overlap and contain a child, choose the parent with the
    shortest duration as it represents the most specific/immediate context.

    However, if the child has multiple containing parents that are at the same
    hierarchy level (i.e., the parents don't contain each other), returns None
    to avoid ambiguity. The child visit will remain at its current level.

    Args:
        child_dict: Child visit record
        potential_parents: List of potential parent visit records

    Returns:
        visit_occurrence_id of the most specific parent, or None if:
        - No parent found
        - Multiple parents at the same hierarchy level exist (ambiguous)
    """
    if not potential_parents:
        return None

    child_person_id = child_dict.get('person_id')
    child_visit_id = child_dict.get('visit_occurrence_id')

    # Filter to parents that contain this child
    containing_parents = []
    for parent in potential_parents:
        # Same person
        if parent.get('person_id') == child_person_id:
            # Not self
            if parent.get('visit_occurrence_id') != child_visit_id:
                # Temporally contains child
                if is_temporally_contained(child_dict, parent):
                    containing_parents.append(parent)

    if not containing_parents:
        return None

    # Check if any of the containing parents are at the same hierarchy level
    # (i.e., they don't contain each other)
    if len(containing_parents) > 1:
        for i in range(len(containing_parents)):
            for j in range(i + 1, len(containing_parents)):
                parent_i = containing_parents[i]
                parent_j = containing_parents[j]

                # Check if parent_i contains parent_j
                i_contains_j = is_temporally_contained(parent_j, parent_i)

                # Check if parent_j contains parent_i
                j_contains_i = is_temporally_contained(parent_i, parent_j)

                # If neither contains the other, they're at the same level (siblings)
                if not i_contains_j and not j_contains_i:
                    parent_i_id = parent_i.get('visit_occurrence_id')
                    parent_j_id = parent_j.get('visit_occurrence_id')
                    logger.warning(
                        f"Visit {child_visit_id} has multiple parents at the same hierarchy level "
                        f"(parents {parent_i_id} and {parent_j_id} don't contain each other). "
                        f"Keeping in current level to avoid ambiguity."
                    )
                    return None

    # All parents are in a hierarchical chain - find the most specific (shortest duration)
    min_duration = None
    most_specific_parent = None

    for parent in containing_parents:
        duration = get_visit_duration_days(parent)
        if duration is not None:
            if min_duration is None or duration < min_duration:
                min_duration = duration
                most_specific_parent = parent

    if most_specific_parent:
        parent_id = most_specific_parent.get('visit_occurrence_id')
        return parent_id

    return None


def create_visit_detail_record(visit_dict: OMOPRecord,
                               top_level_parent_id: int64,
                               immediate_parent_id: int64 | None = None) -> OMOPRecord:
    """
    Convert a visit (visit_occurrence) record into visit_detail format.

    Args:
        visit_dict: Visit record to convert
        top_level_parent_id: The top-level visit_occurrence_id
        immediate_parent_id: The immediate parent's visit_detail_id (or None for Layer 2)

    Returns:
        Dictionary in visit_detail format
    """
    detail_record = {}

    # Map visit_occurrence fields to visit_detail fields
    field_mapping = {
        'visit_occurrence_id': 'visit_detail_id',
        'person_id': 'person_id',
        'visit_concept_id': 'visit_detail_concept_id',
        'visit_start_date': 'visit_detail_start_date',
        'visit_start_datetime': 'visit_detail_start_datetime',
        'visit_end_date': 'visit_detail_end_date',
        'visit_end_datetime': 'visit_detail_end_datetime',
        'visit_type_concept_id': 'visit_detail_type_concept_id',
        'provider_id': 'provider_id',
        'care_site_id': 'care_site_id',
        'visit_source_value': 'visit_detail_source_value',
        'visit_source_concept_id': 'visit_detail_source_concept_id',
        'admitting_source_value': 'admitting_source_value',
        'admitting_source_concept_id': 'admitting_source_concept_id',
        'discharge_to_source_value': 'discharge_to_source_value',
        'discharge_to_concept_id': 'discharge_to_concept_id',
        'filename': 'filename',
        'cfg_name': 'cfg_name',
    }

    # Copy mapped fields
    for src_field, dest_field in field_mapping.items():
        if src_field in visit_dict:
            detail_record[dest_field] = visit_dict[src_field]

    # Set parent references
    detail_record['visit_occurrence_id'] = top_level_parent_id
    detail_record['visit_detail_parent_id'] = immediate_parent_id
    detail_record['preceding_visit_detail_id'] = None

    return detail_record


def reclassify_nested_visit_occurrences_as_detail(omop_dict: dict[str, list[OMOPRecord]]) -> dict[str, list[OMOPRecord]]:
    """
    Reclassify nested visit_occurrence records as visit_detail records.
    This function is called after all parsing is complete.

    Process:
    1. Identify inpatient parent visits (<= 1 year duration)
    2. For each visit, find its most specific parent
    3. Only top-level parents (no parent themselves) stay in visit_occurrence
    4. All nested visits go to visit_detail with visit_detail_parent_id for multi-level nesting

    Args:
        omop_dict: Dictionary of domain → list of records (from parse_doc)

    Returns:
        Updated omop_dict with:
        - Modified visit (visit_occurrence) list (parents + standalone)
        - New visit_detail list (nested children)
    """
    # Find Visit in omop_dict
    visit_key = None
    if 'Visit' in omop_dict and omop_dict['Visit']:
        visit_key = 'Visit'

    if not visit_key:
        logger.info("No visit_occurrence data found for hierarchy processing")
        return omop_dict

    visit_list = omop_dict[visit_key]
    if not visit_list:
        logger.info("Empty visit_occurrence list")
        return omop_dict

    logger.info(f"Processing visit hierarchy for {len(visit_list)} visits")

    # Step 1: Identify inpatient parents
    parent_visits = identify_inpatient_parents(visit_list)

    if not parent_visits:
        logger.info("No inpatient parent visits found - returning original visit_occurrence")
        return omop_dict

    logger.info(f"Identified {len(parent_visits)} potential parent visits")

    # Step 2: For each visit, determine if it should be nested and find its most specific parent
    visit_to_parent_map = {}
    nested_visit_ids = set()

    # Create lookup dict for faster access
    visit_lookup = {v.get('visit_occurrence_id'): v for v in visit_list}

    for visit in visit_list:
        visit_id = visit.get('visit_occurrence_id')

        # Find the most specific parent for this visit
        most_specific_parent_id = find_most_specific_parent(visit, parent_visits)

        if most_specific_parent_id is not None:
            # This visit has a parent, so it should be nested
            visit_to_parent_map[visit_id] = most_specific_parent_id
            nested_visit_ids.add(visit_id)
            logger.debug(f"Visit {visit_id} will be nested under parent {most_specific_parent_id}")

    logger.info(f"Found {len(nested_visit_ids)} visits to be nested")

    # Step 3: Identify which parent visits are themselves nested (multi-level scenario)
    parent_ids = {p.get('visit_occurrence_id') for p in parent_visits}
    nested_parent_ids = parent_ids & nested_visit_ids

    logger.info(f"Found {len(parent_ids - nested_parent_ids)} top-level parents and {len(nested_parent_ids)} nested parents")

    # Step 4: Create visit_detail records for all nested visits
    visit_detail_list = []

    for visit_id in nested_visit_ids:
        visit = visit_lookup.get(visit_id)
        if visit:
            immediate_parent_id = visit_to_parent_map[visit_id]

            # Find the top-level visit_occurrence_id by traversing up the hierarchy
            top_level_parent_id = immediate_parent_id
            while top_level_parent_id in visit_to_parent_map:
                top_level_parent_id = visit_to_parent_map[top_level_parent_id]

            # Determine visit_detail_parent_id
            if immediate_parent_id in nested_parent_ids:
                # Immediate parent is in visit_detail
                visit_detail_parent_id = immediate_parent_id
            else:
                # Immediate parent is in visit_occurrence (top-level)
                visit_detail_parent_id = None

            # Create visit_detail record
            detail_record = create_visit_detail_record(visit, top_level_parent_id, visit_detail_parent_id)
            visit_detail_list.append(detail_record)

    logger.info(f"Created {len(visit_detail_list)} visit_detail records")

    # Step 5: Create final visit_occurrence (remove ALL nested children, keep only top-level)
    final_visit_occurrence = [v for v in visit_list if v.get('visit_occurrence_id') not in nested_visit_ids]

    logger.info(f"Removed {len(nested_visit_ids)} nested visits from visit_occurrence")
    logger.info(f"Final visit_occurrence contains {len(final_visit_occurrence)} records")

    # Update omop_dict
    omop_dict[visit_key] = final_visit_occurrence
    if visit_detail_list:
        omop_dict['Visit_detail'] = visit_detail_list

    return omop_dict


""" domain_dates tell the FK functionality in do_foreign_keys() how to 
    choose visits for domain_rows.It is one of the most encumbered parts of the code.

    Rules:
    - Encounters must be populated before domains. This is controlled by the
      order of the metadata files in the metadata/__init__.py file.
    - This structure must include a mapping from start or start and end to
      names of the fields for each specific domain to be processed.
    - These are _config_ names, not domain names. For example, the domain
      Measurement is fed by configs names Measurement_vital_signs, and 
      Measurement_results. They are the keys into the output dict where the
      visit candidates will be found.
    + This all happens in the do_basic_keys 

    Background: An xml file is processed in phases, one for each configuration file in 
    the metadata directory. Since the configuration files are organized by omop table,
    it's helpful to think of the phases being the OMOP tables too.  Within each config 
    phase, there is another level of phases: the types of the fields: none, constant, 
    basic, derived, domain, hash, and foreign key. This means any fields in the current 
    config phase are available for looking up the value of a foreign key.

"""
domain_dates = {
    'Measurement': {'date': ['measurement_date', 'measurement_datetime'],
                    'id': 'measurement_id'},
    'Observation': {'date': ['observation_date', 'observation_datetime'],
                    'id': 'observation_id'},
    'Condition'  : {'start': ['condition_start_date', 'condition_start_datetime'], 
                    'end':   ['condition_end_date', 'condition_end_datetime'],
                    'id': 'condition_id'},
    'Procedure'  : {'date': ['procedure_date', 'procedure_datetime'],
                    'id': 'procedure_occurrence_id'},
    'Drug'       : {'start': ['drug_exposure_start_date', 'drug_exposure_start_datetime'],
                    'end': ['drug_exposure_end_date', 'drug_exposure_end_datetime'],
                    'id': 'drug_exposure_id'},
    'Device'     : {'start': ['device_exposure_start_date', 'device_exposure_start_datetime'],
                    'end': ['device_exposure_end_date', 'device_exposure_end_datetime'],
                    'id': 'device_exposure_id'},
}

@typechecked 
def strip_tz(dt): # Strip timezone
    if isinstance(dt, datetime.datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

@typechecked 
def reconcile_visit_FK_with_specific_domain(domain: str, 
                                            domain_dict: list[dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date] ] | None , 
                                            visit_dict:  list[dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date] ] | None):
    if visit_dict is None:
        logger.error(f"no visits for {domain} in reconcile_visit_FK_with_specific_domain, reconcilliation")
        return

    if domain_dict is None:
        logger.error(f"no data for {domain} in reconcile_visit_FK_with_specific_domain, reconcilliation")
        return
    
    # Only Measurement, Observation, Condition, Procedure, Drug, and Device participate in Visit FK reconciliation
    if domain not in domain_dates:
        logger.error(f"no metadata for domain {domain} in reconcile_visit_FK_with_specific_domain, reconcilliation")
        return

    if 'date' in domain_dates[domain].keys():
        # Logic for domains with just one date
        for thing in domain_dict:

            date_field_name = domain_dates[domain]['date'][0]
            datetime_field_name = domain_dates[domain]['date'][1]

            # Start with the plain date. If a datetime value is present, prefer it (more specific)
            date_field_value = thing[date_field_name]
            if thing[datetime_field_name] is not None and isinstance(thing[datetime_field_name], datetime.datetime):
                date_field_value = strip_tz(thing[datetime_field_name])

            if date_field_value is not None:
                matches = []

                for visit in visit_dict:
                    try:
                        start_visit_date = visit['visit_start_date']
                        start_visit_datetime = strip_tz(visit['visit_start_datetime'])
                        end_visit_date = visit['visit_end_date']
                        end_visit_datetime = strip_tz(visit['visit_end_datetime'])

                        in_window = False
                        # Match using datetime
                        if isinstance(date_field_value, datetime.datetime):
                            if start_visit_datetime != end_visit_datetime:
                                in_window = start_visit_datetime <= date_field_value <= end_visit_datetime
                            else:
                                end_visit_datetime_adjusted = datetime.datetime.combine(end_visit_date,
                                                                                        datetime.time(23, 59, 59))
                                in_window = start_visit_datetime <= date_field_value <= end_visit_datetime_adjusted

                        # Match using only dates
                        elif isinstance(date_field_value, datetime.date):
                            in_window = start_visit_date <= date_field_value <= end_visit_date

                        if in_window:
                            matches.append(visit['visit_occurrence_id'])

                    except KeyError as ke:
                        logger.error(f"missing field  \"{ke}\", in visit reconcilliation, see warnings for detail")
                        logger.warning(f"missing field  \"{ke}\", in visit reconcilliation, got error {type(ke)} ")
                    except Exception as e:
                        pass

                if len(matches) == 1:
                    thing['visit_occurrence_id'] = matches[0]
                elif len(matches) == 0:
                    logger.error(f" couldn't reconcile visit for {domain} event: {thing}")
                else:
                    logger.warning(
                        "Ambiguous visit match for %s (id=%s): %d candidates; leaving visit_occurrence_id unset",
                        domain, thing.get(domain_dates[domain]['id']), len(matches)
                    )
                    thing['__visit_candidates'] = matches

            else:
                # S.O.L.
                logger.error(f"no date available for visit reconcilliation in domain {domain} for {thing}")

    # Logic for domains with start and end date/dateime
    elif 'start' in domain_dates[domain].keys() and 'end' in domain_dates[domain].keys():
        for thing in domain_dict:
            start_date_field_name = domain_dates[domain]['start'][0]
            start_datetime_field_name = domain_dates[domain]['start'][1]
            end_date_field_name = domain_dates[domain]['end'][0]
            end_datetime_field_name = domain_dates[domain]['end'][1]

            start_date_value = None
            end_date_value = None

            # Prefer datetime if available
            if thing[start_datetime_field_name] is not None and isinstance(thing[start_datetime_field_name],
                                                                           datetime.datetime):
                start_date_value = strip_tz(thing[start_datetime_field_name])
            else:
                start_date_value = thing[start_date_field_name]

            # Prefer datetime if available, else use end_date field, else fallback to start_date
            if thing[end_datetime_field_name] is not None and isinstance(thing[end_datetime_field_name],
                                                                         datetime.datetime):
                end_date_value = strip_tz(thing[end_datetime_field_name])
            elif thing[end_date_field_name] is not None:
                end_date_value = thing[end_date_field_name]
            else:
                end_date_value = start_date_value

            if start_date_value is not None and end_date_value is not None:
                matches = []

                for visit in visit_dict:
                    try:
                        start_visit_date = visit['visit_start_date']
                        start_visit_datetime = strip_tz(visit['visit_start_datetime'])
                        end_visit_date = visit['visit_end_date']
                        end_visit_datetime = strip_tz(visit['visit_end_datetime'])

                        in_window = False
                        # Adjust datetime comparisons for start and end values
                        if isinstance(start_date_value, datetime.datetime) and isinstance(end_date_value,
                                                                                          datetime.datetime):
                            if start_visit_datetime != end_visit_datetime:
                                in_window = (
                                        (start_visit_datetime <= start_date_value <= end_visit_datetime) and
                                        (start_visit_datetime <= end_date_value <= end_visit_datetime)
                                )
                            else:
                                end_visit_datetime_adjusted = datetime.datetime.combine(end_visit_date,
                                                                                        datetime.time(23, 59, 59))
                                in_window = (
                                        (start_visit_datetime <= start_date_value <= end_visit_datetime_adjusted) and
                                        (start_visit_datetime <= end_date_value <= end_visit_datetime_adjusted)
                                )
                        # Compare with dates if datetime is not available
                        elif isinstance(start_date_value, datetime.date) and isinstance(end_date_value, datetime.date):
                            in_window = (
                                    (start_visit_date <= start_date_value <= end_visit_date) and
                                    (start_visit_date <= end_date_value <= end_visit_date)
                            )

                        if in_window:
                            matches.append(visit['visit_occurrence_id'])

                    except KeyError as ke:
                        print(f"WARNING missing field  \"{ke}\", in visit reconcilliation, got error {type(ke)} ")
                    except Exception as e:
                        print(f"WARNING something wrong in visit reconciliation: {e}")

                if len(matches) == 1:
                    thing['visit_occurrence_id'] = matches[0]
                elif len(matches) == 0:
                    logger.error(f" couldn't reconcile visit for {domain} event: {thing}")
                else:
                    logger.warning(
                        "Ambiguous visit match for %s (id=%s): %d candidates; leaving visit_occurrence_id unset",
                        domain, thing.get(domain_dates[domain]['id']), len(matches)
                    )
                    thing['__visit_candidates'] = matches

            else:
                # S.O.L.
                print(f"ERROR no date available for visit reconcilliation in domain {domain} (detail in logs)")
                logger.error(f" no date available for visit reconcilliation in domain {domain} for {thing}")

    else:
        logger.error("??? bust in domain_dates for reconcilliation")


@typechecked
def assign_visit_occurrence_ids_to_events(data_dict :dict[str, 
                                                 list[ dict[str,  None | str | float | int |int64 | datetime.datetime | datetime.date] | None  ] | None]) :
    # data_dict is a dictionary of config_names to a list of record-dicts
    # Only Measurement, Observation, Condition, Procedure, Drug, and Device participate in Visit FK reconciliation
    metadata = [
        ('Measurement', 'Measurement_results'),
        ('Measurement', 'Measurement_vital_signs'),
        ('Observation', 'Observation'),
        ('Condition',   'Condition'),
        ('Procedure',   'Procedure_activity_procedure'),
        ('Procedure',   'Procedure_activity_observation'),
        ('Procedure',   'Procedure_activity_act'),
        ('Drug',        'Medication_medication_activity'),
        ('Drug',        'Medication_medication_dispense'),
        ('Drug',        'Immunization_immunization_activity'),
        ('Device',      'Device_organizer_supply'),
        ('Device',      'Device_supply'),
        ('Device',      'Device_organizer_procedure'),
        ('Device',      'Device_procedure'),
    ]

    for meta_tuple in metadata:
        reconcile_visit_FK_with_specific_domain(meta_tuple[0], data_dict[meta_tuple[1]], data_dict['Visit'])


@typechecked
def assign_visit_detail_ids_to_events(data_dict: dict[str,
                                                         list[dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date] | None] | None]):
    """
    visit_detail FK reconciliation: Match clinical domain events to visit_detail records.

    This assigns visit_detail_id to events that occur during nested visits.
    Events must already have visit_occurrence_id set (from assign_visit_occurrence_ids_to_events).

    For events that fall within multiple nested visits, chooses the most specific
    (smallest duration) visit_detail.

    Args:
        data_dict: Dictionary with domain data and visit_detail table
    """
    if 'Visit_detail' not in data_dict or not data_dict['Visit_detail']:
        logger.info("No visit_detail records found - skipping visit_detail FK reconciliation")
        return

    visit_detail_list = data_dict['Visit_detail']

    # Metadata for domains that need visit_detail reconciliation
    metadata = [
        ('Measurement', 'Measurement_results'),
        ('Measurement', 'Measurement_vital_signs'),
        ('Observation', 'Observation'),
        ('Condition', 'Condition'),
        ('Procedure', 'Procedure_activity_procedure'),
        ('Procedure', 'Procedure_activity_observation'),
        ('Procedure', 'Procedure_activity_act'),
        ('Drug', 'Medication_medication_activity'),
        ('Drug', 'Medication_medication_dispense'),
        ('Drug', 'Immunization_immunization_activity'),
        ('Device', 'Device_organizer_supply'),
        ('Device', 'Device_supply'),
        ('Device', 'Device_organizer_procedure'),
        ('Device', 'Device_procedure'),
    ]

    for meta_tuple in metadata:
        domain = meta_tuple[0]
        config_name = meta_tuple[1]

        if config_name in data_dict and data_dict[config_name]:
            reconcile_visit_detail_FK_with_specific_domain(domain, data_dict[config_name], visit_detail_list)


@typechecked
def reconcile_visit_detail_FK_with_specific_domain(domain: str,
                                                    domain_dict: list[dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date]] | None,
                                                    visit_detail_dict: list[dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date]] | None):
    """
    Match events to visit_detail records by temporal containment.
    Choose the most specific (smallest duration) matching visit_detail.

    Args:
        domain: Domain name (e.g., 'Measurement', 'Condition')
        domain_dict: List of event records to reconcile
        visit_detail_dict: List of visit_detail records
    """
    if not visit_detail_dict or not domain_dict:
        return

    if domain not in domain_dates:
        logger.debug(f"No date metadata for domain {domain} in visit_detail reconciliation")
        return

    logger.info(f"Reconciling visit_detail FKs for {domain} ({len(domain_dict)} events, {len(visit_detail_dict)} visit_details)")

    # Visit_detail date field mapping
    visit_detail_date_fields = {
        'start_date': 'visit_detail_start_date',
        'start_datetime': 'visit_detail_start_datetime',
        'end_date': 'visit_detail_end_date',
        'end_datetime': 'visit_detail_end_datetime',
    }

    matched_count = 0
    no_match_count = 0

    # Process events with a single date field
    if 'date' in domain_dates[domain].keys():
        for thing in domain_dict:
            # Skip if no visit_occurrence_id
            if 'visit_occurrence_id' not in thing or thing['visit_occurrence_id'] is None:
                continue

            date_field_name = domain_dates[domain]['date'][0]
            datetime_field_name = domain_dates[domain]['date'][1]

            # Get event date (prefer datetime over date)
            event_date = None
            if thing[datetime_field_name] is not None and isinstance(thing[datetime_field_name], datetime.datetime):
                event_date = strip_tz(thing[datetime_field_name])
            else:
                event_date = thing[date_field_name]

            if event_date is None:
                continue

            # Find matching visit_details
            matches = []
            for vd in visit_detail_dict:
                # Must be in the same visit_occurrence
                if vd.get('visit_occurrence_id') != thing.get('visit_occurrence_id'):
                    continue

                # Get visit_detail dates
                vd_start_datetime = strip_tz(vd.get(visit_detail_date_fields['start_datetime']))
                vd_start_date = vd.get(visit_detail_date_fields['start_date'])
                vd_end_datetime = strip_tz(vd.get(visit_detail_date_fields['end_datetime']))
                vd_end_date = vd.get(visit_detail_date_fields['end_date'])

                # Check containment
                in_window = False
                if isinstance(event_date, datetime.datetime):
                    if vd_start_datetime and vd_end_datetime:
                        in_window = vd_start_datetime <= event_date <= vd_end_datetime
                elif isinstance(event_date, datetime.date):
                    if vd_start_date and vd_end_date:
                        in_window = vd_start_date <= event_date <= vd_end_date

                if in_window:
                    matches.append(vd)

            # Set visit_detail_id based on matches
            if len(matches) == 1:
                thing['visit_detail_id'] = matches[0]['visit_detail_id']
                matched_count += 1
            elif len(matches) > 1:
                # Multiple matches - choose most specific (smallest duration)
                most_specific = min(matches, key=lambda vd: get_visit_detail_duration(vd))
                thing['visit_detail_id'] = most_specific['visit_detail_id']
                matched_count += 1
                logger.debug(f"{domain} event matched {len(matches)} visit_details, chose most specific (id={most_specific['visit_detail_id']})")
            else:
                # No match - leave visit_detail_id as None
                no_match_count += 1

    # Process events with start and end dates
    elif 'start' in domain_dates[domain].keys() and 'end' in domain_dates[domain].keys():
        for thing in domain_dict:
            # Skip if no visit_occurrence_id
            if 'visit_occurrence_id' not in thing or thing['visit_occurrence_id'] is None:
                continue

            start_date_field = domain_dates[domain]['start'][0]
            start_datetime_field = domain_dates[domain]['start'][1]
            end_date_field = domain_dates[domain]['end'][0]
            end_datetime_field = domain_dates[domain]['end'][1]

            # Get event dates
            start_date_value = None
            end_date_value = None

            if thing[start_datetime_field] is not None and isinstance(thing[start_datetime_field], datetime.datetime):
                start_date_value = strip_tz(thing[start_datetime_field])
            else:
                start_date_value = thing[start_date_field]

            if thing[end_datetime_field] is not None and isinstance(thing[end_datetime_field], datetime.datetime):
                end_date_value = strip_tz(thing[end_datetime_field])
            elif thing[end_date_field] is not None:
                end_date_value = thing[end_date_field]
            else:
                end_date_value = start_date_value

            if start_date_value is None or end_date_value is None:
                continue

            # Find matching visit_details
            matches = []
            for vd in visit_detail_dict:
                # Must be in the same visit_occurrence
                if vd.get('visit_occurrence_id') != thing.get('visit_occurrence_id'):
                    continue

                # Get visit_detail dates
                vd_start_datetime = strip_tz(vd.get(visit_detail_date_fields['start_datetime']))
                vd_start_date = vd.get(visit_detail_date_fields['start_date'])
                vd_end_datetime = strip_tz(vd.get(visit_detail_date_fields['end_datetime']))
                vd_end_date = vd.get(visit_detail_date_fields['end_date'])

                # Check containment (both start and end must be within visit_detail window)
                in_window = False
                if isinstance(start_date_value, datetime.datetime) and isinstance(end_date_value, datetime.datetime):
                    if vd_start_datetime and vd_end_datetime:
                        in_window = (vd_start_datetime <= start_date_value <= vd_end_datetime and
                                    vd_start_datetime <= end_date_value <= vd_end_datetime)
                elif isinstance(start_date_value, datetime.date) and isinstance(end_date_value, datetime.date):
                    if vd_start_date and vd_end_date:
                        in_window = (vd_start_date <= start_date_value <= vd_end_date and
                                   vd_start_date <= end_date_value <= vd_end_date)

                if in_window:
                    matches.append(vd)

            # Set visit_detail_id based on matches
            if len(matches) == 1:
                thing['visit_detail_id'] = matches[0]['visit_detail_id']
                matched_count += 1
            elif len(matches) > 1:
                # Multiple matches - choose most specific (smallest duration)
                most_specific = min(matches, key=lambda vd: get_visit_detail_duration(vd))
                thing['visit_detail_id'] = most_specific['visit_detail_id']
                matched_count += 1
                logger.debug(f"{domain} event matched {len(matches)} visit_details, chose most specific (id={most_specific['visit_detail_id']})")
            else:
                # No match - leave visit_detail_id as None
                no_match_count += 1

    logger.info(f"{domain}: {matched_count} events matched to visit_detail, {no_match_count} without visit_detail match")


@typechecked
def get_visit_detail_duration(visit_detail_dict: dict) -> float:
    """
    Calculate duration of a visit_detail in days.

    Args:
        visit_detail_dict: Visit_detail record

    Returns:
        Duration in days (float)
    """
    start_datetime = visit_detail_dict.get('visit_detail_start_datetime')
    end_datetime = visit_detail_dict.get('visit_detail_end_datetime')
    start_date = visit_detail_dict.get('visit_detail_start_date')
    end_date = visit_detail_dict.get('visit_detail_end_date')

    # Prefer datetime for precision
    if start_datetime and end_datetime:
        delta = end_datetime - start_datetime
        return delta.total_seconds() / 86400  # Convert to days
    elif start_date and end_date:
        delta = end_date - start_date
        return float(delta.days)

    return 0.0


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

#    # Post-process: Create visit_detail from visit hierarchy
#    try:
#        omop_dict = reclassify_nested_visit_occurrences_as_detail(omop_dict)
#
#    except Exception as e:
#        logger.error(f"Error processing visit hierarchy: {e}")
#        logger.error(traceback.format_exc())
#        # Continue with original data if hierarchy processing fails

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
            
        #if data_dict_list is not None:
        #    print(f"...PARSED, got {len(data_dict_list)}")
        #else:
        #    print(f"...PARSED, got **NOTHING** {data_dict_list} ")

#    # Post-process: Create visit_detail from visit hierarchy
#    try:
#        omop_dict = reclassify_nested_visit_occurrences_as_detail(omop_dict)
#
#    except Exception as e:
#        logger.error(f"Error processing visit hierarchy: {e}")
#        logger.error(traceback.format_exc())
#        # Continue with original data if hierarchy processing fails

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
    assign_visit_occurrence_ids_to_events(omop_data)
    assign_visit_detail_ids_to_events(omop_data)
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
