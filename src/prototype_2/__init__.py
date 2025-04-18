
import pandas as pd
import logging
import sys
import os

MIN_PYTHON = (3, 10)
if sys.version_info < MIN_PYTHON:
    sys.exit(f"Python version {MIN_PYTHON}  or later is required.")

logging.basicConfig(
    stream=sys.stdout,
    format='%(levelname)s: %(message)s',
    # level=logging.ERROR
    level=logging.WARNING
    # level=logging.INFO
    # level=logging.DEBUG
)

codemap_xwalk = None
ccda_value_set_mapping_table_dataset = None
visit_concept_xwalk_mapping_dataset = None

codemap_xwalk_dict = None
ccda_value_set_mapping_table_dict = None
visit_concept_xwalk_mapping_dict = None


def set_codemap_xwalk_dict(map):
    global codemap_xwalk_dict
    codemap_xwalk_dict = map

def get_codemap_xwalk():
    return None # TODO

def get_codemap_xwalk_dict():
    return codemap_xwalk_dict


def set_ccda_value_set_mapping_table_dict(map):
    global ccda_value_set_mapping_table_dict
    ccda_value_set_mapping_table_dict = map 

def get_ccda_value_set_mapping_table():
    return None # TODO

def get_ccda_value_set_mapping_table_dict():
    return ccda_value_set_mapping_table_dict


def set_visit_concept_xwalk_mapping_dict(map):
    global visit_concept_xwalk_mapping_dict
    visit_concept_xwalk_mapping_dict = map

def get_visit_concept_xwalk_mapping():
    return None # TODO 
def get_visit_concept_xwalk_mapping_dict():
    return visit_concept_xwalk_mapping_dict


