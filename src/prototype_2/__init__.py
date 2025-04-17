
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

def set_codemap_xwalk(map):
    global codemap_xwalk
    codemap_xwalk =  map

def set_ccda_value_set_mapping_table_dataset(map):
    global ccda_value_set_mapping_table_dataset
    ccda_value_set_mapping_table_dataset = map 

def set_visit_concept_xwalk_mapping_dataset(map):
    global visit_concept_xwalk_mapping_dataset
    visit_concept_xwalk_mapping_dataset = map


