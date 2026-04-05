""" setup_omop
    Initiates an in-memory instance of DuckDB, reads in the OMOP DDL,
    and reads in any data provided.

    For now, it's useful to see issues regarding  PK presence and uniqueness, datatypes..

    TODO: This includes abuse of the OMOP DDL.  Better solutions  include
    - better metadata so the resulting dataset and CSV look like OMOP
    - a second stage here that modifies the resulting datasets to look more
      like OMOP
    - some compromise means getting a handle on how narrow the CSV can be
      compared to OMOP. Can you leave out unused nullable fields?
"""

OMOP_CDM_DIR = "resources/" #  "../CommonDataModel/inst/ddl/5.3/duckdb/"
OMOP_CSV_DATA_DIR = "output/"

import io
import os
import re
import logging
import importlib.util
from typing import Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(levelname)s: %(message)s',
#    filename=f"logs/load_omop.log",
#    force=True,
    level=logging.INFO
    # level=logging.WARNING level=logging.ERROR # level=logging.INFO # level=logging.DEBUG
)



METADATA_DIR = os.path.join(os.path.dirname(__file__), 'metadata')


def generate_cfg_name_to_domain_map() -> Dict[str, str]:
    """
    Dict[table_name] --> domain name

    Scans all .py files in the metadata directory, inspects their
    'metadata' variable, and builds a dictionary mapping config names
    to their 'expected_domain_id'.
    """
    domain_map = {}
    if not os.path.isdir(METADATA_DIR):
        logging.error(f"Metadata directory not found at: {METADATA_DIR}")
        return {}

    # Scan the directory for Python files
    for filename in os.listdir(METADATA_DIR):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]
            file_path = os.path.join(METADATA_DIR, filename)
            try:
                # Dynamically load the module from its file path
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Check if the module has the required 'metadata' dictionary
                if hasattr(module, 'metadata'):
                    # --- CORE LOGIC: Extract the mapping ---
                    # Iterate through the top-level keys in the metadata dict
                    for config_name, domain_details in module.metadata.items():
                        # Safely get the nested 'expected_domain_id'
                        root_info = domain_details.get('root', {})
                        expected_domain = root_info.get('expected_domain_id')
                        if expected_domain:
                            domain_map[config_name] = expected_domain
                        else:
                            logging.warning(f"'{config_name}' in '{filename}' is missing 'expected_domain_id'.")
                else:
                    logging.warning(f"Module '{module_name}' does not contain a 'metadata' dictionary.")
            except Exception as e:
                logging.error(f"Failed to process metadata from '{filename}': {e}")

    return domain_map


config_to_domain_name_dict = generate_cfg_name_to_domain_map()


domain_name_to_table_name = {
    'Care_Site'  : 'care_site',
    'Condition'  : 'condition_occurrence',
    'Drug'       : 'drug_exposure',
    'Location'   : 'location',
    'Measurement': 'measurement',
    'Observation': 'observation',
    'Person'     : 'person',
    'Procedure'  : 'procedure_occurrence',
    'Provider'   : 'provider',
    'Visit'      : 'visit_occurrence',
    'Device'     : 'device_exposure',
    'VisitDetail': 'visit_detail',
}

sql_import_dict = {
    'Device': {
        'column_list': [
            'device_exposure_id',
            'person_id',
            'device_concept_id',
            'device_exposure_start_date',
            'device_exposure_start_datetime',
            'device_exposure_end_date',
            'device_exposure_end_datetime',
            'device_type_concept_id',
            'unique_device_id',
            'quantity',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'device_source_value',
            'device_source_concept_id'
        ],
        'sql': None,
        'table_name': "device_exposure",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(device_exposure_id) as p_id, 
                       count(distinct device_exposure_id) as d_p_id
                FROM device_exposure
                """
    },
    'Procedure': {
        'column_list': [
            'procedure_occurrence_id',
            'person_id',
            'procedure_concept_id',
            'procedure_date',
            'procedure_datetime',
            'procedure_type_concept_id',
            'modifier_concept_id',
            'quantity',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'procedure_source_value',
            'procedure_source_concept_id',
            'modifier_source_value'
        ],
        'sql': None,
        'table_name': "procedure_occurrence",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(procedure_occurrence_id) as p_id, 
                       count(distinct procedure_occurrence_id) as d_p_id
                FROM procedure_occurrence
                """
    },
    'Drug': {
        'column_list': [
            'drug_exposure_id',
            'person_id',
            'drug_concept_id',
            'drug_exposure_start_date',
            'drug_exposure_start_datetime',
            'drug_exposure_end_date',
            'drug_exposure_end_datetime',
            'verbatim_end_date',
            'drug_type_concept_id',
            'stop_reason',
            'refills integer',
            'quantity',
            'days_supply',
            'sig',
            'route_concept_id',
            'lot_number',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'drug_source_value',
            'drug_source_concept_id',
            'route_source_value',
            'dose_unit_source_value'
        ],
        'sql': None,
        'table_name': "drug_exposure",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(drug_exposure_id) as p_id, 
                       count(distinct drug_exposure_id) as d_p_id
                FROM drug_exposure
                """
    },
    'Observation': {
        'column_list': [
            'observation_id',
            'person_id',
            'observation_concept_id',
            'observation_date',
            'observation_datetime',
            'observation_type_concept_id',
            'value_as_number',
            'value_as_string',
            'value_as_concept_id',
            'qualifier_concept_id',
            'unit_concept_id',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'observation_source_value',
            'observation_source_concept_id',
            'unit_source_value',
            'qualifier_source_value'
        ],
        'sql': None,
        'table_name': "observation",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(observation_id) as p_id, 
                       count(distinct observation_id) as d_p_id
                FROM observation
                """
    },
    'Location': {
        'column_list': [
            'location_id', 'address_1', 'address_2', 'city', 'state', 'zip', 
            'county', 'location_source_value'
        ],
        'sql': None,
        'table_name': "location",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(location_id) as p_id,
                       count(distinct location_id) as d_p_id
                FROM location
                """
    },
    'Provider': {
        'column_list': [
            'provider_id',
            'provider_name',
            'npi',
            'dea',
            'specialty_concept_id',
            'care_site_id',
            'year_of_birth',
            'gender_concept_id',
            'provider_source_value',
            'specialty_source_value',
            'specialty_source_concept_id',
            'gender_source_value',
            'gender_source_concept_id'
        ],
        'sql': None,
        'table_name': "provider",
        'pk_query': """
                SELECT count(*) as row_ct, 
                count(provider_id) as p_id, 
                count(distinct provider_id) as d_p_id
                FROM provider
                """
    },
    'Care_Site': {
        'column_list': [
            'care_site_id',
            'care_site_name',
            'place_of_service_concept_id',
            'location_id', 
            'care_site_source_value',
            'place_of_service_source_value'
        ],
        'sql': None,
        'table_name': "care_site",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(care_site_id) as p_id, 
                       count(distinct care_site_id) as d_p_id
                FROM care_site
                """
    },
    'Person': {
        'column_list': [
            'person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth', 'day_of_birth',
            'birth_datetime', 'race_concept_id', 'ethnicity_concept_id',
            'location_id', 'provider_id', 'care_site_id', 'person_source_value',
            'gender_source_value', 'gender_source_concept_id', 'race_source_value',
            'race_source_concept_id', 'ethnicity_source_value', 'ethnicity_source_concept_id'
            ],
        'sql': None,
        'table_name': "person",
        'pk_query': """
                SELECT count(*) as row_ct, 
                       count(person_id) as p_id,
                       count(distinct person_id) as d_p_id
                FROM person
                """
    },
    'Visit': {
        'column_list': [
                    'visit_occurrence_id',
                    'person_id',
                    'visit_concept_id',
                    'visit_start_date', 'visit_start_datetime',
                    'visit_end_date', 'visit_end_datetime',
                    'visit_type_concept_id',
                    'provider_id', 'care_site_id',
                    'visit_source_value', 'visit_source_concept_id',
                    'admitting_source_concept_id', 'admitting_source_value',
                    'discharge_to_source_concept_id', 'discharge_to_source_value',
                    'preceding_visit_occurrence_id'
                    ],
        'sql': None,
        'table_name': "visit_occurrence",
        'pk_query': """
                SELECT count(*) as row_ct,
                       count(visit_occurrence_id) as p_id,
                       count(distinct visit_occurrence_id) as d_p_id
                FROM visit_occurrence
                """
    },
    'VisitDetail': {
        'column_list': [
                    'visit_detail_id',
                    'person_id',
                    'visit_detail_concept_id',
                    'visit_detail_start_date', 'visit_detail_start_datetime',
                    'visit_detail_end_date', 'visit_detail_end_datetime',
                    'visit_detail_type_concept_id',
                    'provider_id', 'care_site_id',
                    'visit_detail_source_value', 'visit_detail_source_concept_id',
                    'admitting_source_concept_id', 'admitting_source_value',
                    'discharge_to_source_concept_id', 'discharge_to_source_value',
                    'preceding_visit_detail_id', 'visit_detail_parent_id',
                    'visit_occurrence_id'
                    ],
        'sql': None,
        'table_name': "visit_detail",
        'pk_query': """
                SELECT count(*) as row_ct,
                       count(visit_detail_id) as p_id,
                       count(distinct visit_detail_id) as d_p_id
                FROM visit_detail
                """
    },
    'Measurement': {
        'column_list': [
                    'measurement_id', ' person_id', 'measurement_concept_id',
                    'measurement_date', 'measurement_datetime', 'measurement_time',
                    'measurement_type_concept_id', 'operator_concept_id',
                    'value_as_number', 'value_as_concept_id',
                    'unit_concept_id', 'range_low', 'range_high',
                    'provider_id',
                    'visit_occurrence_id', 'visit_detail_id',
                    'measurement_source_value', 'measurement_source_concept_id',
                    'unit_source_value', 'value_source_value'
                    ],
        'sql': None,
        'table_name': "measurement",
        'pk_query': """
                SELECT count(*) as row_ct,
                       count(measurement_id) as p_id,
                       count(distinct measurement_id) as d_p_id
                FROM measurement
                """
    },
    'Condition': {
        'column_list': [
                    'condition_occurrence_id', ' person_id', 'condition_concept_id',
                    'condition_start_date', 'condition_start_datetime',
                    'condition_end_date', 'condition_end_datetime'
                    'condition_type_concept_id',
                    'condition_status_concept_id',
                    'stop_reason',
                    'provider_id',
                    'visit_occurrence_id', 'visit_detail_id',
                    'condition_source_value', 'condition_source_concept_id',
                    'condition_status_source_concept_id', 'condition_status_source_value'
                    ],
        'sql': None,
        'table_name': "condition_occurrence",
        'pk_query': """### TODO
                SELECT count(*) as row_ct,
                       count(condition_occurrence_id) as p_id,
                       count(distinct condition_occurrence_id) as d_p_id
                FROM condition_occurrence
                """
    }
}
