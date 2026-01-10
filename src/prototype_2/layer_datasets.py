
import argparse
import logging
import math
import os
import pandas as pd
import re
from typeguard import typechecked
import traceback

try:
    from foundry.transforms import Dataset
except Exception:
    print("no foundry transforms imported")
from collections import defaultdict
import lxml
import tempfile
from numpy import int32
from numpy import int64
from numpy import float32
from numpy import datetime64
import numpy as np
import warnings
from foundry.transforms import Dataset
import datetime as DT

import prototype_2.data_driven_parse as DDP
import prototype_2.visit_reconcilliation as VR
import prototype_2.value_transformations as VT
import prototype_2.util as U
from prototype_2.ddl import sql_import_dict
from prototype_2.ddl import config_to_domain_name_dict
from prototype_2.ddl import domain_name_to_table_name
from prototype_2.metadata import get_meta_dict
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types 
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_required

DO_VISIT_DETAIL = False


""" layer_datasets.py
    This is a layer over data_driven_parse.py that takes the 
    dictionary of lists of dictionaries, a dictionary of rows
    where the keys are dataset_names. It converts these structures
    to pandas dataframes and then merges dataframes destined for /
    the same domain. Reason being that multiple places in CCDA
    generate data for the same OMOP domain. It then publishes
    the dataframes as datasets into the Spark world in Foundry.
    
    Run 
        - from dataset named "ccda_documents" with export:
            bash> python3 -m prototype_2.layer_datasets -ds ccda_documents -x
        - from directory named "resources" without export:
            bash> python3 -m prototype_2.layer_datasets -d resources
"""


#****************************************************************
#*                                                              *
warnings.simplefilter(action='ignore', category=FutureWarning) #*
#*                                                              * 
#****************************************************************a

logging.basicConfig(
        filename="layer_datasets.log",
        filemode="w",
        level=logging.INFO ,
        format='%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d %(message)s' )

logger = logging.getLogger(__name__)


@typechecked
def show_column_dict(config_name, column_dict):
    for key,val in column_dict.items():
        print(f"   config: {config_name}  key:{key} length(val):{len(val)}")


def find_max_columns(config_name :str, domain_list: list[ dict[str, tuple[ None | str | float | int | int64, str]] | None  ]) -> dict[str, any]:
    """  Give a list of dictionaries, find the maximal set of columns that has the basic OMOP columns. 

         Trying to deal with a list that may have dictionaries that lack certain fields.
         An option is to go with a completely canonical list, like from the DDL, but we want
         to remain flexible and be able to easily add columns that are not part of the DDL for 
         use later in Spark. It is also true that we do load into an RDB here, DuckDB, to 
         check PKs and FK constraints, but only on the OMOP columns. The load scripts there
         use the DDL and ignore columns to the right we want to allow here.
    """
    domain = None
    try:
        domain = config_to_domain_name_dict[config_name]
    except Exception as e:
        logger.error(f"ERROR no domain for {config_name} in {config_to_domain_name_dict.keys()}"
                     "The config_to_domain_name_dict in ddl.py probably needs this to be added to it.")
        raise e

    chosen_row =-1
    num_columns = 0
    row_num=-1
    for col_dict in domain_list:
        # Q1 does  this dict at least have what the ddl expects?
        good_row = True
        for key in sql_import_dict[domain]['column_list']:
            if key not in col_dict:
                good_row = False
        # Q2: does it have the most extra
        if good_row and len(col_dict.keys()) > num_columns:
            chosen_row = row_num
        row_num += 1
    return domain_list[row_num]


# List of columns disallowed to be NULL
NON_NULLABLE_COLUMNS = {
    table: [
        field
        for field, required in domain_dataframe_column_required[table].items()
        if required
    ]
    for table in domain_dataframe_column_required
}

@typechecked
def create_omop_domain_dataframes(omop_data: dict[str, list[ dict[str,  None | str | float | int | int64] | None  ] | None],
                                  filepath) ->  dict[str, pd.DataFrame]:
    """ transposes the rows into columns,
        creates a Pandas dataframe
    """
    df_dict = {}
    for config_name, domain_list in omop_data.items():
        # Transpose to a dictionary of named columns.

        # Initialize a dictionary of columns from schema
        if domain_list is None or len(domain_list) < 1:
            logger.error(f"(create_omop_domain_dataframes) No data to create dataframe for {config_name} from {filepath} {domain_list}")
        else:
            column_list = find_max_columns(config_name, domain_list)
            column_dict = dict((k, []) for k in column_list) #dict.fromkeys(column_list)

            # Add the data from all the rows
            for domain_data_dict in domain_list:
                for field in column_dict.keys():
                    prepared_value = None
                    if field in domain_data_dict:
                        if domain_data_dict[field] == 'RECONCILE FK':
                            logger.error(f"RECONCILE FK for {field} in {config_name}")
                            prepared_value = None
                        elif field == 'visit_concept_id' and type(domain_data_dict[field]) == str:
                            # hack when visit_type_xwalk returns a string
                            prepared_value =  int32(domain_data_dict[field])
                        elif field[-8:] == "datetime" and domain_data_dict[field] is not None:
                            try:
                                prepared_value = domain_data_dict[field].replace(tzinfo=None)
                                logger.info(f"DATETIME conversion  {type(domain_data_dict[field])} {domain_data_dict[field]} {field} ")
                            except Exception as e:
                                prepared_value = None
                                logger.error(f"ERROR  TZ {type(domain_data_dict[field])} {domain_data_dict[field]} {field} {e} TB:{traceback.format_exc(e)}")
                        else:
                            prepared_value = domain_data_dict[field]

                        if prepared_value != prepared_value:
                            # for debuggin in Spark, raise exception
                            msg=f"layered_datasets.create_omop_domain_dataframes() NaN/NaT {config_name} {field} {prepared_value} <--"
                            raise Exception(msg)

                        # if prepared_value is None and field == 'condition_start_date':
                        #    # for debuggin in Spark, raise exception
                        #    msg=f"layered_datasets.create_omop_domain_dataframes() None start-date {config_name} {field} {prepared_value} <--"
                        #    raise Exception(msg)
                    else:
                        # field is not in dict, so would be null, but odd for other reasons, want to know about this
                        if prepared_value is None:
                            # for debuggin in Spark, raise exception
                            msg=f"layered_datasets.create_omop_domain_dataframes() not in dict {config_name} {field} {prepared_value} <--"
                            raise Exception(msg)
                    column_dict[field].append(prepared_value)

            # Use domain_dataframe_colunn_types to cast dataframe columns as directed
            # Create a Pandas dataframe from the data_dict
            try:
                ##show_column_dict(config_name, column_dict)
                domain_df = pd.DataFrame(column_dict)
                domain_name = config_to_domain_name_dict[config_name]
                table_name = domain_name_to_table_name[domain_name]
                if table_name in domain_dataframe_column_types.keys():
                    non_nullable_cols = NON_NULLABLE_COLUMNS.get(table_name, [])
                    for column_name, column_type in domain_dataframe_column_types[table_name].items():
                        if column_type in [datetime64, DT.date, DT.datetime]:
                            domain_df[column_name] = pd.to_datetime(domain_df[column_name], errors='coerce')
                        else:
                            try:
                                # Only fill missing values for non-nullable columns
                                if column_name not in non_nullable_cols:
                                    # For nullable integer columns, use pandas nullable Int64 and Int32 type
                                    if column_type == int64:
                                        domain_df[column_name] = domain_df[column_name].astype('Int64')
                                    elif column_type == int32:
                                        domain_df[column_name] = domain_df[column_name].astype('Int32')
                                    else:
                                        # leave as None/NaN for other types
                                        domain_df[column_name] = domain_df[column_name]
                                else:
                                    domain_df[column_name] = domain_df[column_name].fillna(0).astype(column_type)  # generates downcasting wwarnings and doesn't throw, 
                                    # domain_df[column_name] = domain_df[column_name].fillna(cast(column_type, 0)).astype(column_type)  # throwss
                                    # domain_df[column_name] = domain_df[column_name].astype(column_type).fillna(0) # cast errors on the None
                            except Exception as e:
                                logger.error(f"CAST ERROR in layer_datasets.py create_omop_domain_dataframes() table:{table_name} column:{column_name} type:{column_type}  ")
                                if column_name in domain_df:
                                    logger.error(f"    (cont.)   value:{domain_df[column_name]}  type:{type(domain_df[column_name])}")
                                else:
                                    logger.error(f"    (cont.)  column \"{column_name}\" is not in the domain_df for domain \"{domain_name}\"")
                                logger.error(f"    (cont.)  exception:{e}")

                    # After casting datetimes, drop rows with nulls in non-nullable columns
                    before_dropped = len(domain_df)
                    domain_df = domain_df.dropna(subset=non_nullable_cols)
                    after_dropped = len(domain_df)

                    if before_dropped != after_dropped:
                        logger.warning(
                            f"{config_name}: dropped {before_dropped - after_dropped} rows with missing required fields "
                            f"(table={table_name})"
                            )

                df_dict[config_name] = domain_df
            except ValueError as ve:
                logger.info(f"when creating dataframe for {config_name} in {filepath} HAVE DATA {df_dict}")
                show_column_dict(config_name, column_dict)
                df_dict[config_name] = None
            # except Exception as x:
                # logger.error(f"exception {config_name} in {filepath} NO DATA RETURNED {x}")
                # show_column_dict(config_name, column_dict)
                # df_dict[config_name] = None
            logger.error(f"(create_omop_domain_dataframes) No data to create dataframe for {config_name} from {filepath} {domain_list}")
    return df_dict


@typechecked
def write_csvs_from_dataframe_dict(df_dict :dict[str, pd.DataFrame], file_name, folder):
    """ writes a CSV file for each dataframe
        uses the key of the dict as filename
    """
    for config_name, domain_dataframe in df_dict.items():
        filepath = f"{folder}/{file_name}__{config_name}.csv"
        if domain_dataframe is not None:
            domain_dataframe.to_csv(filepath, sep=",", header=True, index=False)
        else:
            logger.error(f"ERROR: NOT WRITING domain {config_name} to file {filepath}, no dataframe")

@typechecked
def process_string(contents, filepath, write_csv_flag) -> dict[str, pd.DataFrame]:
    """
        * E X P E R I M E N T A L *

        Processes a string creates dataset and writes csv
        returns dataset

        (really calls into a lot of DDP detail and seems like it belongs there)
    """
    base_name = os.path.basename(filepath)

    logger.info(f"parsing string from {filepath}")
    omop_data = DDP.parse_string(contents, filepath, get_meta_dict())
    logger.info(f"--parsing string from file:{filepath} keys:{omop_data.keys()} p:{len(omop_data['Person'])} m:{len(omop_data['Measurement'])} ")

    # Visit FK reconciliation:
    VR.assign_visit_occurrence_ids_to_events(omop_data)

    if DO_VISIT_DETAIL:
        if 'VISITDETAIL_visit_occurrence' in omop_data and omop_data['VISITDETAIL_visit_occurrence']:
            logger.info("Starting visit_detail FK reconciliation")
            VR.assign_visit_detail_ids_to_events(omop_data)

    logger.info(f"-- after reconcile parsing string from file:{filepath} keys:{omop_data.keys()} p:{len(omop_data['Person'])} m:{len(omop_data['Measurement'])} ")
    if omop_data is not None or len(omop_data) < 1:
        dataframe_dict = create_omop_domain_dataframes(omop_data, filepath)
    else:
        logger.error(f"no data from {filepath}")

    if write_csv_flag:
        write_csvs_from_dataframe_dict(dataframe_dict, base_name, "output")
    return dataframe_dict


@typechecked
def process_string_to_dict(contents, filepath, write_csv_flag, codemap_dict, visit_map_dict, valueset_map_dict, mspi_map_dict, partner_map_dict ) -> dict[str, list[dict]]:
    """
        Processes an XML CCDA string, returns data as Python structures.

        Requires python dictionaries for mapping, brought in here, initialized to the package as 
        part of making them available to executors in Spark.

        Returns  dict of column lists
    """
    VT.set_codemap_dict(codemap_dict)
    VT.set_valueset_dict(valueset_map_dict)
    VT.set_visitmap_dict(visit_map_dict)

    if mspi_map_dict:
        VT.set_mspi_map(mspi_map_dict)
    if partner_map_dict:
        VT.set_partner_map(partner_map_dict)

    if len(VT.get_codemap_dict()) < 1:
        raise Exception(f"codemap length {len(VT.get_codemap_dict())}")
    if len(VT.get_valueset_dict() ) < 1:    
        raise Exception(f"valueset map length {len(VT.get_valueset_dict())}" )
    if len(VT.get_visitmap_dict() ) < 1:
        raise Exception(f"visit map length {len(VT.get_visitmap_dict())}" )

    test_value = codemap_dict[('2.16.840.1.113883.6.96', '608837004')]
    if test_value[0]['target_concept_id'] != 1340204:
        msg=f"codemap_xwalk test failed to deliver correct code, got: {test_value}"
        raise Exception(msg)

    test_value = valueset_map_dict[('2.16.840.1.113883.6.238','2106-3')]
    if test_value[0]['target_concept_id'] != '8527':
                msg=f"valueset map test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)

    test_value = visit_map_dict[('2.16.840.1.113883.6.259','1026-4')]
    if test_value[0]['target_concept_id'] != '9201':
                msg=f"visit map test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)

    omop_data = DDP.parse_string(contents, filepath, get_meta_dict())

    # Visit FK reconciliation:
    VR.assign_visit_occurrence_ids_to_events(omop_data)

    if DO_VISIT_DETAIL:
        if 'VISITDETAIL_visit_occurrence' in omop_data and omop_data['VISITDETAIL_visit_occurrence']:
            logger.info("Starting visit_detail FK reconciliation")
            VR.assign_visit_detail_ids_to_events(omop_data)

    return omop_data


@typechecked
def process_file(filepath, write_csv_flag) -> dict[str, pd.DataFrame]:
    """ processes file, processes visits, creates dataset, writes csv
        returns dataset
    """
    base_name = os.path.basename(filepath)

    omop_data = DDP.parse_doc(filepath, get_meta_dict())

    # Visit FK reconciliation:
    VR.assign_visit_occurrence_ids_to_events(omop_data)

    if DO_VISIT_DETAIL:
        if 'VISITDETAIL_visit_occurrence' in omop_data and omop_data['VISITDETAIL_visit_occurrence']:
            logger.info("Starting visit_detail FK reconciliation")
            VR.assign_visit_detail_ids_to_events(omop_data)

    # Convert from list of dictionaries/records to dataframes/datasets
    if omop_data is not None or len(omop_data) < 1:
        dataframe_dict = create_omop_domain_dataframes(omop_data, filepath)
    else:
        logger.error(f"no data from {filepath}")
        return None

    if write_csv_flag:
        write_csvs_from_dataframe_dict(dataframe_dict, base_name, "output")

    return dataframe_dict


@typechecked
def dict_summary(my_dict):
    for key in my_dict:
        logger.info(f"Summary {key} {len(my_dict[key])}")


@typechecked
def build_file_to_domain_dict(meta_config_dict :dict[str, dict[str, dict[str, str]]]) -> dict[str, str]:
    """ The meta_config_dict is a dictionary keyed by domain filenames that
        has the data that drives the conversion. Included is a 'root' element
        that has an attribute 'expected_domain_id' that we're after to identify
        the OMOP domain that a file's data is destined for.
        This is where multiple files for the same domain get combined.     
        
        For example, the Measurement domain, rows for the measurement table can 
        come from at least two kinds of files:
         <file>__Measurement_results.csv
         <file>__Measurement_vital_signs.csv
         
       This map maps from filenames to domains
    """
    file_domain_map = {} 
    for file_domain in meta_config_dict:
        file_domain_map[file_domain] = meta_config_dict[file_domain]['root']['expected_domain_id']
    return file_domain_map


@typechecked
def export_to_foundry(domain_name, df):
    """
    exports domains to datasets in Foundry.
    """
    
    if domain_name not in domain_name_to_table_name:
        logger.error(f"ERROR: not able to map domain:{domain_name} to dataset/table name")

    dataset_name = domain_name_to_table_name[domain_name]
    logger.info(f"EXPORTING: {dataset_name}")
    try:
        export_dataset = Dataset.get(dataset_name)
        export_dataset.write_table(df)
        logger.info(f"Successfully exported dataset '{dataset_name}'")
    except Exception as e:
        logger.error(f"    ERROR: {e}")
        error_message = str(e)

        
def combine_datasets(omop_dataset_dict):    

    # COMBINE like datasets, datasets from different parse configurations in the metadata
    # that produce rows for the same domain. 
    #
    # We need to collect all files/datasets that have the same expected_domain_id.
    # For example, the Measurement domain, rows for the measurement table can 
    # come from at least two kinds of files:
    #     <file>__Measurement_results.csv
    #     <file>__Measurement_vital_signs.csv
    # Two dictionaries at play here:
    # 1 is the omop_dataset_dict which is a dictionary of datasets keyed by their domain_keys or config filenames
    # 2 is the config data that comes from get_meta_dict

    file_to_domain_dict = build_file_to_domain_dict(get_meta_dict())
    domain_dataset_dict = {}
    for filename in omop_dataset_dict:
        domain_id = file_to_domain_dict[filename]
        if filename in omop_dataset_dict and omop_dataset_dict[filename] is not None:
            if domain_id in domain_dataset_dict and domain_dataset_dict[domain_id] is not None:
                domain_dataset_dict[domain_id] = pd.concat([ domain_dataset_dict[domain_id], omop_dataset_dict[filename] ])
            else:
                domain_dataset_dict[domain_id] = omop_dataset_dict[filename]      
        else:
            logger.error(f"NO DATA for config {filename} in LD.combine_datasets()")

    return domain_dataset_dict


def do_export_datasets(domain_dataset_dict):
    # export the datasets to Spark/Foundry
    for domain_id in domain_dataset_dict:
        logger.info(f"Exporting dataset for domain:{domain_id} dim:{domain_dataset_dict[domain_id].shape}")
        export_to_foundry(domain_id, domain_dataset_dict[domain_id])      

        
def do_write_csv_files(domain_dataset_dict):
    for domain_id in domain_dataset_dict:
        if domain_id in domain_dataset_dict and domain_dataset_dict[domain_id] is not None:
            logger.info(f"Writing CSV for domain:{domain_id} dim:{domain_dataset_dict[domain_id].shape}")
            domain_dataset_dict[domain_id].to_csv(f"output/domain_{domain_id}.csv")
        else:
            logger.error(f"Error Writing CSV for domain:{domain_id} no such table in dict")
 

        
# ENTRY POINT for dataset of files
def process_dataset_of_files(dataset_name, export_datasets, write_csv_flag, limit, skip):
    logger.info("starting dataset:{dataset_name} export:{export_datasets} csv:{write_csv_flag} limit:{limit}")
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    ccda_documents = Dataset.get(dataset_name)
    logger.info(ccda_documents.files())
    ccda_documents_generator = ccda_documents.files()
    skip_count=0
    file_count=0
    for filegen in ccda_documents_generator:
        if skip>0 and skip_count < skip:
            skip_count+=1
            logger.info(f"skipping  {skip_count} {type(filegen)}")
        else:
            if limit == 0 or file_count < limit:
                filepath = filegen.download()
                
                logger.info(f"PROCESSING {file_count} {os.path.basename(filepath)}  {file_count}  export:{export_datasets} csv:{write_csv_flag} limit:{limit}")
                new_data_dict = process_file(filepath, write_csv_flag)
                
                for key in new_data_dict:
                    if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                        if new_data_dict[key] is  not None:
                            omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
                    else:
                        omop_dataset_dict[key]= new_data_dict[key]
                    if new_data_dict[key] is not None:
                        logger.info(f"{filepath} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                    else:
                        logger.info(f"{filepath} {key} {len(omop_dataset_dict)} None / no data")
                file_count += 1
            else:
                break
            
    domain_dataset_dict = combine_datasets(omop_dataset_dict)
    if write_csv_flag:
        logger.info(f"Writing CSV for input dataset: {dataset_name}")
        do_write_csv_files(domain_dataset_dict)

    if export_datasets:
        logger.info(f"Exporting dataset for {dataset_name}") 
        do_export_datasets(domain_dataset_dict)
    
    
# ENTRY POINT for dataset of strings
def process_dataset_of_strings(dataset_name, export_datasets, write_csv_flag):
    logger.info(f"DATA SET NAME: {dataset_name}")
    
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    ccda_ds = Dataset.get(dataset_name)
    ccda_df = ccda_ds.read_table(format='pandas')
    # columns: 'timestamp', 'mspi', 'site', 'status_code', 'response_text',
    # FOR EACH ROW 
    # Iterating through ALL rows, not just the first one
    for index, row in ccda_df.iterrows():
        # Using columns from the dataframe to preserve identity
        text = row['response_text'] 
        original_filename = row.get('response_file_path', f"generated_{index}.xml")
        
        doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
        # (don't close the opening tag because it has attributes)
        # works: doc_regex = re.compile(r'(<section>.*?</section>)', re.DOTALL)
        
        # FOR EACH "DOC" in this row (hopefully just 1)
        for match in doc_regex.finditer(text):
            match_tuple = match.groups(0)
            
            # We use a directory + the original filename 
            # This ensures VT.map_filename_to_mspi finds the key in the map!
            with tempfile.TemporaryDirectory() as temp_dir:
                file_path = os.path.join(temp_dir, os.path.basename(original_filename))
                
                with open(file_path, 'w') as f:
                    f.write(match_tuple[0]) # .encode())
                
                new_data_dict = process_file(file_path, write_csv_flag)
                
                if new_data_dict:
                    for key, df in new_data_dict.items():
                        if df is not None:
                            if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                                omop_dataset_dict[key] = pd.concat([omop_dataset_dict[key], df], ignore_index=True)
                            else:
                                omop_dataset_dict[key] = df
            
    domain_dataset_dict = combine_datasets(omop_dataset_dict)
    if write_csv_flag:
        do_write_csv_files(domain_dataset_dict)

    if export_datasets:
        do_export_datasets(domain_dataset_dict)
    
    
# ENTRY POINT for directory of files
def process_directory(directory_path, export_datasets, write_csv_flag):
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    only_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    for file in (only_files):
        if file.endswith(".xml"):
            new_data_dict = process_file(os.path.join(directory_path, file), write_csv_flag)
            for key in new_data_dict:
                if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                    if new_data_dict[key] is  not None:
                        omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
                else:
                    omop_dataset_dict[key]= new_data_dict[key]
                if new_data_dict[key] is not None:
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                else:
                    logger.info(f"{file} {key} {len(omop_dataset_dict)} None / no data")
                    
    domain_dataset_dict = combine_datasets(omop_dataset_dict)
    if write_csv_flag:
        do_write_csv_files(domain_dataset_dict)

    if export_datasets:
        do_export_datasets(domain_dataset_dict)
         

# JUPYTER ENTRY POINT
def main():
    parser = argparse.ArgumentParser(
        prog='CCDA - OMOP parser with datasets layer layer_datasets.py',
        description="reads CCDA XML, translate to and writes OMOP CSV files",
        epilog='epilog?')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--directory', help="directory of files to parse")
    group.add_argument('-f', '--filename', help="filename to parse")
    group.add_argument('-ds', '--dataset', help="dataset to parse")
    parser.add_argument('-x', '--export', action=argparse.BooleanOptionalAction, help="export to foundry")
    parser.add_argument('-c', '--write_csv', action=argparse.BooleanOptionalAction, help="write CSV files to local")
    #parser.add_argument('-l', '--limit', action=argparse.BooleanOptionalAction, type=int, help="max files to process")  #, default=0)
    parser.add_argument('-l', '--limit', type=int, help="max files to process", default=0)
    parser.add_argument('-s', '--skip', type=int, help="files to skip before processing to limit, -s 100 ", default=0) 
    args = parser.parse_args()
    print(f"got args:  dataset:{args.dataset} export:{args.export} csv:{args.write_csv} limit:{args.limit}")
    print(args)

    
    omop_dataset_dict = {} # keyed by dataset_names (legacy domain names)
    
    try:
        logger.info("starting with maps")
        logger.info("xwalk visitmap")
        visit_map_df = Dataset.get("visit_concept_xwalk_mapping_dataset").read_table(format="pandas")
        visitmap_dict = U.create_visit_dict(visit_map_df)
        logger.error(f"VISITMAP  {len(visitmap_dict)}")
        VT.set_visitmap_dict(visitmap_dict)

        logger.info("xwalk valuesetmap")
        valueset_map_df = Dataset.get("ccda_value_set_mapping_table_dataset").read_table(format="pandas")
        valueset_dict = U.create_valueset_dict(valueset_map_df)
        logger.error(f"VALUESET  {len(valueset_dict)}")
        VT.set_valueset_dict(valueset_dict)
        
        logger.info("xwalk codemap")
        codemap_df = Dataset.get("codemap_xwalk").read_table(format="pandas")
        codemap_dict = U.create_codemap_dict(codemap_df)
        logger.error(f"CODEMAP  {len(codemap_dict)}")
        VT.set_codemap_dict(codemap_dict)

        metadata_df = Dataset.get("ccda_response_metadata").read_table(format="pandas")
        # Create a dictionary: { 'filename.xml': mspi_value }
        # We use 'response_file_path' as the key to match the 'filename' in the OMOP output
        mspi_map = dict(zip(metadata_df['response_file_path'], metadata_df['mspi']))
        partner_map = dict(zip(metadata_df['response_file_path'], metadata_df['healthcare_site']))
        VT.set_mspi_map(mspi_map)
        VT.set_partner_map(partner_map)
        logger.info(f"MSPI Map initialized with {len(mspi_map)} entries.")

        
        
        logger.info("Successfully loaded and initialized mapping dictionaries.")

    except Exception as e:
        logger.error(f"Failed to load mapping datasets from Foundry: {e}")
        logger.error(traceback.format_exc(e))
        return # Exit if mappings cannot be loaded

    if True:
        # Single File, put the datasets into the omop_dataset_dict
        if args.filename is not None:
            process_file(args.filename, args.write_csv)
            
        elif args.directory is not None:
            domain_dataset_dict = process_directory(args.directory, args.export, args.write_csv)
        elif args.dataset is not None:
            domain_dataset_dict = process_dataset_of_files(args.dataset, args.export, args.write_csv, args.limit, args.skip)
        else:
            logger.error("Did args parse let us  down? Have neither a file, nor a directory.")

            
if __name__ == '__main__':
    main()
