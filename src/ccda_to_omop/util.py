
from collections import defaultdict
import logging 
from typeguard import typechecked
from dateutil.parser import parse
import datetime

logging.basicConfig(
        filename="layer_datasets.log",
        filemode="w",
        level=logging.INFO ,
        format='%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
"""
    These three functions create dictionaries from the vocabulary xwalk 
    pandas dataframes.
    Each dictionary, given vocabulary and code, provides each of
    source_concept_id, target_domain_id, or target_concept_id.
    It does this by returning a row-like dictionary with those field names
    as keys.
    The columns in the source datasets differ. Read carefully.
    Only the codemap provides the source_concept_id. The others just the two
    target fields.

    Each key may have more than one value.
    { 
        (vocab, code) : [
            {   'source_concept_id': None,
                'target_domain_id': row['target_domain_id'],
                'target_concept_id': row['target_concept_id'] 
            }
        ]
    }
"""

def create_codemap_dict(codemap_df):
    logger.info(f"w xwalk create_codemap_dict {type(codemap_df)} {len(codemap_df)}")
    codemap_dict = defaultdict(list)
    for _, row in codemap_df.iterrows():
        code_system = row['src_vocab_code_system']
        if code_system is not None and isinstance(code_system, str):
            code_system = code_system.strip()
        code = row['src_code']
        if code is not None and isinstance(code_system, str):
            code = code.strip()
        codemap_dict[(code_system,  code)].append({
            'source_concept_id': row['source_concept_id'],  # dont' strip() integers
            'target_domain_id': row['target_domain_id'].strip(),
            'target_concept_id': row['target_concept_id']  # don't strip() integers
        })

    return codemap_dict
    

def create_valueset_dict(codemap_df):
    logger.info(f"w xwalk create_valueset_dict {type(codemap_df)}  {len(codemap_df)}")
    codemap_dict = {}
    for _, row in codemap_df.iterrows():
        if (row['codeSystem'], row['src_cd']) not in codemap_dict:
            codemap_dict[(row['codeSystem'], row['src_cd'])] = []
        codemap_dict[(row['codeSystem'], row['src_cd'])].append({
            'source_concept_id': None,
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id'] })

    return codemap_dict


def create_visit_dict(codemap_df):
    logger.info(f"w xwalk create_visit_dict {type(codemap_df)} {len(codemap_df)}")
    codemap_dict = {}
    for _, row in codemap_df.iterrows():
        if (row['codeSystem'], row['src_cd']) not in codemap_dict:
            codemap_dict[(row['codeSystem'], row['src_cd'])] = []
        codemap_dict[(row['codeSystem'], row['src_cd'])].append({
            'source_concept_id': None,
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id'] })

    return codemap_dict


@typechecked
def cast_to_date(string_value) ->  datetime.date | None:
    # TODO does CCDA always do dates as YYYYMMDD ?
    # https://build.fhir.org/ig/HL7/CDA-ccda/StructureDefinition-USRealmDateTimeInterval-definitions.html
    # doc says YYYMMDD... examples show ISO-8601. Should use a regex and detect parse failure.
    # TODO  when  is it date and when datetime

    try:
        datetime_val = parse(string_value, ignoretz=True)
        return datetime_val.date()
    except Exception as x:
        logger.warning(f"ERROR couldn't parse {string_value} as date. Exception:{x}")
        return None
        #return  datetime.date.fromisoformat("1970-01-01")
    except ValueError as ve:
        logger.warning(f"ERROR couldn't parse {string_value} as date. ValueError:{ve}")
        return None
        #return  datetime.date.fromisoformat("1970-01-01")


def cast_to_datetime(string_value) -> datetime.datetime | None:
    try:
        datetime_val = parse(string_value, ignoretz=True)
        return datetime_val
    except Exception as x:
        print(f"ERROR couldn't parse {string_value} as datetime. {x}")
        return None
        #return  datetime.date.fromisoformat("1970-01-01T00:00:00"