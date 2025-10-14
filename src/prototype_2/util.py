
from collections import defaultdict
import logging 
logging.basicConfig(
        filename="layer_datasets.log",
        filemode="w",
        level=logging.INFO ,
        format='%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d %(message)s')

logger = logging.getLogger(__name__)
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