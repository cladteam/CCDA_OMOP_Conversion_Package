import ccda_to_omop

def set_allow_no_matching_concept(b):
    ccda_to_omop.ALLOW_NO_MATCHING_CONCEPT=b

def get_allow_no_matching_concept():
    return ccda_to_omop.ALLOW_NO_MATCHING_CONCEPT
