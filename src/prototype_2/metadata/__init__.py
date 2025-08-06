

import prototype_2.metadata.person      as person
import prototype_2.metadata.visit_encompassingEncounter_responsibleParty as visit_encompassingEncounter_responsibleParty
import prototype_2.metadata.visit       as visit
import prototype_2.metadata.measurement as measurement
import prototype_2.metadata.measurement_vital_signs as measurement_vs
import prototype_2.metadata.observation as observation
import prototype_2.metadata.observation_social_history_smoking as observation_social_history_smoking
import prototype_2.metadata.observation_social_history_pregnancy as observation_social_history_pregnancy
import prototype_2.metadata.observation_social_history_tobacco_use as observation_social_history_tobacco_use
import prototype_2.metadata.observation_social_history_cultural as observation_social_history_cultural
import prototype_2.metadata.observation_social_history_home_environment as observation_social_history_home_environment
import prototype_2.metadata.condition as condition
import prototype_2.metadata.location as location
import prototype_2.metadata.care_site_ee_hcf_location as care_site_ee_hcf_location
import prototype_2.metadata.care_site_ee_hcf as care_site_ee_hcf
import prototype_2.metadata.care_site_pr_location as care_site_pr_location
import prototype_2.metadata.care_site_pr as care_site_pr
import prototype_2.metadata.provider as provider
import prototype_2.metadata.provider_encompassingEncounter as provider_encompassingEncounter	
import prototype_2.metadata. provider_encompassingEncounter_responsibleParty as provider_encompassingEncounter_responsibleParty
from prototype_2.metadata import visit_encompassingEncounter
import prototype_2.metadata.provider_header_documentationOf as provider_header_documentationOf
import prototype_2.metadata.medication_medication_dispense as medication_medication_dispense
import prototype_2.metadata.medication_medication_activity as medication_medication_activity
import prototype_2.metadata.immunization_immunization_activity as immunization_immunization_activity
import prototype_2.metadata.procedure_activity_procedure as procedure_activity_procedure
import prototype_2.metadata.procedure_activity_observation as procedure_activity_observation
import prototype_2.metadata.procedure_activity_act as procedure_activity_act
import prototype_2.metadata.device_organizer_supply as device_organizer_supply
import prototype_2.metadata.device_supply as device_supply
import prototype_2.metadata.device_organizer_procedure as device_organizer_procedure
import prototype_2.metadata.device_procedure as device_procedure

""" The meatadata is 3 nested dictionaries:
    - meta_dict: the dict of all domains
    - domain_dict: a dict describing a particular domain
    - field_dict: a dict describing a field component of a domain
    These names are used in the code to help orient the reader

    An output_dict is created for each domain. The keys are the field names,
    and the values are the values of the attributes from the elements.

    REMEMBER to update the ddl.py file as well.
"""

# ***
#  NB: *** Order is important here. ***
# ***
#  PKs like person and visit must come before referencing FK configs, like in measurement

meta_dict =  location.metadata | \
             provider_header_documentationOf.metadata | \
             person.metadata | \
             visit_encompassingEncounter.metadata | \
             visit_encompassingEncounter_responsibleParty.metadata | \
             visit.metadata  | \
             measurement.metadata | \
             measurement_vs.metadata | \
             observation.metadata  | \
             observation_social_history_smoking.metadata | \
             observation_social_history_pregnancy.metadata | \
             observation_social_history_tobacco_use.metadata | \
             observation_social_history_cultural.metadata | \
             observation_social_history_home_environment.metadata | \
             medication_medication_dispense.metadata | \
             medication_medication_activity.metadata | \
             condition.metadata | \
             care_site_ee_hcf.metadata | \
             care_site_ee_hcf_location.metadata | \
             care_site_pr.metadata | \
             care_site_pr_location.metadata | \
             provider.metadata | \
             immunization_immunization_activity.metadata | \
             procedure_activity_procedure.metadata | \
             procedure_activity_observation.metadata | \
             procedure_activity_act.metadata | \
             device_organizer_supply.metadata | \
             device_supply.metadata | \
             device_organizer_procedure.metadata | \
             device_procedure.metadata | \
             provider_encompassingEncounter.metadata | \
             provider_encompassingEncounter_responsibleParty.metadata 


def get_meta_dict():
    metadata = meta_dict
    try:
        from user_mappings import overlay_mappings
        metadata = meta_dict | overlay_mappings
        print("iNFO: got user mappings  and overlaid them.")
    except Exception as e:
        print("iNFO: no user mappings available, nothing overlaid, using package mappings as-is.")
        print(f"    {e}")
    return metadata
