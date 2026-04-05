
from numpy import int32
from numpy import float32
import ccda_to_omop.value_transformations as VT


# This config intentionally left blank to mark that we have considered this source of data from
# the section snooper and deemed it inappropriate or unusable.
metadata = {}
    #'VISIT-from-encounter_participant_role': {
    	  #  'element':
    		 # ("./hl7:component/hl7:structuredBody/hl7:component/hl7:section/"
    		  # "hl7:templateId[@root='2.16.840.1.113883.10.20.22.2.22' or @root='2.16.840.1.113883.10.20.22.2.22.1']"
    		  # '/../hl7:entry/hl7:encounter[@moodCode="EVN"]/hl7:participant/hl7:participantRole')
    		    # FIX: another template at the observation level here: "2.16.840.1.113883.10.20.22.4.2  Result Observation is an entry, not a section
                #The concepts for a participantRole, where the participant is a kind of location must be getting mapped to a visit type and leading us here. 
                #Regardless of the exact concepts (from HSLOC), there aren't dates or a provider available, making it a useless visit row.