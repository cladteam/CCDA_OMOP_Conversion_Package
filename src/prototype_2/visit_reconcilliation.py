
"""
    Visit_reconcilliation.py

    This module contains functions for linking domain rows to visits
    as well as functions to differentiate visit_occurrence from visit_detail.

    Main entry points are:
    - assign_visit_occurrence_ids_to_events()
    - assign_visit_detail_ids_to_events()
    - reclassify_nested_visit_occurrences_as_detail()

    This code processes visit data to create a hierarchical structure
    where inpatient parent visits (<= 1 year duration) are kept in visit_occurrence
    and their nested child visits are moved to visit_detail.

    Process:
    1. Identify inpatient parent visits with duration <= 1 year
    2. Find visits temporally nested within each parent
    3. Create visit_detail records for nested children
    4. Return updated visit_occurrence (parents + standalone) and new visit_detail list

"""
import datetime
import logging
from numpy import int64
from typeguard import typechecked
from prototype_2 import ddl as DDL

logger = logging.getLogger(__name__)




# Type alias for OMOP record dictionaries
OMOPRecord = dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date]

# OMOP standard concept IDs for inpatient visits
INPATIENT_CONCEPT_IDS = {
    9201   # Inpatient Visit
}

# Maximum duration for a valid inpatient parent (in days)
MAX_PARENT_DURATION_DAYS = 367


@typechecked
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

    # Strip timezone info
    start = strip_tz(start)
    end = strip_tz(end)

    # Handle both datetime and date objects
    if isinstance(start, datetime.datetime) and isinstance(end, datetime.datetime):
        return (end - start).total_seconds() / 86400
    elif isinstance(start, datetime.date) and isinstance(end, datetime.date):
        return float((end - start).days)
    else:
        return None


@typechecked
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


@typechecked
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

    # Normalize timezone info to allow comparison (strip timezone if present)
    child_start = strip_tz(child_start)
    child_end = strip_tz(child_end)
    parent_start = strip_tz(parent_start)
    parent_end = strip_tz(parent_end)

    # Check temporal containment
    return parent_start <= child_start and parent_end >= child_end


@typechecked
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


@typechecked
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


@typechecked
def reclassify_nested_visit_occurrences_as_detail(omop_dict: dict[str, list[OMOPRecord] | None]) -> dict[str, list[OMOPRecord] | None]:
    """
    Main entry point for visit hierarchy processing.
    Merges visits from both 'Visit' and 'Visit_encompassingEncounter' configs,
    deduplicates them, processes hierarchy, and creates visit_detail records.

    Strategy:
    1. Collect visits from both configs
    2. Deduplicate (keep 'Visit' when duplicate exists with 'Visit_encompassingEncounter')
    3. Process merged list as unified hierarchy
    4. Put results in 'Visit' config and create 'VISITDETAIL_visit_occurrence' config

    Args:
        omop_dict: Dictionary of config_name → list of records

    Returns:
        Updated omop_dict with:
        - 'Visit': top-level visit_occurrence records (merged from both configs)
        - 'VISITDETAIL_visit_occurrence': visit_detail records
        - 'Visit_encompassingEncounter': removed (merged into 'Visit')
    """
    # Step 1: Collect visits from both configs
    all_visits = []

    if 'Visit' in omop_dict and omop_dict['Visit']:
        all_visits.extend(omop_dict['Visit'])
        logger.info(f"Collected {len(omop_dict['Visit'])} visits from 'Visit' config")

    if 'Visit_encompassingEncounter' in omop_dict and omop_dict['Visit_encompassingEncounter']:
        all_visits.extend(omop_dict['Visit_encompassingEncounter'])
        logger.info(f"Collected {len(omop_dict['Visit_encompassingEncounter'])} visits from 'Visit_encompassingEncounter' config")

    if not all_visits:
        logger.info("No visit data found to process")
        return omop_dict

    # If only one visit, no hierarchy processing needed
    if len(all_visits) == 1:
        logger.info("Only one visit found - no hierarchy processing needed")
        omop_dict['Visit'] = all_visits
        return omop_dict

    logger.info(f"Total visits collected before deduplication: {len(all_visits)}")

    # Step 2: Deduplicate - keep 'Visit' when duplicate exists
    # Use visit_occurrence_id as the unique key
    visit_map = {}  # Key: visit_occurrence_id, Value: visit record

    for visit in all_visits:
        visit_id = visit.get('visit_occurrence_id')
        if visit_id in visit_map:
            # Duplicate found - keep 'Visit' over 'Visit_encompassingEncounter'
            existing = visit_map[visit_id]
            if existing.get('cfg_name') == 'Visit_encompassingEncounter' and visit.get('cfg_name') == 'Visit':
                visit_map[visit_id] = visit  # Replace with 'Visit'
            # Otherwise keep existing
        else:
            visit_map[visit_id] = visit

    deduplicated_visits = list(visit_map.values())
    logger.info(f"After deduplication: {len(deduplicated_visits)} unique visits (removed {len(all_visits) - len(deduplicated_visits)} duplicates)")

    # Step 3: Process hierarchy on deduplicated visits
    # Identify potential parent visits (inpatient with valid duration)
    parent_visits = identify_inpatient_parents(deduplicated_visits)
    logger.info(f"Identified {len(parent_visits)} potential parent visits")

    if not parent_visits:
        logger.info("No inpatient parent visits found - all visits stay as visit_occurrence")
        omop_dict['Visit'] = deduplicated_visits
        if 'Visit_encompassingEncounter' in omop_dict:
            del omop_dict['Visit_encompassingEncounter']
        return omop_dict

    # Build parent mapping for each visit
    visit_to_parent_map = {}
    nested_visit_ids = set()
    visit_lookup = {v.get('visit_occurrence_id'): v for v in deduplicated_visits}

    for visit in deduplicated_visits:
        visit_id = visit.get('visit_occurrence_id')

        # Find the most specific parent for this visit
        most_specific_parent_id = find_most_specific_parent(visit, parent_visits)

        if most_specific_parent_id is not None:
            visit_to_parent_map[visit_id] = most_specific_parent_id
            nested_visit_ids.add(visit_id)
            logger.debug(f"Visit {visit_id} will be nested under parent {most_specific_parent_id}")

    logger.info(f"Found {len(nested_visit_ids)} visits to be nested")

    # Step 4: Identify multi-level nesting (parents that are themselves nested)
    parent_ids = {p.get('visit_occurrence_id') for p in parent_visits}
    nested_parent_ids = parent_ids & nested_visit_ids

    logger.info(f"Found {len(parent_ids - nested_parent_ids)} top-level parents and {len(nested_parent_ids)} nested parents")

    # Step 5: Create visit_detail records for all nested visits
    visit_detail_list = []
    for visit_id in nested_visit_ids:
        visit = visit_lookup.get(visit_id)
        if visit:
            immediate_parent_id = visit_to_parent_map[visit_id]

            # Find top-level visit_occurrence_id by traversing up hierarchy
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

    # Step 6: Create final visit_occurrence (remove nested children, keep only top-level)
    final_visit_occurrence = [v for v in deduplicated_visits if v.get('visit_occurrence_id') not in nested_visit_ids]
    logger.info(f"Final visit_occurrence contains {len(final_visit_occurrence)} records")

    # Step 7: Update omop_dict with results
    omop_dict['Visit'] = final_visit_occurrence

    # Remove 'Visit_encompassingEncounter' config (merged into 'Visit')
    if 'Visit_encompassingEncounter' in omop_dict:
        del omop_dict['Visit_encompassingEncounter']
        logger.info("Removed 'Visit_encompassingEncounter' config (merged into 'Visit')")

    # Create 'VISITDETAIL_visit_occurrence' config with visit_detail records
    if visit_detail_list:
        omop_dict['VISITDETAIL_visit_occurrence'] = visit_detail_list
        logger.info(f"Created 'VISITDETAIL_visit_occurrence' config with {len(visit_detail_list)} records")

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
def assign_visit_occurrence_ids_to_events(data_dict: dict[str,
                                                             list[dict[str, None | str | float | int | int64 | datetime.datetime | datetime.date] | None] | None]):
    # data_dict is a dictionary of config_names to a list of record-dicts
    # Only Measurement, Observation, Condition, Procedure, Drug, and Device participate in Visit FK reconciliation
    # Visit hierarchy processing (reclassify_nested_visit_occurrences_as_detail) merges both
    # Visit and Visit_encompassingEncounter configs, so by this point all visits (visit_occurrence) are in 'Visit'.
    VISIT_CFG_NAME = 'Visit'

    # Use ddl.py mappings as single source of truth
    config_to_domain_map = DDL.config_to_domain_name_dict

    # Only process configs that exist in data_dict and have clinical events needing visit reconciliation
    for cfg_name, domain_name in config_to_domain_map.items():
        if cfg_name in data_dict and data_dict[cfg_name]:
            if domain_name in ['Measurement', 'Observation', 'Condition', 'Procedure', 'Drug', 'Device']:
                reconcile_visit_FK_with_specific_domain(domain_name, data_dict[cfg_name], data_dict[VISIT_CFG_NAME])

    for cfg_name, domain_name in config_to_domain_map.items():
        if cfg_name in data_dict and data_dict[cfg_name]:
            if domain_name in ['Measurement', 'Observation', 'Condition', 'Procedure', 'Drug', 'Device']:
                for record in data_dict[cfg_name]:
                    if '__visit_candidates' in record:
                        del record['__visit_candidates']


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
        data_dict: Dictionary with config_name → list of records (omop_dict)
    """
    VISIT_DETAIL_CFG_NAME = 'VISITDETAIL_visit_occurrence'

    if VISIT_DETAIL_CFG_NAME not in data_dict or not data_dict[VISIT_DETAIL_CFG_NAME]:
        logger.info("No visit_detail records found - skipping visit_detail FK reconciliation")
        return

    visit_detail_list = data_dict[VISIT_DETAIL_CFG_NAME]
    logger.info(f"Processing visit_detail FK reconciliation for {len(visit_detail_list)} visit_detail records")

    # Use ddl.py mappings as single source of truth
    config_to_domain_map = DDL.config_to_domain_name_dict

    # Only process configs that exist and have clinical events needing visit_detail reconciliation
    for cfg_name, domain_name in config_to_domain_map.items():
        if cfg_name in data_dict and data_dict[cfg_name]:
            if domain_name in ['Measurement', 'Observation', 'Condition', 'Procedure', 'Drug', 'Device']:
                reconcile_visit_detail_FK_with_specific_domain(domain_name, data_dict[cfg_name], visit_detail_list)


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
        # Strip timezone to allow subtraction
        start_datetime = strip_tz(start_datetime)
        end_datetime = strip_tz(end_datetime)
        delta = end_datetime - start_datetime
        return delta.total_seconds() / 86400  # Convert to days
    elif start_date and end_date:
        delta = end_date - start_date
        return float(delta.days)

    return 0.0
