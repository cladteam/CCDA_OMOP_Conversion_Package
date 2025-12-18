import subprocess

import logging
import sys
import os
import importlib.util
from functools import reduce
from typing import Dict, Any

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

METADATA_DIR = os.path.dirname(__file__)


# copied get_branch() from master branch, restored on Sep 17, responding to Chris' comment on GitHub #474
def get_branch():
    """
        This code attempts to use git to get a branch name.
        It will only apply user mappings if it can verify it is not working in a main 
        or master branch. If git fails, it assumes master and doesn't apply them.

        Suggestions to use an environment variable FOUNDRY_BRANCH_NAME fail because the variable isn't set.
    """
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            stdout=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None
    except FileNotFoundError:
        return None
    except Exception:
        return None


def custom_sort_key(filename):
    """
    Returns a tuple for sorting based on a multi-level priority:
    Location, care_site, provider, person,  from Sep 26 with Chris R discussion in Slack and #474
    """
    # Group 0: Highest priority
    if filename == 'location.py' or filename.startswith("LOCATION"):
        return (0, filename)
    # Group 1
    elif filename.startswith('care_site') or filename.startswith('CARE_SITE'):
        return (1, filename)
    # Group 2
    elif filename.startswith('provider') or filename.startswith('PROVIDER'):
        return (2, filename)
    # Group 3
    elif filename == 'person.py' or filename.startswith('PERSON'):
        return (3, filename)
    # Group 4: visit
    elif filename.startswith('visit') or filename.startswith('VISIT'):
        return (4, filename)
    else:
        return (5, filename)


def discover_and_sort_metadata() -> Dict[str, Any]:
    """
    Discovers all metadata files, sorts them with a custom priority,
    and merges their 'metadata' dictionaries into one.
    """
    metadata_dicts = []
    if not os.path.isdir(METADATA_DIR):
        logging.error(f"Metadata directory not found at: {METADATA_DIR}")
        return {}
            
    files_to_skip = ['__init__.py', 'test.py', 'ddl.py', 'util.py', 
        'test' # (though a dir, still needs to be skipped, getting an error about test.py?
    ]
    filenames = os.listdir(METADATA_DIR)
    filenames.sort(key=custom_sort_key)
    for filename in filenames:
        if filename.endswith('.py') and (filename not in files_to_skip):
            module_name = filename[:-3]
            file_path = os.path.join(METADATA_DIR, filename)
            try:
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                module = importlib.util.module_from_spec(spec)
                if spec is None or module is None:
                    logging.warning(f"Could not load spec for module: {module_name} for file: {file_path}")
                    continue
                spec.loader.exec_module(module)
                if hasattr(module, 'metadata'):
                    metadata_dicts.append(module.metadata)
                else:
                    logging.warning(f"Module '{module_name}' for {file_path} does not contain a 'metadata' dictionary.")
            except Exception as e:
                logging.error(f"metadata/__init__.py discover_and_sort_metadata() Failed to import metadata (not the folder, but imports stuff) from file:'{filename}' in dir: {METADATA_DIR} {file_path} NOT FATAL, skipped!: {e}")
    if not metadata_dicts:
        logging.error(f"metadata/__init__.py discover_and_sort_metadata()  no metadata_dicts from {METADATA_DIR}")
        return {}
    return reduce(lambda a, b: a | b, metadata_dicts)


def get_meta_dict():
    metadata = discover_and_sort_metadata()

    # Don't apply user mappings if we can't be sure we're not running in master.
    # i.e. Only apply user mappings in development branches.
    current_branch = get_branch() 
    if current_branch is not None and current_branch != 'master' and current_branch != 'main':
        try:
            from user_mappings import overlay_mappings
            metadata = discover_and_sort_metadata() | overlay_mappings
            print("iNFO: got user mappings  and overlaid them.")
        except Exception as e:
            print("iNFO: no user mappings available, nothing overlaid, using package mappings as-is.")
            print(f"    {e}")

        return metadata
    else:
        print("iNFO: it appears this might be running in a main or master branch, so any user mappings will not be applied, using package mappings as-is.")
        return metadata
