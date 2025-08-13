
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

# NOTE: The global dictionaries and their setters/getters
# have been moved to value_transformations.py
