#!/usr/bin/env python2

"""
Server configuration, constants and functionality.
"""

from datetime import datetime
import os
import yaml

# Load config.yaml from this directory.
_MODULE_PATH = os.path.abspath(__file__)
_CONFIG_DIR = os.path.dirname(_MODULE_PATH)
_CONFIG_FILE_PATH = os.path.join(_CONFIG_DIR, "config.yaml")
with open(_CONFIG_FILE_PATH) as stream:
    _CONFIG = yaml.safe_load(stream)

# Expose paths.
TASKS_DIR = _CONFIG["paths"]["tasks_dir"]
CONTESTS_DIR = _CONFIG["paths"]["contests_dir"]
USERS_FILE = _CONFIG["paths"]["users_file"]
REQUESTS_DIR = _CONFIG["paths"]["requests_dir"]

# Expose constants.
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def time_from_str(time_str, str_format=TIME_FORMAT):
    """
    Convert the given string to a datetime object.
    By default, accepts the time format constant above, e.g.:
    "2000-01-01 10:00"
    """
    return datetime.strptime(time_str, str_format)
