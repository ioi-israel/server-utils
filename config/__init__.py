#!/usr/bin/env python2

"""
Server configuration, constants and functionality.
"""

from datetime import datetime, timedelta
import os
import yaml

# Load config.yaml from this directory.
_MODULE_PATH = os.path.abspath(__file__)
_CONFIG_DIR = os.path.dirname(_MODULE_PATH)
_CONFIG_FILE_PATH = os.path.join(_CONFIG_DIR, "config.yaml")
with open(_CONFIG_FILE_PATH) as stream:
    _CONFIG = yaml.safe_load(stream)

# Expose paths.
CLONE_DIR = _CONFIG["paths"]["clone_dir"]
REQUESTS_DIR = _CONFIG["paths"]["requests_dir"]
LOCK_FILE = _CONFIG["paths"]["lock_file"]

# Expose constants.
LOCK_LIFETIME = timedelta(seconds=_CONFIG["locks"]["lifetime"])
LOCK_TIMEOUT = timedelta(seconds=_CONFIG["locks"]["timeout"])
REQUEST_COOLING = _CONFIG["requests"]["cooling_period"]
ACTIVE_CONTESTS = _CONFIG["requests"]["active_contests"]
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def time_from_str(time_str, str_format=TIME_FORMAT):
    """
    Convert the given string to a datetime object.
    By default, accepts the time format constant above, e.g.:
    "2000-01-01 10:00:00"
    """
    return datetime.strptime(time_str, str_format)
