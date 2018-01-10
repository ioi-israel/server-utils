#!/usr/bin/env python2

"""
Server configuration, constants and functionality.
"""

import os
import yaml

# Load config.yaml from this directory.
_MODULE_PATH = os.path.abspath(__file__)
_CONFIG_DIR = os.path.dirname(_MODULE_PATH)
_CONFIG_FILE_PATH = os.path.join(_CONFIG_DIR, "config.yaml")
with open(_CONFIG_FILE_PATH) as stream:
    _CONFIG = yaml.safe_load(stream)

# Expose constants.
TASKS_DIR = _CONFIG["paths"]["tasks_dir"]
CONTESTS_DIR = _CONFIG["paths"]["contests_dir"]
USERS_FILE = _CONFIG["paths"]["users_file"]
