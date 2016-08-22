# -*- coding: utf-8 -*-

"""
dao module
=========

Provides:
    1. Data Access Object definitions for different relations
    2. Execution of SQL statements

How to use the documentation
----------------------------
Documentation is available in one form: docstrings provided
with the code

Copyright (c) 2016, Edgar A. Margffoy.
MIT, see LICENSE for more details.
"""

import os
import sys
import utils
import flights_dao
import countries_dao


__version__ = '1.0.0'

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
