# -*- coding: utf-8 -*-

"""
logic module
=========

Provides:
    1. Single point of entry to SQL transactions and connections
    2. Execution of SQL statements

How to use the documentation
----------------------------
Documentation is available in one form: docstrings provided
with the code

Copyright (c) 2016, Edgar A. Margffoy.
MIT, see LICENSE for more details.
"""

import tm
import os
import sys
import dao

__version__ = '1.0.0'

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
