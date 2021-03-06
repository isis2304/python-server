# -*- coding: utf-8 -*-

"""
logic module
=========

Provides:
    1. Single point of entry to SQL transactions and connections
    2. Execution of SQL statements
    3. Execution of distributed transactions

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
import mq
import dtm

__version__ = '1.0.0'
__all__ = ["tm", "mq"]

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
