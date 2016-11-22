# -*- coding: utf-8 -*-

"""
mq module
=========

Provides:
    1. RabbitMQ message listeners
    2. Execution of distributed transactions

How to use the documentation
----------------------------
Documentation is available in one form: docstrings provided
with the code

Copyright (c) 2016, Edgar A. Margffoy.
MIT, see LICENSE for more details.
"""

import os
import sys
import videos_remote

__version__ = '1.0.0'
__all__ = ["videos_remote"]

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
