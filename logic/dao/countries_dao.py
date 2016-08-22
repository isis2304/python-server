# -*- coding: iso-8859-15 -*-

import os
import sys
import tornado.web
import tornado.escape
from decorators import returnobj

@returnobj
def get_countries(cur):
    stmt = 'SELECT * FROM COUNTRIES'
    cur.execute(stmt)
    values = cur.fetchall()
    return cur, values

@returnobj
def get_country(cur, iso_code):
    stmt = 'SELECT * FROM COUNTRIES WHERE iso_code = %s'
    cur.execute(stmt, (iso_code,))
    value = cur.fetchall()
    return cur, value
