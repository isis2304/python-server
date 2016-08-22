# -*- coding: iso-8859-15 -*-

import os
import sys
import time
import tornado.web
import tornado.escape

def get_query_size(cur, query):
    stmt = 'SELECT COUNT(*) as total FROM ('+query+') bind'
    print stmt
    cur.execute(stmt)
    values = cur.fetchall()
    return values[0][0]

def obj_conv(cur, values):
    t = time.time()
    col_names = [col.name for col in cur.description]
    values = map(lambda x: dict(zip(col_names, x)), values)
    print "Conversion time: %g" % (time.time()-t)
    return values