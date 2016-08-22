# -*- coding: iso-8859-15 -*-

import os
import sys
import time
import functools

def returnobj(func=None, add_ret=False):
    if not func:
       return functools.partial(returnobj, add_ret=add_ret)
    @functools.wraps(func)
    def obj_conv(*args, **kwargs):
        print "Additional return objects: %s" % (str(add_ret))
        if not add_ret:
           cur, values = func(*args, **kwargs)
        else:
           cur, values, r = func(*args, **kwargs)
        # print cur.description
        t = time.time()
        col_names = [col.name for col in cur.description]
        values = map(lambda x: dict(zip(col_names, map(str, x))), values)
        print "Conversion time: %g" % (time.time()-t)
        # print values
        if add_ret:
            values = (values, r)
        return values
    return obj_conv
