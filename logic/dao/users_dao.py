# -*- coding: iso-8859-15 -*-

import os
import sys
import time
import datetime
from decorators import returnobj

def register_user(cur, user): 
    stmt_1 = u'SELECT COUNT(*) FROM PERSONS'
    stmt_2 = u"""INSERT INTO PERSONS(id, name, birth_day, second_name, 
    last_name, country, city, address, zip, phone, email) VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    keys = ["id", "name", "birth_day", "second_name", "last_name", 
            "country", "city", "address", "zip", "phone", "email"]

    cur.execute(stmt_1)
    _id = cur.fetchall()[0][0]
    user.id = _id+1

    values = user.sql(keys)
    print(values)
    cur.execute(stmt_2, values)
    print(cur.query)
    return user



