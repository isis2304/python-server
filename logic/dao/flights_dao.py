# -*- coding: iso-8859-15 -*-

import os
import sys
import time
import utils
import tornado.web
import tornado.escape
from decorators import returnobj
from psycopg2.extensions import AsIs

@returnobj
def get_flight_count(cur):
    stmt = 'SELECT COUNT(*) as total FROM FLIGHTS'
    cur.execute(stmt)
    value = cur.fetchall()
    return cur, value

@returnobj
def get_flights(cur, col_name, start, length, order):
    stmt = """
    SELECT * FROM FLIGHTS
    ORDER BY %s %s
    LIMIT %s OFFSET %s          
    """
    cur.execute(stmt, (AsIs(col_name), AsIs(order), length, start))
    values = cur.fetchall()
    return cur, values

# @returnobj(add_ret=True)
def get_flights_from_to_date(cur, data, col_name, start, length, order):
    stmt = """
    SELECT * FROM
    (SELECT f.id, f.flight_number, f.airline as carrier,
               air.name as airline,
               f.departure_airport as departure_airport_code,
               a.full_name as departure_airport_name,
               a.city as departure_city, f.departure_time,
               d.name as departure_day, f.total_price,
               f.arrival_airport as arrival_airport_code, f.arrival_time  
     FROM flights f, airports a, airlines air, weekdays d
     WHERE
        f.departure_airport = a.iata_code AND
        f.departure_day = d.number AND
        f.airline = air.iata_code AND
        a.city LIKE %s AND
        d.name LIKE %s) z
    INNER JOIN
    (SELECT iata_code as arrival_airport_code, 
            full_name as arrival_airport_name,
            city as arrival_city
    FROM airports
    WHERE city LIKE %s) k
    ON z.arrival_airport_code = k.arrival_airport_code
    ORDER BY %s %s
    LIMIT %s OFFSET %s
    """
    t = time.time()
    cur.execute(stmt, ('%'+data['from']+'%', '%'+data['day']+'%', 
                       '%'+data['to']+'%', AsIs(col_name),
                       AsIs(order), length, start))
    values = cur.fetchall()
    values = utils.obj_conv(cur, values)
    print "Time Elapsed: %g" % (time.time()-t)
    query = cur.query
    print query
    query = '\n'.join(query.split('\n')[0:-2])
    t = time.time()
    # print query
    total = utils.get_query_size(cur, query)
    print "Time Elapsed: %g" % (time.time()-t)
    return values, total

