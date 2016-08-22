import os
import sys
import pytz
import datetime
import cx_Oracle
import progressbar

URL = 'fn3.oracle.virtual.uniandes.edu.co'
PORT = 1521
SERV = 'prod'
USER = 'ISIS2304MO11620'
PASSWORD = 'ElPhdabCrXl9'

dsn_tns = cx_Oracle.makedsn(URL, PORT, SERV)

conn = cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)

cur = conn.cursor()

# bar = progressbar.ProgressBar(redirect_stdout=True)
# print "Now Processing: countries.csv"

# with open('contents/countries.csv') as fp:
#   lines = fp.readlines()[1:]

# for country in bar(lines):
#    info = country.strip('\n').split('|')
#    # print "Uploading: %s" % (info[1])
#    try:
#       # print (info[0], info[1])
#       cur.execute(u'INSERT INTO COUNTRIES(iso_code, name) VALUES (:iso_code, :name)', {'iso_code':info[0], 'name':info[1]})
#       conn.commit()
#    except cx_Oracle.IntegrityError:
#       conn.rollback() 

bar = progressbar.ProgressBar(redirect_stdout=True)

print "Now Processing: airlines.csv"

blacklist = []

with open('contents/airlines.csv') as fp:
    lines = fp.readlines()

airlines = {}
header = lines[0].strip('\n').split('|')
lines = lines[1:]
for airline in bar(lines):
    info = airline.strip('\n').split('|')
    airline = dict(zip(header, info))
    # print airline
    if airline['iata'] != '':
       airlines[airline['iata']] = airline
       # try:
       #   cur.execute('INSERT INTO AIRLINES(iata_code, icao_code, iso_country, name) VALUES (:iata, :icao, :iso_country, :name)', airline)
       #   conn.commit()
       # except cx_Oracle.IntegrityError:
       #   conn.rollback()

bar = progressbar.ProgressBar(redirect_stdout=True)

print "Now Processing: airports.csv"

with open('contents/airports.csv', 'rb') as fp:
    lines = fp.readlines()

lines = map(lambda x: x.strip('\n'), lines)
header = lines[0].split('|')
lines = lines[1:]

sentence = """INSERT INTO AIRPORTS(iata_code, icao_code, iso_country, city, timezone, 
                                   utc_offset, name, full_name, altitude, latitude, 
                                   longitude) 
              VALUES 
              (:iata, :icao, :iso_country, :city, :tz, :utc, :name, :full_name, :alt, :lat, :lon)"""

airports = {}
for airport in bar(lines):
    airport = dict(zip(header, airport.split('|')))
# #    print "Uploading: %s" % (airport['full_name'])
#     cur.execute(sentence, airport)
#     conn.commit()
    airports[airport['iata']] = airport

bar = progressbar.ProgressBar(redirect_stdout=True)

print "Now Processing: flight_stops.csv"

with open('contents/flight_stops.csv', 'rb') as fp:
    lines = fp.readlines()


lines = map(lambda x: x.strip('\n'), lines)
header = lines[0].split('|')
lines = lines[1:]

flight_stops = {}
for stop in bar(lines):
    stop = dict(zip(header, stop.split('|')))
    # print stop
    
    if stop['stop_airport_code'] in airports:
        stop_tz = pytz.timezone(airports[stop['stop_airport_code']]['tz'])   
        stop['arrival_time'] = stop_tz.localize(datetime.datetime.strptime(stop['arrival_time'], '%H:%M:%S'))
        stop['departure_time'] = stop_tz.localize(datetime.datetime.strptime(stop['departure_time'], '%H:%M:%S'))
        stop['elapsed_time'] = datetime.timedelta(minutes=int(stop['elapsed_time']))
        stop['stop_duration'] = datetime.timedelta(minutes=int(stop['stop_duration']))
     
        key = '%s%s%s%s%s' % (stop['operating_airline'], stop['flight_number'], stop['departure_airport_code'], stop['arrival_airport_code'], stop['day_of_week'])
#        print "Loading: %s" % (key)
        if key not in flight_stops:
           flight_stops[key] = [stop]
        else:
           flight_stops[key].append(stop) 


# print "Now Processing: flights.csv"

# # operating_airline|flight_number|departure_airport_code|arrival_airport_code|day_of_week

# flight_insertion = """INSERT INTO FLIGHTS(id, flight_number, airline, departure_airport, arrival_airport, 
#                                           elapsed_time, departure_day, arrival_day, departure_time, 
#                                           arrival_time, departure_terminal, arrival_terminal, base_fare, 
#                                           base_currency, taxes, taxes_currency, total_price, price_currency, 
#                                           aircraft) 
#                       VALUES (:ids, :flight_number, :operating_airline, :departure_airport_code, :arrival_airport_code, 
#                       :elapsed_time, :departure_day_of_week, :arrival_day_of_week, :departure_time, :arrival_time, 
#                       :departure_airport_terminal, :arrival_airport_terminal, :base_fare, :base_fare_currency, 
#                       :taxes, :taxes_currency, :total_price, :total_price_currency, :aircraft)"""

# stop_insertion = """INSERT INTO FLIGHT_STOPS(flight, stop_airport, arrival_time, 
#                                              departure_time, flight_time, stop_duration)
#                     VALUES (:ids, :stop_airport_code, :arrival_time, :departure_time, :elapsed_time, :stop_duration)"""

# bar = progressbar.ProgressBar(redirect_stdout=True)

# with open('contents/flights.csv', 'rb') as fp:
#     lines = fp.readlines()

# lines = map(lambda x: x.strip('\n'), lines)
# header = lines[0].split('|')
# lines = lines[1:]

# i = 1
# for flight in lines:
#     flight = dict(zip(header, [j if j != '' and j != 'Unknown' else None for j in flight.split('|')]))
#     # print flight
#     key = '%s%s%s%s%s' % (flight['operating_airline'], flight['flight_number'], flight['departure_airport_code'],
#                           flight['arrival_airport_code'], flight['departure_day_of_week'])
#     # print 'Uploading: %s' % (key)
#     flight['elapsed_time'] = datetime.timedelta(minutes=int(flight['elapsed_time']))
#     dep_tz = pytz.timezone(airports[flight['departure_airport_code']]['tz'])
#     arr_tz = pytz.timezone(airports[flight['arrival_airport_code']]['tz'])
#     flight['departure_time'] = dep_tz.localize(datetime.datetime.strptime(flight['departure_time'], '%H:%M:%S'))
#     flight['arrival_time'] = arr_tz.localize(datetime.datetime.strptime(flight['arrival_time'], '%H:%M:%S')) 
#     flight['ids'] = i
#     # try:
#     if flight['flight_number'] is not None and flight['operating_airline'] in airlines:
#        print flight
#        cur.execute(flight_insertion, flight)
#        conn.commit()
#        if key in flight_stops:
#           for stop in flight_stops[key]:
#               try:
#                  stop['ids'] = i
#                  cur.execute(stop_insertion, stop)
#                  conn.commit()
#               except cx_Oracle.IntegrityError:
#                  conn.rollback()
#     bar.update(i)
#     i += 1  
#     # except cx_Oracle.IntegrityError:
#     #     conn.rollback()



print "Now Processing: flights.csv"

# operating_airline|flight_number|departure_airport_code|arrival_airport_code|day_of_week

flight_insertion = """INSERT INTO FLIGHTS(id, flight_number, airline, departure_airport, arrival_airport, 
                                          elapsed_time, departure_day, arrival_day, departure_time, 
                                          arrival_time, departure_terminal, arrival_terminal, base_fare, 
                                          base_currency, taxes, taxes_currency, total_price, price_currency, 
                                          aircraft) 
                      VALUES (:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18)"""

stop_insertion = """INSERT INTO FLIGHT_STOPS(flight, stop_airport, arrival_time, 
                                             departure_time, flight_time, stop_duration)
                    VALUES (:0, :1, :2, :3, :4, :5)"""

bar = progressbar.ProgressBar()

with open('contents/flights.csv', 'rb') as fp:
    lines = fp.readlines()

lines = map(lambda x: x.strip('\n'), lines)
header = lines[0].split('|')
lines = lines[1:]

i = 1
j = 1
for flight in bar(lines):
    flight = dict(zip(header, [j if j != '' and j != 'Unknown' else None for j in flight.split('|')]))
    # print flight
    key = '%s%s%s%s%s' % (flight['operating_airline'], flight['flight_number'], flight['departure_airport_code'],
                          flight['arrival_airport_code'], flight['departure_day_of_week'])
    # print 'Uploading: %s' % (key)
    flight['elapsed_time'] = datetime.timedelta(minutes=int(flight['elapsed_time']))
    dep_tz = pytz.timezone(airports[flight['departure_airport_code']]['tz'])
    arr_tz = pytz.timezone(airports[flight['arrival_airport_code']]['tz'])
    flight['departure_time'] = dep_tz.localize(datetime.datetime.strptime(flight['departure_time'], '%H:%M:%S'))
    flight['arrival_time'] = arr_tz.localize(datetime.datetime.strptime(flight['arrival_time'], '%H:%M:%S')) 
    values = (i, flight['flight_number'], flight['operating_airline'], flight['departure_airport_code'],
              flight['arrival_airport_code'], flight['elapsed_time'], flight['departure_day_of_week'],
              flight['arrival_day_of_week'], flight['departure_time'], flight['arrival_time'],
              flight['departure_airport_terminal'], flight['arrival_airport_terminal'],
              flight['base_fare'], flight['base_fare_currency'], flight['taxes'],
              flight['taxes_currency'], flight['total_price'], flight['total_price_currency'],
              flight['aircraft'])
    # try:
    # bar2 = progressbar.ProgressBar(redirect_stdout=True)
    if flight['flight_number'] is not None and flight['operating_airline'] in airlines:
       # print values
       cur.execute(flight_insertion, values)
       conn.commit()
       if key in flight_stops:
          for stop in flight_stops[key]:
              try:
                 values = (i, stop['stop_airport_code'], stop['arrival_time'],
                           stop['departure_time'], stop['elapsed_time'],
                           stop['stop_duration'])
                 cur.execute(stop_insertion, values)
                 conn.commit()
              except cx_Oracle.IntegrityError:
                 conn.rollback()
              except cx_Oracle.DatabaseError:
                 print values
                 conn.rollback()
    # bar.update(i)
    i += 1  
