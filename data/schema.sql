CREATE TABLE WEEKDAYS
(
    num INTEGER CONSTRAINT weekdays_pk PRIMARY KEY,
    name NVARCHAR2(80) NOT NULL
);

CREATE TABLE COUNTRIES
(
    iso_code CHAR(2) CONSTRAINT countries_pk PRIMARY KEY,
    name NVARCHAR2(80) NOT NULL
);

CREATE TABLE AIRLINES
(
    iata_code CHAR(2) CONSTRAINT airlines_pk PRIMARY KEY,
    icao_code CHAR(3) NULL,
    iso_country CHAR(2) NOT NULL,
    name NVARCHAR2(80) NOT NULL,
    FOREIGN KEY (iso_country) REFERENCES COUNTRIES(iso_code)
);

CREATE TABLE AIRPORTS
(
    iata_code CHAR(3) CONSTRAINT airports_pk PRIMARY KEY,
    icao_code CHAR(4),
    iso_country CHAR(2) NOT NULL,
    city NVARCHAR2(80) NOT NULL,
    timezone NVARCHAR2(80) NULL,
    utc_offset INTEGER NOT NULL,
    name NVARCHAR2(80) NOT NULL,
    full_name NVARCHAR2(80) NOT NULL,
    altitude FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,

    FOREIGN KEY (iso_country) REFERENCES COUNTRIES(iso_code)

);

CREATE TABLE FLIGHTS
(
	  id INTEGER CONSTRAINT flights_pk PRIMARY KEY,
	  flight_number NVARCHAR2(80) NOT NULL,
	  airline CHAR(2) NOT NULL,
    departure_airport CHAR(3) NOT NULL,
    arrival_airport CHAR(3) NOT NULL,
    elapsed_time INTERVAL DAY(3) TO SECOND NOT NULL,
    departure_day INTEGER NOT NULL,
    arrival_day INTEGER NOT NULL,
    departure_time TIMESTAMP WITH TIME ZONE NOT NULL,
    arrival_time TIMESTAMP WITH TIME ZONE NOT NULL,
    departure_terminal NVARCHAR2(80) NULL,
    arrival_terminal NVARCHAR2(80) NULL,
    base_fare NUMBER(19,4) NOT NULL,
    base_currency CHAR(3) NOT NULL,
    taxes NUMBER(19,4) NOT NULL,
    taxes_currency CHAR(3) NOT NULL,
    total_price NUMBER(19,4) NOT NULL,
    price_currency CHAR(3) NOT NULL,
    aircraft NVARCHAR2(80) NULL,

    FOREIGN KEY (airline) REFERENCES AIRLINES(iata_code),
    FOREIGN KEY (departure_airport) REFERENCES AIRPORTS(iata_code),
    FOREIGN KEY (arrival_airport) REFERENCES AIRPORTS(iata_code),
    FOREIGN KEY (departure_day) REFERENCES WEEKDAYS(num),
    FOREIGN KEY (arrival_day) REFERENCES WEEKDAYS(num)

);

CREATE INDEX flight_idx on FLIGHTS(departure_airport, arrival_airport, departure_day); 

CREATE TABLE FLIGHT_STOPS
(
     flight INTEGER NOT NULL,
     stop_airport CHAR(3) NOT NULL,
     arrival_time TIMESTAMP WITH TIME ZONE NOT NULL,
     departure_time TIMESTAMP WITH TIME ZONE NOT NULL,
     flight_time INTERVAL DAY(3) TO SECOND NOT NULL,
     stop_duration INTERVAL DAY(3) TO SECOND NOT NULL,

     CONSTRAINT stops_pk PRIMARY KEY (flight, stop_airport),
     FOREIGN KEY (flight) REFERENCES FLIGHTS(id),
     FOREIGN KEY (stop_airport) REFERENCES AIRPORTS(iata_code)
);


CREATE TABLE PERSONS
(
     id INTEGER CONSTRAINT person_pk PRIMARY KEY,
     name NVARCHAR2(80) NOT NULL,
     birth_day DATE NOT NULL,
     second_name NVARCHAR2(80) NULL,
     last_name NVARCHAR2(80) NOT NULL,
     country CHAR(2) NOT NULL,
     city NVARCHAR2(80) NOT NULL,
     address NVARCHAR2(80) NOT NULL,
     zip NVARCHAR2(80) NULL,
     phone NVARCHAR2(80) NOT NULL,
     email NVARCHAR2(80) NOT NULL,

     FOREIGN KEY (country) REFERENCES COUNTRIES(iso_code)
);

CREATE TABLE RESERVATIONS
(
	 reservation_code NVARCHAR2(80) CONSTRAINT reservation_pk PRIMARY KEY,
	 person INTEGER NOT NULL,
	 confirmed CHAR(1) NOT NULL,

	 FOREIGN KEY (person) REFERENCES PERSONS(id)
);

CREATE TABLE FLIGHT_PASSENGERS
(
	   reservation_code NVARCHAR2(80) NOT NULL,
     flight_id INTEGER NOT NULL,
     passenger_id INTEGER NOT NULL,
     seat_number CHAR(2) NOT NULL,
     checked_bags INTEGER NOT NULL,
     vegetarian_meal CHAR(1) NOT NULL,
     special_assistance CHAR(1) NOT NULL,

     CONSTRAINT passengers_pk PRIMARY KEY (reservation_code, flight_id, passenger_id),
     FOREIGN KEY (reservation_code) REFERENCES RESERVATIONS(reservation_code),
     FOREIGN KEY (flight_id) REFERENCES FLIGHTS(id),
     FOREIGN KEY (passenger_id) REFERENCES PERSONS(id)
);

