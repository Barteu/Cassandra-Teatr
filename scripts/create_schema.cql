CREATE KEYSPACE IF NOT EXISTS theatre
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

USE Theatre;

CREATE TABLE performance_seats (
  performance_id uuid ,
  seat_number int,
  title varchar, -- in lowercase
  start_date timestamp,
  taken_by varchar, -- user's email
  PRIMARY KEY (performance_id, seat_number, taken_by)
);

CREATE TABLE performances (
  p_date date,
  title varchar, -- in lowercase
  start_date timestamp,
  end_date timestamp,
  performance_id uuid,
  number_of_seats int,
  PRIMARY KEY (p_date, start_date, title)
);

CREATE TABLE tickets (
  email varchar, 
  performance_id uuid,
  seat_number int,
  first_name varchar,
  last_name varchar,
  buy_timestamp timestamp,
  PRIMARY KEY (email, buy_timestamp, performance_id, seat_number)
);

CREATE TABLE users (
  email varchar, 
  first_name varchar,
  last_name varchar,
  PRIMARY KEY(email)
);
