CREATE KEYSPACE IF NOT EXISTS theatre
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE Theatre;

CREATE TABLE performance_seats (
  performance_id uuid ,
  seat_number int,
  title varchar, -- in lowercase
  start_date timestamp,
  taken_by varchar, -- user's email
  PRIMARY KEY (performance_id, seat_number)
);

CREATE TABLE performances (
  p_date date,
  title varchar, -- in lowercase
  start_date timestamp,
  end_date timestamp,
  performance_id uuid,
  PRIMARY KEY (p_date, start_date, title)
)
WITH CLUSTERING ORDER BY (start_date ASC);

CREATE TABLE tickets (
  email varchar, 
  performance_id uuid,
  seat_number int,
  first_name varchar,
  last_name varchar,
  PRIMARY KEY (email, performance_id, seat_number)
);

CREATE TABLE users (
  email varchar, 
  first_name varchar,
  last_name varchar,
  PRIMARY KEY(email)
);
