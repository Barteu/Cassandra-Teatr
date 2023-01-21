USE theatre;

-- Users

INSERT INTO users (email, first_name, last_name)
VALUES ('john@email.com', 'John','Doe');

INSERT INTO users (email, first_name, last_name)
VALUES ('kate@email.com', 'Kate','Bardot');

INSERT INTO users (email, first_name, last_name)
VALUES ('cezary@theatre.com', 'Cezary','Kowalski');

-- Performances

INSERT INTO performances (p_date, start_date, title, end_date, performance_id, number_of_seats) 
VALUES ('2023-01-20','2023-01-20 13:00:00', 'romeo and juliet', '2023-01-20 14:30:00', c105a644-903d-11ed-a1eb-0242ac120002, 50);

INSERT INTO performances (p_date, start_date, title, end_date, performance_id, number_of_seats) 
VALUES ('2023-01-20', '2023-01-20 17:00:00', 'romeo and juliet','2023-01-20 18:30:00', c105a644-903d-11ed-a1eb-0242ac120003, 50);

INSERT INTO performances (p_date, start_date, title, end_date, performance_id, number_of_seats) 
VALUES ('2023-01-21', '2023-01-21 17:00:00', 'romeo and juliet','2023-01-21 18:30:00', c105a644-903d-11ed-a1eb-0242ac120004, 50);

-- Performance Seats

INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by) 
VALUES (c105a644-903d-11ed-a1eb-0242ac120002,3, 'romeo and juliet', '2023-01-20 13:00:00', 'john@email.com');
INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by) 
VALUES (c105a644-903d-11ed-a1eb-0242ac120002,5, 'romeo and juliet', '2023-01-20 13:00:00', 'kate@email.com');

-- Tickets

INSERT INTO tickets (email, performance_id, seat_number, first_name, last_name, buy_timestamp)
VALUES ('john.doe@email.com', c105a644-903d-11ed-a1eb-0242ac120002 , 3, 'John','Doe', '2022-02-20 13:00:00');

INSERT INTO tickets (email, performance_id, seat_number, first_name, last_name, buy_timestamp)
VALUES ('kate@email.com', c105a644-903d-11ed-a1eb-0242ac120002 , 5, 'Kate','Bardot', '2022-02-20 13:00:00');


