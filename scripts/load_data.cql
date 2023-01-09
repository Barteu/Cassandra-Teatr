USE theatre;

-- Performances

INSERT INTO performances (title, start_date, end_date, performance_id) 
VALUES ('Romeo and Juliet', '2023-01-08 17:00:00','2023-01-08 18:30:00', c105a644-903d-11ed-a1eb-0242ac120002);

INSERT INTO performances (title, start_date, end_date, performance_id) 
VALUES ('Romeo and Juliet', '2023-01-20 17:00:00','2023-01-20 18:30:00', c105a644-903d-11ed-a1eb-0242ac120003);

INSERT INTO performances (title, start_date, end_date, performance_id) 
VALUES ('Romeo and Juliet', '2023-01-21 17:00:00','2023-01-21 18:30:00', c105a644-903d-11ed-a1eb-0242ac120004);


-- Performance Seats

INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120002,0, 'Romeo and Juliet', '2023-01-08 17:00:00', null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120002,1, null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120002,2, 'john.doe@email.com');



INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120003,0, 'Romeo and Juliet', '2023-01-20 17:00:00', null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120003,1, null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120003,2, null);


INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120004,0, 'Romeo and Juliet', '2023-01-21 17:00:00', null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120004,1, null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120004,2, null);

-- Users

INSERT INTO users (email, first_name, last_name)
VALUES ('john.doe@email.com', 'John','Doe');

INSERT INTO users (email, first_name, last_name)
VALUES ('cezary@theatre.com', 'Cezary','Kowalski');


-- Tickets

INSERT INTO tickets (email, performance_id, seat_number, first_name, last_name)
VALUES ('john.doe@email.com', c105a644-903d-11ed-a1eb-0242ac120002 , 2, 'John','Doe');








