USE theatre;

-- Users

INSERT INTO users (email, first_name, last_name)
VALUES ('john@email.com', 'John','Doe');

INSERT INTO users (email, first_name, last_name)
VALUES ('kate@email.com', 'Kate','Bardot');

INSERT INTO users (email, first_name, last_name)
VALUES ('cezary@theatre.com', 'Cezary','Kowalski');

-- Performances

INSERT INTO performances (p_date, start_date, title, end_date, performance_id) 
VALUES ('2023-01-20','2023-01-20 13:00:00', 'romeo and juliet', '2023-01-20 14:30:00', c105a644-903d-11ed-a1eb-0242ac120002);

INSERT INTO performances (p_date, start_date, title, end_date, performance_id) 
VALUES ('2023-01-20', '2023-01-20 17:00:00', 'romeo and juliet','2023-01-20 18:30:00', c105a644-903d-11ed-a1eb-0242ac120003);

INSERT INTO performances (p_date, start_date, title, end_date, performance_id) 
VALUES ('2023-01-21', '2023-01-21 17:00:00', 'romeo and juliet','2023-01-21 18:30:00', c105a644-903d-11ed-a1eb-0242ac120004);

-- Performance Seats

INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120002,1, 'romeo and juliet', '2023-01-20 13:00:00', null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,2, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,3, 'john@email.com');
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,4, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,5, 'kate@email.com');
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,6, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,7, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,8, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,9, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,10, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,11, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,12, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,13, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,14, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,15, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,16, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,17, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,18, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,19, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,20, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,21, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,22, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,23, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,24, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,25, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,26, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,27, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,28, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,29, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,30, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,31, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,32, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,33, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,34, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,35, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,36, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,37, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,38, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,39, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,40, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,41, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,42, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,43, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,44, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,45, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,46, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,47, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,48, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,49, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120002,50, null);


INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120003,1, 'romeo and juliet', '2023-01-20 17:00:00', null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,2, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,3, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,4, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,5, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,6, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,7, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,8, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,9, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,10, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,11, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,12, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,13, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,14, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,15, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,16, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,17, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,18, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,19, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,20, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,21, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,22, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,23, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,24, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,25, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,26, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,27, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,28, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,29, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,30, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,31, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,32, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,33, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,34, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,35, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,36, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,37, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,38, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,39, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,40, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,41, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,42, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,43, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,44, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,45, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,46, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,47, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,48, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,49, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120003,50, null);


INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by)
VALUES (c105a644-903d-11ed-a1eb-0242ac120004,1, 'romeo and juliet', '2023-01-21 17:00:00', null);

INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,2, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,3, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,4, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,5, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,6, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,7, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,8, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,9, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,10, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,11, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,12, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,13, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,14, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,15, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,16, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,17, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,18, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,19, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,20, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,21, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,22, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,23, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,24, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,25, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,26, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,27, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,28, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,29, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,30, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,31, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,32, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,33, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,34, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,35, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,36, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,37, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,38, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,39, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,40, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,41, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,42, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,43, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,44, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,45, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,46, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,47, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,48, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,49, null);
INSERT INTO performance_seats (performance_id, seat_number, taken_by) VALUES (c105a644-903d-11ed-a1eb-0242ac120004,50, null);


-- Tickets

INSERT INTO tickets (email, performance_id, seat_number, first_name, last_name)
VALUES ('john.doe@email.com', c105a644-903d-11ed-a1eb-0242ac120002 , 3, 'John','Doe');

INSERT INTO tickets (email, performance_id, seat_number, first_name, last_name)
VALUES ('kate@email.com', c105a644-903d-11ed-a1eb-0242ac120002 , 5, 'Kate','Bardot');


