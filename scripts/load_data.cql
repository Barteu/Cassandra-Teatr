USE theatre;

-- Performances

INSERT INTO performances (title, start_date, end_date, performance_id) 
VALUES ('Romeo and Juliet', '2023-01-08 17:00:00','2023-01-08 18:30:00', 1);

-- Performance Seats

INSERT INTO performance_seats (performance_id, seat_number, title, start_date, is_taken)
VALUES (1,0, 'Romeo and Juliet', '2023-01-08 17:00:00', false);

INSERT INTO performance_seats (performance_id, seat_number, is_taken)
VALUES (1,1, false);

INSERT INTO performance_seats (performance_id, seat_number, is_taken)
VALUES (1,2, true);

-- Users

INSERT INTO users (email, first_name, last_name)
VALUES ('john.doe@email.com', 'John','Doe');

-- Tickets

INSERT INTO tickets (email, performance_id, seat_number, first_name, last_name)
VALUES ('john.doe@email.com', 1, 2, 'John','Doe');








