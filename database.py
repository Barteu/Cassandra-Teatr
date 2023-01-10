from cassandra.cluster import Cluster
from cassandra.cqlengine.query import BatchStatement
from cassandra import ConsistencyLevel

import time

class Database():
        
    def __init__(self, addresses=['0.0.0.0'], port=9042):
        self.cluster = Cluster(addresses, port)
        try:
            self.session = self.cluster.connect('store', wait_for_all_pools=True)
            self.session.execute('USE Theatre')
        except Exception as e:
            print("Could not connect to the cluster. ", e)
            raise e

        self.insert_performance_stmt = None
        self.insert_user_stmt = None
        self.select_email_stmt = None
        self.select_user_stmt = None
        self.insert_performance_seat_stmt = None
        self.select_performances_by_dates_stmt = None
        self.select_performances_by_dates_and_title_stmt = None

        self.prepare_statements()

    def finalize(self):
        try:
            self.cluster.shutdown()
        except Exception as e:
            print("Could not close existing cluster. ", e)
            raise e

    def prepare_statements(self):
        try:
            self.insert_performance_stmt = self.session.prepare("INSERT INTO performances (p_date, start_date, title, end_date, performance_id) VALUES (?, ?,?,?,?) IF NOT EXISTS")
            self.insert_user_stmt =  self.session.prepare("INSERT INTO users (email, first_name, last_name) VALUES (?,?,?);")
            self.select_email_stmt = self.session.prepare("SELECT email from users WHERE email=?;")
            self.select_user_stmt = self.session.prepare("SELECT * from users WHERE email=?;")
            self.select_performances_by_dates_stmt = self.session.prepare("SELECT * FROM performances where p_date in ?;")
            self.insert_performance_seat_stmt = self.session.prepare("INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by) VALUES (?,?,?,?,?);")
        except Exception as e:
            print("Could not prepare statements. ", e)
            raise e

    def select_all_performances(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM performances;') 
        except Exception as e:
            print(e)
        return rows

    def select_all_performance_seats(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM performance_seats;')
        except Exception as e:
            print(e)
        return rows

    def select_all_tickets(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM tickets;')
        except Exception as e:
            print(e)
        return rows

    def select_all_users(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM users;')
            return rows
        except Exception as e:
            print(e)
        return rows


    def select_user(self, email):
        try:
            row = self.session.execute(self.select_user_stmt,[email]).one()
            if row:
                return row
            return False
        except Exception as e:
            print(e)
        return None

    def insert_user(self, email, first_name, last_name):
        try:
            rows = self.session.execute(self.select_email_stmt,[email])
            if rows.one():
                return False
            self.session.execute(self.insert_user_stmt,[email,first_name,last_name])
            return True
        except Exception as e:
            print(e)
        return None
    
    def insert_performance(self, p_date,start_date, title, end_date, uuid):
        try:
            result = self.session.execute(self.insert_performance_stmt,[p_date, start_date,title, end_date, uuid])
            if result.one().applied:
                return True
        except Exception as e:
            print("Could not insert performance. ", e)
        return False

    def insert_performance_seat(self, performance_id, seat_number, title=None, start_date=None, taken_by = None):
        try:
            result = self.session.execute(self.insert_performance_seat_stmt, [performance_id, seat_number, title, start_date, taken_by])
            if result.one().applied:
                return True
        except Exception as e:
            print("Could not insert performance seat. ", e)
        return False

    def insert_performance_seats_batch(self, performance_id, seat_numbers, titles, start_dates, taken_by):
        try:
            batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
            for i in range(len(seat_numbers)):
                 batch.add(self.insert_performance_seat_stmt,[performance_id, 
                                                              seat_numbers[i], 
                                                              titles[i] if len(titles)>i else None,
                                                              start_dates[i] if len(start_dates)>i else None,
                                                              taken_by[i] if len(taken_by)>i else None])

            self.session.execute(batch)
            return True
        except Exception as e:
            print("Could not batch insert performance seats. ", e)
        return False

    def select_performances_by_dates(self, dates):
        rows = []
        try:
            rows = self.session.execute(self.select_performances_by_dates_stmt,[dates])
        except Exception as e:
            print(e)
        return rows


