from cassandra.cluster import Cluster
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
        self.prepare_statements()

    def finalize(self):
        try:
            self.cluster.shutdown()
        except Exception as e:
            print("Could not close existing cluster. ", e)
            raise e

    def prepare_statements(self):
        try:
            self.insert_performance_stmt = self.session.prepare("INSERT INTO performances (title, start_date, end_date, performance_id) VALUES (?,?,?,?) IF NOT EXISTS")
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
            row = self.session.execute("SELECT * from users WHERE email=%s;",[email]).one()
            if row:
                return row
            return False
        except Exception as e:
            print(e)
        return None

    def insert_user(self, email, first_name, last_name):
        try:
            rows = self.session.execute("SELECT email from users WHERE email=%s",(email))
            if rows.one():
                return False
            self.session.execute("INSERT INTO users (email, first_name, last_name) VALUES (%s,%s,%s);",(email,first_name,last_name))
            return True
        except Exception as e:
            print(e)
        return None


    def select_current_performances(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM performances where start_date>toTimestamp(now()) ALLOW FILTERING;')
        except Exception as e:
            print(e)
        return rows

    def insert_performance(self, title, start_date, end_date, uuid):
        try:
            result = self.session.execute(self.insert_performance_stmt,[title, start_date, end_date, uuid])
            if result.one().applied:
                return True
        except Exception as e:
            print("Could not insert performance. ", e)
        return False
