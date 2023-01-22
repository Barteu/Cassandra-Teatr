from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.cqlengine.query import BatchStatement
from cassandra import ConsistencyLevel, Timeout
from cassandra.auth import PlainTextAuthProvider
import time
import sys, os



class Database():

    def __init__(self, addresses=['0.0.0.0'], disable_prints = False, port=9042, timeout=10, connect_timeout=20):
        
        if disable_prints:
            sys.stdout = open(os.devnull, 'w')

        # Repeat query execution max num times if timeout occurs. Each repeat doubles timeout.  MIN=1
        self.NUM_EXTENDED_TIMEOUT = 6

        defaullt_profile = ExecutionProfile(
            request_timeout = timeout
        )
        profile_quorum_long = ExecutionProfile(
            request_timeout = timeout*32,
            consistency_level=ConsistencyLevel.QUORUM
        )

        ap = PlainTextAuthProvider(username='cassandra', password='cassandra')

        self.cluster = Cluster(addresses, 
                               port, 
                               auth_provider=ap,
                               connect_timeout=connect_timeout,
                               execution_profiles={**{f'profile{j}':ExecutionProfile(request_timeout = timeout*(2**j)) 
                                                   for j in [i for i in range(self.NUM_EXTENDED_TIMEOUT)]}, 
                                                   EXEC_PROFILE_DEFAULT: defaullt_profile,
                                                   'profile_quorum_long':profile_quorum_long
                                                   }
                               )
        self.addresses = addresses
        try:
            self.session = self.cluster.connect('theatre', wait_for_all_pools=False)
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
        self.select_user_tickets_stmt = None
        self.insert_user_ticket_stmt = None
        self.update_performance_seat_take_seat_stmt = None
        self.update_performance_seat_free_seat_stmt = None
        self.select_performance_seats_stmt = None 
        self.select_performance_seat_performance_info_stmt = None

        self.prepare_statements()

    def finalize(self):
        try:
            self.cluster.shutdown()
            sys.stdout = sys.__stdout__
        except Exception as e:
            print("Could not close existing cluster. ", e)
            raise e

    def prepare_statements(self):
        try:
            self.insert_performance_stmt = self.session.prepare("INSERT INTO performances (p_date, start_date, title, end_date, performance_id, number_of_seats) VALUES (?,?,?,?,?,?);")
            self.insert_performance_stmt.consistency_level = ConsistencyLevel.ONE
            
            self.insert_user_stmt =  self.session.prepare("INSERT INTO users (email, first_name, last_name) VALUES (?,?,?);")
            self.insert_user_stmt.consistency_level = ConsistencyLevel.ONE

            self.select_email_stmt = self.session.prepare("SELECT email from users WHERE email=?;")
            self.select_email_stmt.consistency_level = ConsistencyLevel.ONE

            self.select_user_stmt = self.session.prepare("SELECT * from users WHERE email=?;")
            self.select_user_stmt.consistency_level = ConsistencyLevel.ONE

            self.select_performances_by_dates_stmt = self.session.prepare("SELECT * FROM performances where p_date in ?;")
            self.select_performances_by_dates_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.select_performances_by_all_stmt = self.session.prepare("SELECT * FROM performances where p_date=? and start_date=? and title=?;")
            self.select_performances_by_all_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.insert_performance_seat_stmt = self.session.prepare("INSERT INTO performance_seats (performance_id, seat_number, title, start_date, taken_by) VALUES (?,?,?,?,?);")
            self.insert_performance_seat_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.select_taken_by_from_performance_seats_stmt = self.session.prepare("SELECT taken_by from performance_seats where performance_id=? and seat_number in ?;")
            self.select_taken_by_from_performance_seats_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.delete_performance_seat_stmt = self.session.prepare("DELETE FROM performance_seats where performance_id=? and seat_number=? and taken_by=?;")
            self.delete_performance_seat_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.select_user_tickets_stmt = self.session.prepare("SELECT performance_id, buy_timestamp, seat_number, first_name, last_name from tickets WHERE email=? and buy_timestamp>=? and buy_timestamp<=?;")
            self.select_user_tickets_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.select_user_tickets_check_stmt = self.session.prepare("SELECT count(*) as cnt from tickets WHERE email=? and buy_timestamp=? and performance_id=? and seat_number in ?;")
            self.select_user_tickets_check_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.insert_user_ticket_stmt = self.session.prepare("INSERT INTO tickets (email, buy_timestamp, performance_id, seat_number, first_name, last_name) VALUES (?,?,?,?,?,?);")
            
            self.select_performance_seats_stmt = self.session.prepare("SELECT * FROM performance_seats where performance_id=?;")
            self.select_performance_seats_stmt.consistency_level = ConsistencyLevel.QUORUM

            self.select_performance_seat_performance_info_stmt = self.session.prepare("SELECT title, start_date FROM performance_seats where performance_id=? and seat_number=?;")
            self.select_performance_seat_performance_info_stmt.consistency_level = ConsistencyLevel.ONE

        except Exception as e:
            print("Could not prepare statements. ", e)
            raise e

    def select_all_performances(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM performances;', execution_profile='profile_quorum_long') 
        except Exception as e:
            print(e)
        return rows

    def select_all_performance_seats(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM performance_seats;', execution_profile='profile_quorum_long')
        except Exception as e:
            print(e)
        return rows

    def select_all_tickets(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM tickets;', execution_profile='profile_quorum_long')
        except Exception as e:
            print(e)
        return rows

    def select_all_users(self):
        rows = []
        try:
            rows = self.session.execute('SELECT * FROM users;', execution_profile='profile_quorum_long')
            return rows
        except Exception as e:
            print(e)
        return rows

    def count_performances(self):
        try:
            result = self.session.execute('SELECT count(*) FROM performances;', execution_profile='profile_quorum_long') 
            return result.one().count
        except Exception as e:
            print(e)
        return -1

    def count_performance_seats(self):
        try:
            result = self.session.execute('SELECT count(*) FROM performance_seats;', execution_profile='profile_quorum_long')
            return result.one().count
        except Exception as e:
            print(e)
        return -1

    def count_tickets(self):
        try:
            result = self.session.execute('SELECT count(*) FROM tickets;', execution_profile='profile_quorum_long')
            return result.one().count
        except Exception as e:
            print(e)
        return -1

    def count_users(self):
        try:
            result = self.session.execute('SELECT count(*) FROM users;', execution_profile='profile_quorum_long')
            return result.one().count
        except Exception as e:
            print(e)
        return -1

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
    
    def insert_performance(self, p_date,start_date, title, end_date, uuid, number_of_seats):
        try:
            self.session.execute(self.insert_performance_stmt,[p_date, start_date,title, end_date, uuid, number_of_seats])
            result = self.session.execute(self.select_performances_by_all_stmt,[p_date, start_date, title])
            if result.one():
                return True
        except Exception as e:
            print("Could not insert performance. ", e)
        return False

    def insert_performance_seat(self, performance_id, seat_number, title=None, start_date=None, taken_by = None):
        for try_num in range(self.NUM_EXTENDED_TIMEOUT):
            try:
                result = self.session.execute(self.select_taken_by_from_performance_seats_stmt, [performance_id, [seat_number]])
                if result.one():
                    return False
                self.session.execute(self.insert_performance_seat_stmt, [performance_id, seat_number, title, start_date, taken_by])
                result = self.session.execute(self.select_taken_by_from_performance_seats_stmt, [performance_id, [seat_number]])

                try_again = False
                for r in result:
                    if r.taken_by != taken_by:
                        self.session.execute(self.delete_performance_seat_stmt, [performance_id, seat_number, taken_by])
                        try_again = True
                        break
                if try_again:
                    pass

                return True
            except Timeout as timeout_e:
                print("Timeout exception occurs. ", timeout_e)
            except Exception as e:
                print("Could not insert performance seat. ", e)
        return False

    def insert_performance_seats_batch(self, performance_id, seat_numbers, title, start_date, taken_by):
 
        for try_num in range(self.NUM_EXTENDED_TIMEOUT):
            try:
                result = self.session.execute(self.select_taken_by_from_performance_seats_stmt, [performance_id, seat_numbers])
                if result.one():
                    return False
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                for i in range(len(seat_numbers)):
                    batch.add(self.insert_performance_seat_stmt,[performance_id, 
                                                                seat_numbers[i], 
                                                                title,
                                                                start_date,
                                                                taken_by])
                self.session.execute(batch, execution_profile=f'profile{try_num}')

                result = self.session.execute(self.select_taken_by_from_performance_seats_stmt, [performance_id, seat_numbers])
                try_again = False
                for r in result:
                    if r.taken_by != taken_by:
                        for seat_number in seat_numbers:
                            self.session.execute(self.delete_performance_seat_stmt, [performance_id, seat_number, taken_by])
                        try_again = True
                        break

                if try_again:
                    pass
                return True
            except Timeout as timeout_e:
                print("Timeout exception occurs. ", timeout_e)
            except Exception as e:
                print("Could not batch insert performance seats. ", e)
              
        return False

    def select_performances_by_dates(self, dates, is_timeout_extended = False):
        rows = []
        try:
            if is_timeout_extended:
                rows = self.session.execute(self.select_performances_by_dates_stmt,[dates], execution_profile=f'profile{int(self.NUM_EXTENDED_TIMEOUT/2)}')
            else:
                rows = self.session.execute(self.select_performances_by_dates_stmt,[dates])
        except Exception as e:
            print(e)
        return rows

    def select_user_tickets(self, email, buy_timestamp_start, buy_timestamp_end):
        rows = []
        try:
            rows = self.session.execute(self.select_user_tickets_stmt, [email, buy_timestamp_start, buy_timestamp_end])
        except Exception as e:
            print(e)
        return rows

    def insert_user_ticket(self, email, buy_timestamp, performance_id, seat_number, first_name, last_name):
        try:
            result = self.session.execute(self.select_user_tickets_check_stmt, [email, buy_timestamp, performance_id, [seat_number]])
            if result.one().cnt != 0:
                return True
            self.session.execute(self.insert_user_ticket_stmt,[email, buy_timestamp, performance_id, seat_number, first_name, last_name])
            result = self.session.execute(self.select_user_tickets_check_stmt, [email, buy_timestamp, performance_id, [seat_number]])
            if result.one().cnt != 0:
                return True
            print("Could not insert user ticket.")
        except Exception as e:
            print("Could not insert user ticket.", e)
        return False

    def insert_user_ticket_batch(self, email, buy_timestamp, performance_id, seat_numbers, first_names, last_names):
        for try_num in range(self.NUM_EXTENDED_TIMEOUT):
            try:
                result = self.session.execute(self.select_user_tickets_check_stmt, [email, buy_timestamp, performance_id, seat_numbers])
                if result.one().cnt != 0:
                    return True
                batch = BatchStatement()
                for i, seat_number in enumerate(seat_numbers):
                    batch.add(self.insert_user_ticket_stmt,[email, buy_timestamp, performance_id, seat_number, first_names[i], last_names[i]])
                self.session.execute(batch, execution_profile=f'profile{try_num}')
                result = self.session.execute(self.select_user_tickets_check_stmt, [email, buy_timestamp, performance_id, seat_numbers])
                if result.one().cnt != 0:
                    return True
                print("Could not insert user tickets.")
            except Timeout as timeout_e:
                print("Timeout exception occurs. ", timeout_e)
            except Exception as e:
                print("Could not insert user tickets.", e)

        return False

    # def update_performance_seat_take_seat(self, performance_id, seat_number, email):
    #     try:
    #         result = self.session.execute(self.update_performance_seat_take_seat_stmt,[email, performance_id, seat_number])
    #         if not result.one().applied:
    #             print("Could not update performance seat (take).")
    #             return False
    #         return True
    #     except Exception as e:
    #         print("Could not update performance seat (take).", e)
    #     return False

    # def update_performance_seat_take_seat_batch(self, performance_id, seat_numbers, email):
    #     try:
    #         batch = BatchStatement()
    #         for seat_number in seat_numbers:
    #             batch.add(self.update_performance_seat_take_seat_stmt,[email, performance_id, seat_number])
    #         result = self.session.execute(batch)
    #         for r in result:
    #             if not r.applied:
    #                 print("Could not update performance seat batch(take).")
    #                 return False
    #         return True
    #     except Exception as e:
    #         print("Could not update performance seat batch(take).", e)
    #     return False

    # def update_performance_seat_free_seat(self, performance_id, seat_number):
    #     try:
    #         result = self.session.execute(self.update_performance_seat_take_seat_stmt,[performance_id, seat_number])
    #         if not result:
    #             print("Could not update performance seat (free).")
    #             return False
    #         return True
    #     except Exception as e:
    #         print("Could not update performance seat (free).", e)
    #     return False
    
    def select_performance_seats(self, performance_id, is_timeout_extended = False):
        rows = []
        try:
            if is_timeout_extended:
                rows = self.session.execute(self.select_performance_seats_stmt, [performance_id], execution_profile=f'profile{int(self.NUM_EXTENDED_TIMEOUT/2)}')
            else:
                rows = self.session.execute(self.select_performance_seats_stmt, [performance_id])
        except Exception as e:
            print(e)
        return rows

    def select_performance_seat_performance_info(self, performance_id, seat_number):
        try:
            row = self.session.execute(self.select_performance_seat_performance_info_stmt,[performance_id, seat_number]).one()
            if row:
                return row
            return False
        except Exception as e:
            print(e)
        return None
