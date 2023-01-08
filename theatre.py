from cassandra.cluster import Cluster
import db_queries as db
if __name__ == "__main__":
 
    try:
        cluster = Cluster(['127.0.0.1'],port=9042)
        session = cluster.connect('store',wait_for_all_pools=True)
        print('Connected to the cluster')
    except Exception as e:
        print(e)
        

    rows = db.get_all_performances(session)
    for row in rows:
        print(row.title, row.start_date, row.end_date, row.performance_id)
    
    rows = db.get_all_performance_seats(session)
    for row in rows:
        print(row.performance_id, row.seat_number, row.title, row.start_date, row.is_taken)

    rows = db.get_all_tickets(session)
    for row in rows:
        print(row.email, row.performance_id, row.seat_number, row.first_name, row.last_name)

    rows = db.get_all_users(session)
    for row in rows:
        print(row.email, row.first_name, row.last_name)

    