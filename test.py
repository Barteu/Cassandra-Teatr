from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid

if __name__ == "__main__": 
    
    db = Database()

    rows = db.select_all_performances()
    for row in rows:
        print(row)
    
    rows = db.select_all_performance_seats()
    for row in rows:
        print(row)

    rows = db.select_all_tickets()
    for row in rows:
        print(row)

    rows = db.select_all_users()
    for row in rows:
        print(row)


    user = db.select_user('john.doe@email.com')
    print('Select user: ', user)

    rows = db.select_current_performances()
    for row in rows:
        print(row)

    p_uuid = uuid.uuid1()
    db.insert_performance('Little Red Riding Hood', dt.datetime.strptime('2023-01-16 12:00', '%Y-%m-%d %H:%M'), dt.datetime.strptime('2023-01-16 13:15', '%Y-%m-%d %H:%M'), p_uuid)

    rows = db.select_all_performances()
    for row in rows:
        print(row)

    db.finalize()
