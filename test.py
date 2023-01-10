from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid
from utils import get_various_titles

if __name__ == "__main__": 
    
    db = Database()

    # rows = db.select_all_tickets()
    # for row in rows:
    #     print(row)

    # rows = db.select_all_users()
    # for row in rows:
    #     print(row)


    # user = db.select_user('john.doe@email.com')
    # print('Select user: ', user)

    # rows = db.select_current_performances()
    # for row in rows:
    #     print(row)

    p_uuid = uuid.uuid1()
    db.insert_performance('Little Red Riding Hood', dt.datetime.strptime('2023-01-16 12:00', '%Y-%m-%d %H:%M'), dt.datetime.strptime('2023-01-16 13:15', '%Y-%m-%d %H:%M'), p_uuid)
    db.insert_performance_seats_batch(p_uuid, [x for x in range(1,51)], ['Little Red Riding Hood'],[dt.datetime.strptime('2023-01-16 12:00', '%Y-%m-%d %H:%M')],[None])

    p_uuid2 = uuid.uuid1()
    db.insert_performance('Shrek', dt.datetime.strptime('2023-01-17 13:00', '%Y-%m-%d %H:%M'), dt.datetime.strptime('2023-01-17 15:00', '%Y-%m-%d %H:%M'), p_uuid2)
    db.insert_performance_seats_batch(p_uuid2, [x for x in range(1,51)], ['Shrek'],[dt.datetime.strptime('2023-01-17 15:00', '%Y-%m-%d %H:%M')],[None])

    p_uuid3 = uuid.uuid1()
    db.insert_performance('Shrek', dt.datetime.strptime('2023-01-18 13:00', '%Y-%m-%d %H:%M'), dt.datetime.strptime('2023-01-18 15:00', '%Y-%m-%d %H:%M'), p_uuid3)
    db.insert_performance_seats_batch(p_uuid3, [x for x in range(1,51)], ['Shrek'],[dt.datetime.strptime('2023-01-18 15:00', '%Y-%m-%d %H:%M')],[None])



    # rows = db.select_all_performances()
    # for row in rows:
    #     print(row)

    # rows = db.select_all_performance_seats()
    # for row in rows:
    #     print(row)

    # rows = db.select_current_performances_by_title(get_various_titles('shrek'))
    # for row in rows:
    #     print(row)

    rows = db.select_performances_by_date(dt.datetime.strptime('2023-01-18', '%Y-%m-%d'),dt.datetime.strptime('2023-01-18', '%Y-%m-%d')+dt.timedelta(days=1))
    for row in rows:
        print(row)



    db.finalize()
