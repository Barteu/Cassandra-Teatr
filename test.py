from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid
from reload_db import drop_schema, create_schema, load_test_data

if __name__ == "__main__": 
    
    db = Database()
    drop_schema(db)
    create_schema(db)
    load_test_data(db)
    p_uuid = uuid.uuid1()
    p_date = dt.datetime.strptime('2023-01-16', '%Y-%m-%d')
    start_date = dt.datetime.strptime('2023-01-16 12:00','%Y-%m-%d %H:%M')
    title = 'Little Red Riding Hood'.lower()
    end_date = dt.datetime.strptime('2023-01-16 13:15', '%Y-%m-%d %H:%M')
    db.insert_performance(p_date,
                            start_date,
                            title,
                            end_date,
                            p_uuid,
                            50)
    r = db.session.execute(db.session.prepare("SELECT COUNT(*) as cnt from performances where p_date=? and start_date=? and title=?;"), [p_date, start_date,title])
    if r.one().cnt == 1:
        print("INSERT into performances success")
    else:
        print("INSERT into performances fail")

    db.insert_performance_seats_batch(p_uuid, [4,5,6], title, start_date,'john@email.com')

    r = db.session.execute(db.session.prepare("SELECT COUNT(*) as cnt from performance_seats where performance_id=? and seat_number in ?;"),[p_uuid, [4,5,6]])
    if r.one().cnt == 3:
        print("INSERT into performance_seats success")
    else:
        print("INSERT into performance_seats fail")


    db.insert_performance_seats_batch(p_uuid, [4,5,6], title, start_date,'kate@email.com')
    r = db.session.execute(db.session.prepare("SELECT taken_by from performance_seats where performance_id=? and seat_number in ?;"),[p_uuid, [4,5,6]])
    broke = False
    for ri in r:
        if ri.taken_by == 'kate@email.com':
            broke = True
            print('Failed UPDATE performance seat failed')
            break
    if not broke:
        print('Failed UPDATE performance seat success')

    buy_timestamp = dt.datetime.today()
    db.insert_user_ticket_batch('john@email.com', buy_timestamp, p_uuid, [4,5,6], ['john','joe', 'dezz'], ['doe', 'mamma', 'nuts'])
    r = db.session.execute(db.session.prepare("SELECT count(*) as cnt from tickets where email='john@email.com' and performance_id=? and buy_timestamp=?;"),[p_uuid, buy_timestamp])
    if r.one().cnt == 3:
        print("INSERT into tickets success")
    else:
        print("INSERT into tickets fail")
    print(db.insert_user_ticket('john.doe@email.com', buy_timestamp, p_uuid, 4, 'john', 'doe'))


    db.finalize()


