from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import math
import uuid


# number of threads
N_WORKERS = 50

# number of users (test scenario repeats)
N_USERS = 100

# number of performances each user creates
N_PERFORMANCES = 500

# number of tickets purchased by each user for each created performance (the number is then multiplied by 3) (max=9)
N_BUY_TICKETS_X3 = 2


thread_local = threading.local()

f = open("contact_points.txt", "r")
CONTACT_POINTS = [cp for cp in f.read().splitlines()]

SEATS = [x for x in range(1,51)]

def get_db():
    try:
        if not hasattr(thread_local, "db"):
            thread_local.db =  Database(CONTACT_POINTS)
        return thread_local.db  
    except Exception as e:
        print(e)

def create_account(db, data):
    db.insert_user(f'email{data["id"]}@email.com', f'FirstName{data["id"]}', f'LastName{data["id"]}')

def log_in(db, data):
    db.select_user(f'email{data["id"]}@email.com')

def add_performance(db, data):

    for i,p_date in enumerate(data['p_dates']):
        db.insert_performance(p_date, data['start_dates'][i], data['title'],data['end_dates'][i], data['uuids'][i])
        db.insert_performance_seats_batch(data['uuids'][i], SEATS , [data['title']],[data['start_dates'][i]],[None])

def get_programme(db, data):
    db.select_performances_by_dates(data['p_dates'[:14]])

def buy_tickets(db,data, shift=0):
    # one ticket
    for i,p_date in enumerate(data['p_dates']):
        performances = db.select_performances_by_dates([p_date])

        db.update_performance_seat_take_seat_batch(performances[i%10].performance_id, [i%10+shift], f'email{data["id"]}@email.com')
        db.insert_user_ticket_batch(f'email{data["id"]}@email.com', performances[i%10].performance_id, [i%10+shift], [f'FirstName{data["id"]}'], [f'LastName{data["id"]}'])

    # two tickets
    for i,p_date in enumerate(data['p_dates']):
        performances = db.select_performances_by_dates([p_date])

        db.update_performance_seat_take_seat_batch(performances[i%10].performance_id, [10+(i%10)+shift,20+(i%10)+shift], f'email{data["id"]}@email.com')
        db.insert_user_ticket_batch(f'email{data["id"]}@email.com', performances[i%10].performance_id,  [10+(i%10)+shift,20+(i%10)+shift], [f'FirstName{data["id"]}',f'FirstName{data["id"]}-2'], [f'LastName{data["id"]}',f'LastName{data["id"]}-2'])


def get_user_tickets(db,data):
    db.select_user_tickets(f'email{data["id"]}@email.com')

def jobs(data):
    db = get_db()

    create_account(db, data)
    log_in(db, data)
    add_performance(db, data)
    get_programme(db,data)
    for s in range(N_BUY_TICKETS_X3):
        buy_tickets(db,data,shift=s)
    get_user_tickets(db,data)

def test_jobs_one_thread(data):
    db = Database(CONTACT_POINTS)

    for d in data:
        create_account(db, d)
        log_in(db, d)
        add_performance(db, d)
        get_programme(db,d)
        for s in range(N_BUY_TICKETS_X3):
            buy_tickets(db,d,shift=s)
        get_user_tickets(db,d)

def run_with_threads(n_workers, data):
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        executor.map(jobs, data)


def prepare_data():
    data = [ dict(id=i) for i in range(N_USERS) ]

    start_date = dt.datetime.strptime('2023-01-23', '%Y-%m-%d')
    start_datetime = dt.datetime.strptime('2023-01-23 8:00', '%Y-%m-%d %H:%M')
    
    for d in data:
        n_from = d["id"]*N_PERFORMANCES
        n_to = (d["id"]+1)*N_PERFORMANCES-1
        
        p_dates = [start_date+dt.timedelta(days=math.floor(i/10)) for i in range(n_from,n_to+1)  ]
        start_dates = [start_datetime+dt.timedelta(days=math.floor(i/10),hours=i%10) for i in range(n_from,n_to+1)  ]
        end_dates = [start_datetime+dt.timedelta(days=math.floor(i/10),hours=(i%10)+1) for i in range(n_from,n_to+1)  ]

        d.update({"p_dates":p_dates})
        d.update({"start_dates":start_dates})
        d.update({"end_dates":end_dates})
        d.update({"title":f'title{d["id"]}'})
        d.update({"uuids":[uuid.uuid1() for i in range(N_PERFORMANCES)]})

    return data


data = prepare_data()

# test_jobs_one_thread(data)

start_time = time.time()
run_with_threads(n_workers=N_WORKERS, data=data)
duration = time.time() - start_time
print(f"Finished in {duration:.2f} seconds with {N_WORKERS} threads.")
