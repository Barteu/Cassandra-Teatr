from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
import math
import uuid
from reload_db import drop_schema, create_schema

# number of processes
N_WORKERS = 1

# number of users (test scenario repeats)
N_USERS = 1

# number of performances each user creates
N_PERFORMANCES = 10

# how many tickets should be purchased for each performance multiplied by 10 (from 1 to 10)
N_BUY_TICKETS_X10 = 10



f = open("contact_points.txt", "r")
CONTACT_POINTS = [cp for cp in f.read().splitlines()]

SEATS = [x for x in range(1,101)]


def create_account(db, data):
    db.insert_user(f'email{data["id"]}@email.com', f'FirstName{data["id"]}', f'LastName{data["id"]}')

def log_in(db, data):
    db.select_user(f'email{data["id"]}@email.com')
  
def add_performance(db, data):

    for i,p_date in enumerate(data['p_dates']):
        
        correct = db.insert_performance(p_date, data['start_dates'][i], data['title'],data['end_dates'][i], data['uuids'][i])
        if not correct:
            performances = db.select_performances_by_dates([p_date], is_timeout_extended=True)
            for perf in performances:
                if perf.performance_id == data['uuids'][i]:
                    correct=True
                    break
        if correct:
            db.insert_performance_seats_batch(data['uuids'][i], SEATS , [data['title']],[data['start_dates'][i]],[None])

def get_programme(db, data):
    db.select_performances_by_dates(data['p_dates'][:14])

def buy_tickets(db,data):
    # ugly solution
    """
    two tickets

    data keys:
        id
        seats [p_date, uuid, seat_num]
    """

    for seat in data['seats']:
        performance_id = seat[1]
        seats_num = [seat[2],seat[2]+1]
        user_email = f'email{data["id"]}@email.com'
        success = db.update_performance_seat_take_seat_batch(performance_id, seats_num, user_email)

        if not success:
            seats = db.select_performance_seats(performance_id, is_timeout_extended=True)
            seats_taken = 0
            for seat in seats:
                if seat.seat_number in seats_num and seat.taken_by == user_email:
                    seats_taken+=1
            if seats_taken==2:
                success = True

        if success:
            db.insert_user_ticket_batch(user_email, seat[1],  [seat[2],seat[2]+1], [f'FirstName{data["id"]}',f'FirstName{data["id"]}-2'], [f'LastName{data["id"]}',f'LastName{data["id"]}-2'])
   


def get_user_tickets(db,data):
    db.select_user_tickets(f'email{data["id"]}@email.com')

def jobs(data):
    db = Database(CONTACT_POINTS)

    create_account(db, data)
    log_in(db, data)
    add_performance(db, data)
    get_programme(db,data)

def jobs_tickets(data):
    db = Database(CONTACT_POINTS)
    # buy tickets 
    buy_tickets(db,data)

def test_jobs_tickets_one_thread(data):
    db = Database(CONTACT_POINTS)

    for d in data:
        # buy tickets 
        buy_tickets(db,d)
    db.finalize()

def test_jobs_one_thread(data):
    db = Database(CONTACT_POINTS)

    for d in data:
        create_account(db, d)
        log_in(db, d)
        add_performance(db, d)
        get_programme(db,d)


def run_with_processes(n_workers, data):
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        executor.map(jobs, data)

def run_with_processes_tickets(n_workers,data):
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        executor.map(jobs_tickets, data)

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

def shuffle_data_tickets(data):
    #[p_date, uuid, seat_num]
    seats = [[] for i in range(10)]

    data_2 =  [ {**dict(id=i), 'seats':[] } for i in range(N_USERS-1, -1, -1)]
    n=0
    for d in data:
        d.pop('start_dates', None)
        d.pop('end_dates', None)
        d.pop('title', None)
        for i,p_date in enumerate(d['p_dates']):
            for j in range(N_BUY_TICKETS_X10*5):
                data_2[n%len(data_2)]['seats'].append([p_date,d['uuids'][i],((j)*2)+1])
                n+=1
    return data_2

print(f"Preparing data for test..")
data = prepare_data()
data_2 = shuffle_data_tickets(data)

# clear DB
db = Database(CONTACT_POINTS)
drop_schema(db)
create_schema(db)
db.finalize()

print(f"Testing..")
start_time_1 = time.time()
run_with_processes(n_workers=N_WORKERS, data=data)
duration_1 = time.time() - start_time_1
print(f"\nStage 1  Finished in {duration_1:.2f} seconds with {N_WORKERS} processes.")

data.clear()

start_time_2 = time.time()
print(f"Testing stage 2 (tickets)..")
run_with_processes_tickets(n_workers=N_WORKERS, data=data_2)
duration_2 = time.time() - start_time_2
print(f"Stage 2 Finished in {duration_2:.2f} seconds with {N_WORKERS} processes.")

data_2.clear()

print(f"Calculating results..\n")
time.sleep(5)
db = Database(CONTACT_POINTS)
users_num = db.count_users()
performances_num = db.count_performances()
performances_seats_num = db.count_performance_seats()
tickets_num = db.count_tickets()
db.finalize()

print(f"{'Added users: ':<30}{users_num}/{N_USERS} [loss: {(1.0-users_num/N_USERS)*100:.2f}%]")
print(f"{'Added tickets: ':<30}{tickets_num}/{N_PERFORMANCES*N_USERS*N_BUY_TICKETS_X10*10} [loss: {(1.0-tickets_num/(N_PERFORMANCES*N_USERS*N_BUY_TICKETS_X10*10))*100:.2f}%]")
print(f"{'Added performances: ':<30}{performances_num}/{N_PERFORMANCES*N_USERS} [loss: {(1.0-performances_num/(N_PERFORMANCES*N_USERS))*100:.2f}%]")
print(f"{'Added performances seats: ':<30}{performances_seats_num}/{N_PERFORMANCES*N_USERS*100} [loss: {(1.0-min(performances_seats_num/(N_PERFORMANCES*N_USERS*100),1.0))*100:.2f}%]")

