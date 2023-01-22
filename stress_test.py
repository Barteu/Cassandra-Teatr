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
import random

# number of processes
N_WORKERS = 500

# number of users (test scenario repeats)
N_USERS = 500

# number of performances each user creates
N_PERFORMANCES = 10

# number of seats for each performance
N_SEATS = 200

# how many tickets can be bought simultaneously by user
N_MAX_TICKETS_AT_ONCE = 4

# how many attempts to buy tickets(1 to N_MAX_TICKETS_AT_ONCE at once) should be generated for each performance
N_BUY_TICKETS_ATTEMPTS = 50

# number of attempts to connect to DB 
N_DB_CONNECT_ATTEMPTS = 10

f = open("contact_points.txt", "r")
CONTACT_POINTS = [cp for cp in f.read().splitlines()]


def create_account(db, data):
    db.insert_user(f'email{data["id"]}@email.com', f'FirstName{data["id"]}', f'LastName{data["id"]}')
  
def add_performances(db, data):

    for i,p_date in enumerate(data['p_dates']):
        correct = db.insert_performance(p_date, 
                                        data['start_dates'][i], 
                                        data['title'],
                                        data['end_dates'][i],
                                        data['uuids'][i],
                                        N_SEATS)

def buy_tickets(db, data):
   
    for seat in data['seats_to_buy']:
        performance_id = seat['uuid']
        performances = db.select_performances_by_dates([seat['p_date']])
        performance_exists = False

        selected_performance = None
        for perf in performances:
            if perf.performance_id == performance_id:
                performance_exists = True
                selected_performance = perf
                break
        if performance_exists is False:
            continue
       
        seats_numbers = seat['seats']
      
        user_email = f'email{data["id"]}@email.com'+str(seats_numbers)
        
        buy_timestamp = dt.datetime.today()
        success = db.insert_performance_seats_batch(performance_id, 
                                                    seats_numbers, 
                                                    selected_performance.title,
                                                    selected_performance.start_date,
                                                    user_email
                                                    )

        if not success:
            seats = db.select_performance_seats(performance_id, is_timeout_extended=True)
            seats_taken = 0
            for seat in seats:
                if seat.seat_number in seats_numbers and seat.taken_by == user_email:
                    seats_taken+=1
            if seats_taken==len(seats_numbers):
                success = True
        
        if success:
            result = db.insert_user_ticket_batch(user_email,
                                                 buy_timestamp,
                                                 performance_id,
                                                 seats_numbers,
                                                 [f'FirstName{data["id"]}']*len(seats_numbers),
                                                 [f'LastName{data["id"]}']*len(seats_numbers))

def jobs(data):
    for i in range(N_DB_CONNECT_ATTEMPTS):
        try:
            db = Database(CONTACT_POINTS, disable_prints=True)
            break
        except Exception as e:
            pass
        
    create_account(db, data)
    add_performances(db, data)
    db.finalize()

def jobs_tickets(data):
    for i in range(N_DB_CONNECT_ATTEMPTS):
        try:
            db = Database(CONTACT_POINTS, disable_prints=True)
            break
        except Exception as e:
            pass
    # buy tickets 
    buy_tickets(db,data)
    db.finalize()

def test_jobs_tickets_one_thread(data):
    db = Database(CONTACT_POINTS, disable_prints=False)
    for d in data:
        buy_tickets(db,d)
    db.finalize()

def test_jobs_one_thread(data):

    db = Database(CONTACT_POINTS, disable_prints=False)
    for d in data:
        create_account(db, d)
        add_performances(db, d)
    db.finalize()   

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
    # 'seats_to_buy' is a list of dictionaries which contain 'p_date', 'uuid' and 'seats'
    data_2 =  [ {**dict(id=i), 'seats_to_buy':[]} for i in range(N_USERS-1, -1, -1)]
    
    total_tickets_num = 0
    seats_to_buy = []
    for d in data:
        # for each performance
        for i,p_date in enumerate(d['p_dates']):
            for j in range(N_BUY_TICKETS_ATTEMPTS):
                tickets_num = random.randint(1,N_MAX_TICKETS_AT_ONCE)
                total_tickets_num += tickets_num
                seats = [1+j*2+x for x in range(tickets_num)]
                seats_to_buy.append({'p_date':p_date, 'uuid':d['uuids'][i], 'seats': seats })
                
    random.shuffle(seats_to_buy)
    n = int(len(seats_to_buy)/N_USERS)
    divided_seats_to_buy = [seats_to_buy[i:i+n] for i in range(0, len(seats_to_buy), n)]
    for i,ds in enumerate(divided_seats_to_buy):
        data_2[i]['seats_to_buy'] = ds
    
    return data_2, total_tickets_num

print(f"Preparing data for test..")
data = prepare_data()
data_2, total_tickets_num = shuffle_data_tickets(data)


print("Refreshing database")
#clear DB
db = Database(CONTACT_POINTS, disable_prints=False)
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


str_users = f"{'Added users: ':<30}{users_num}/{N_USERS} [loss: {(1.0-users_num/N_USERS)*100:.2f}%]"
str_performances = f"{'Added performances: ':<30}{performances_num}/{N_PERFORMANCES*N_USERS} [loss: {(1.0-performances_num/(N_PERFORMANCES*N_USERS))*100:.2f}%]"
str_performance_seats = f"{'Added performances seats: ':<30}{performances_seats_num}"
str_tickets = f"{'Added tickets: ':<30}{tickets_num}"


print('Number of processes: ', N_WORKERS)
print(str_users)
print(str_performances)
print(str_performance_seats)
print(str_tickets)

stage_1_inserts = N_USERS*N_PERFORMANCES
stage_1_inserts_per_sec = stage_1_inserts/duration_1

str_stage_1_inserts = f"{'Inserts(users, performances) per sec in stage 1:':<50} {stage_1_inserts_per_sec:.2f}"

stage_2_inserts =N_USERS*N_PERFORMANCES*N_BUY_TICKETS_ATTEMPTS*2
stage_2_inserts_per_sec = stage_2_inserts/duration_2

str_stage_2_inserts = f"{'Inserts(tickets) per sec in stage 2:':<50} {stage_2_inserts_per_sec:.2f}"

# +1 select performance +2 selects during insert seat +2 selects during insert ticket
stage_2_all_operations = (stage_2_inserts/2)*7
stage_2_all_operations_per_sec = stage_2_all_operations/duration_2

str_stage_2_all= f"{'Operations per sec in stage 2:':<50} {stage_2_all_operations_per_sec:.2f}"

print(str_stage_1_inserts)
print(str_stage_2_inserts)
print(str_stage_2_all)
