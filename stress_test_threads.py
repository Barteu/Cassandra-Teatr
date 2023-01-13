from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor

thread_local = threading.local()

f = open("contact_points.txt", "r")
contact_points = [cp for cp in f.read().splitlines()]

def get_db():
    try:
        if not hasattr(thread_local, "db"):
            thread_local.db =  Database(contact_points)
        return thread_local.db  
    except Exception as e:
        print(e)

def io_bound_job(date):
    db = get_db()
    results = db.select_performances_by_dates([date])
    print(f"Response status: {[r for r in results]}")

def run_with_threads(n_workers, dates):
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        executor.map(io_bound_job, dates)



dates = [(dt.datetime.strptime('2023-01-21', '%Y-%m-%d')+dt.timedelta(days=i)) for i in range(30)]

# Run with one thread:
start_one_thread = time.time()

run_with_threads(n_workers=1, dates=dates)
duration = time.time() - start_one_thread
print(f"IO-bound job finished in {duration:.2f} seconds with one thread.")
                        
# Run with ten threads:
start_three_threads = time.time()
run_with_threads(n_workers=3, dates=dates)
duration = time.time() - start_three_threads
print(f"IO-bound job finished in {duration:.2f} seconds with three threads.")
