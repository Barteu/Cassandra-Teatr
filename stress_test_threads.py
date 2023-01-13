from cassandra.cluster import Cluster
from database import Database
import datetime as dt
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor

thread_local = threading.local()

def get_db(contact_points):
    if not hasattr(thread_local, "db"):
        thread_local.db =  Database()
    return thread_local.db  

def io_bound_job(contact_points, date):
    db = get_db(contact_points)

    resp = db.select_performances_by_dates(date)
    print(f"Response status: {resp.one()}")

def run_with_threads(n_jobs, dates):
    # `max_workers` specifies the max number threads to created.
    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        # When the target function is mapped to a list or tuple, the target function
        # is executed for every element of the list or tuple in a separate thread.
        executor.map(io_bound_job, dates)


if __name__ == "__main__": 
    f = open("contact_points.txt", "r")
    contact_points = [cp for cp in f.read().splitlines()]
    
    dates = [dt.datetime.strptime('2023-01-21', '%Y-%m-%d')]

    # Run with one thread:
    start_one_thread = time.time()

    run_with_threads(n_jobs=1, dates=dates)
    duration = time.time() - start_one_thread
    print(f"IO-bound job finished in {duration:.2f} seconds with one thread.")
                            
    # Run with ten threads:
    start_three_threads = time.time()
    run_with_threads(n_jobs=10, dates=dates)
    duration = time.time() - start_three_threads
    print(f"IO-bound job finished in {duration:.2f} seconds with three threads.")
