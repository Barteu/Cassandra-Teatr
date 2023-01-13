from cassandra.cluster import Cluster
from application import Application
from database import Database
import sys

if __name__ == "__main__":
 
    try:
        db = Database()
    except Exception as e:
        print(e)
        sys.exit("Could not connect to DB, try later")
    
    app = Application(db)
    while app.is_running:
        app.show_menu()
        action = input('Choose action: ')
        app.do_action(action)

    app.db.finalize()