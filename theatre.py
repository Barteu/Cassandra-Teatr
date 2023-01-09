from cassandra.cluster import Cluster
from application import Application
from database import Database

if __name__ == "__main__":
 
    try:
        db = Database()
    except Exception as e:
        print(e)
    
    app = Application(db)
    while app.is_running:
        app.show_menu()
        action = input('Choose action: ')
        app.do_action(action)

    db.finalize()