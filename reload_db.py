from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from database import Database

def drop_schema(db):
    with open('scripts/drop_schema.cql', mode='r') as f:
        txt = f.read()
        stmts = txt.split(r';')
        for i in stmts:
            stmt = i.strip()
            if stmt != '':
                print('Executing "' + stmt + '"')
                db.session.execute(stmt)

def create_schema(db):
    
    with open('scripts/create_schema.cql', mode='r') as f:
        txt = f.read()
        stmts = txt.split(r';')
        for i in stmts:
            stmt = i.strip()
            if stmt != '':
                print('Executing "' + stmt + '"')
                db.session.execute(stmt)

def load_test_data(db):
    with open('scripts/load_data.cql', mode='r') as f:
        txt = f.read()
        stmts = txt.split(r';')
        for i in stmts:
            stmt = i.strip()
            if stmt != '':
                print('Executing "' + stmt + '"')
                db.session.execute(stmt)
        

if __name__ == "__main__": 
    
    f = open("contact_points.txt", "r")
    contact_points = [cp for cp in f.read().splitlines()]

    db = Database(contact_points)

    drop_schema(db)
    create_schema(db)
    load_test_data(db)

    db.finalize()
