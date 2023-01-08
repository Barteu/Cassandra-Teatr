from cassandra.cluster import Cluster

def get_all_performances(session):
    rows = []
    try:
        session.execute('USE Theatre')
        rows = session.execute('SELECT * FROM performances;') 
    except Exception as e:
        print(e)
    return rows

def get_all_performance_seats(session):
    rows = []
    try:
        session.execute('USE Theatre')
        rows = session.execute('SELECT * FROM performance_seats;')
    except Exception as e:
        print(e)
    return rows

def get_all_tickets(session):
    rows = []
    try:
        session.execute('USE Theatre')
        rows = session.execute('SELECT * FROM tickets;')
    except Exception as e:
        print(e)
    return rows

def get_all_users(session):
    rows = []
    try:
        session.execute('USE Theatre')
        rows = session.execute('SELECT * FROM users;')
        return rows
    except Exception as e:
        print(e)
    return rows
