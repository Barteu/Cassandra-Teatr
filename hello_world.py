from cassandra.cluster import Cluster
if __name__ == "__main__":
    print('Hello world!')
    cluster = Cluster(['127.0.0.1'],port=9042)
    session = cluster.connect('store',wait_for_all_pools=True)
    session.execute('USE store')
    rows = session.execute('SELECT * FROM shopping_cart;')
    for row in rows:
        print(row.userid,row.item_count,row.last_update_timestamp)