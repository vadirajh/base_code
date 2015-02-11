import pyhs2
 
with pyhs2.connect(host='localhost',
                   port=10000,
                   authMechanism="PLAIN",
                   user='root',
                   password='test',
                   database='default') as conn:
    with conn.cursor() as cur:
        #Show databases
        #print cur.getDatabases()
 
        #Execute query
        #cur.execute("select * from avere_stats_stg")
        cur.execute("select unix_ts,value from avere_stats where name='write_ops'")
 
        #Return column info from query
        #print cur.getSchema()
 
        #Fetch table results
        print '['
        for i in cur.fetch():
            print "{0},".format(i)
        print ']'
