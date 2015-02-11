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
        #cur.execute("add jar /home/hadoop/hive/auxlib/brickhouse-0.7.1-SNAPSHOT.jar")
        #cur.execute("CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF'")
        #cur.execute("select to_json(array(unix_ts, value)) from avere_stats where name='read_ops'")
        cur.execute("select unix_ts, value from avere_stats where name='read_ops' limit 10")
 
        #Return column info from query
        #print cur.getSchema()
 
        #Fetch table results
        for i in cur.fetch():
            print i

