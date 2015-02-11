""" Important: This code assumes you have VPN access to the COLO 
    Also it requires tunneling setup to one of the client machines.
    This was tested with tunneling as L 1001 172.17.1.1:7071
    for a ssh session with 192.168.101.10 (client09).
    
    All the values are hard-coded and this code is being checked in only for
    usage example.
"""
import boto
import boto.s3.connection
from random import randint

my_port = randint(7091, 7091+11)
print "Random port is {0}".format(my_port)
conn = boto.connect_s3(
        aws_access_key_id = 's3media',
        aws_secret_access_key = 's3media',
        is_secure=False,               # uncomment if you are not using ssl
        host = '198.24.35.3',#host = '72.28.98.46',
        port = my_port,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
print 'List of buckets'
for bucket in conn.get_all_buckets():
        print "{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
        )
#bucket = conn.create_bucket('mys3test')
#List content of bucket
"""for key in bucket.list():
       	print "{name}\t{size}\t{modified}".format(
               	name = key.name,
                size = key.size,
       	        modified = key.last_modified,
               	)
"""
bucket = conn.get_bucket('media')
print 'Contents of bucket'
#for key in bucket.list(prefix='s'):
for key in bucket.list():
       	print "{name}\t{size}\t{modified}".format(
               	name = key.name,
                size = key.size,
       	        modified = key.last_modified,
               	)
