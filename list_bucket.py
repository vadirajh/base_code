""" Important: This code assumes you have VPN access to the COLO 
    Also it requires tunneling setup to one of the client machines.
    This was tested with tunneling as L 1001 172.17.1.1:7071
    for a ssh session with 192.168.101.10 (client09).
    
    All the values are hard-coded and this code is being checked in only for
    usage example.
"""
import boto
import boto.s3.connection

conn = boto.connect_s3(
        aws_access_key_id = 'accessid',
        aws_secret_access_key = 'somekey',
        is_secure=True,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
print 'List of buckets'
for bucket in conn.get_all_buckets():
        print "{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
        )
bucket = conn.create_bucket('demo-media')
#List content of bucket
"""for key in bucket.list():
       	print "{name}\t{size}\t{modified}".format(
               	name = key.name,
                size = key.size,
       	        modified = key.last_modified,
               	)
"""
bucket = conn.get_bucket('demo-media')
print 'Contents of bucket'
#for key in bucket.list(prefix='s'):
for key in bucket.list():
       	print "{name}\t{size}\t{modified}".format(
               	name = key.name,
                size = key.size,
       	        modified = key.last_modified,
               	)
