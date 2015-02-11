""" Important: This code assumes you have VPN access to the COLO 
    Also it requires tunneling setup to one of the client machines.
    This was tested with tunneling as L 1001 172.17.1.1:7071
    for a ssh session with 192.168.101.10 (client09).
    
    All the values are hard-coded and this code is being checked in only for
    usage example.
"""
import math, os
import boto
import boto.s3.connection
from filechunkio import FileChunkIO

# Connect to S3
c = boto.connect_s3(
        aws_access_key_id = 's3media',
        aws_secret_access_key = 's3media',
        host = '198.24.35.3',
        port = 7091,
        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
b = c.get_bucket('media')
f = open('/tmp/TASM2.mp4', 'wb')

key = b.lookup('TASM2.mp4')
for bytes in key:
	f.write(bytes)
f.close()
