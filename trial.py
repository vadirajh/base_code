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
import os

# Connect to S3
c = boto.connect_s3(
        aws_access_key_id = 'AKIAJJQ3DNZPH2M7YUSQ',
        aws_secret_access_key = '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',
        is_secure=True,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
b = c.get_bucket('hawk-influxdb/hawk_data')
#for root, dirs, files in os.walk("./influxdb_data", topdown=False):
#    for name in files:
source_path = './influxdb_data/hawk_data0.csv'

source_size = os.stat(source_path).st_size

        # Create a multipart upload request
mp = b.initiate_multipart_upload(os.path.basename(source_path))

        # Use a chunk size of 10 MiB (feel free to change this)
chunk_size = 52428800
chunk_count = int(math.ceil(source_size / chunk_size))

        # Send the file parts, using FileChunkIO to create a file-like object
        # that points to a certain byte range within the original file. We
        # set bytes to never exceed the original file size.
for i in range(chunk_count + 1):
       offset = chunk_size * i
       bytes = min(chunk_size, source_size - offset)
       with FileChunkIO(source_path, 'r', offset=offset,
                           bytes=bytes) as fp:
           mp.upload_part_from_file(fp, part_num=i + 1)

     # Finish the upload
mp.complete_upload()
