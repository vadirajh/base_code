#!/usr/bin/env python
import pika
import subprocess
import sys
USER='vadi'
PASSWORD='vadi'
MASTER='vhosu04'
MASTER_PORT=5672
VHOST='/'

credentials = pika.PlainCredentials(USER,PASSWORD)
parameters = pika.ConnectionParameters(MASTER,MASTER_PORT,VHOST,credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='sink_q')

print ' Waiting for Updates. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print """

    From %r""" %(properties.reply_to,)
    
    sys.stdout.flush()
    sys.stdout.write(body)
    sys.stdout.flush()

channel.basic_consume(callback,
                      queue='sink_q',
                      no_ack=True)

channel.start_consuming()
