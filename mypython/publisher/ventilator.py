#!/usr/bin/env python
import pika
import sys

USER = 'vadi'
PASSWORD = 'vadi'
MASTER = 'vhosu04'
MASTER_PORT = 5672
VHOST = '/'

credentials = pika.PlainCredentials(USER,PASSWORD)
parameters = pika.ConnectionParameters(MASTER,MASTER_PORT,VHOST,credentials)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()
channel.exchange_declare(exchange='logs',
                        type='fanout')
message = ' '.join(sys.argv[1:]) or "ls"
channel.basic_publish(exchange='logs',
                      routing_key='',
                      body=message)
print " [x] Sent %r" % (message,)
connection.close()
