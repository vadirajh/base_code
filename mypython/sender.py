#!/usr/bin/env python
import pika

#connection = pika.BlockingConnection(pika.ConnectionParameters(
#        host='tel02'))
credentials = pika.PlainCredentials('vadi','vadi')
parameters = pika.ConnectionParameters('vhosu04',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()

channel.queue_declare(queue='10.91.38.34')

channel.basic_publish(exchange='',
                      routing_key='10.91.38.34',
                      body='ls')
print " [x] Sent 'ls'"
connection.close()
