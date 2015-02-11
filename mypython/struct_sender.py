#!/usr/bin/env python
import pika
import uuid
import json

#connection = pika.BlockingConnection(pika.ConnectionParameters(
#        host='tel02'))
credentials = pika.PlainCredentials('vadi','vadi')
parameters = pika.ConnectionParameters('10.91.38.34',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()

channel.queue_declare(queue='10.91.38.34')
mycmd = {'task_id' : str(uuid.uuid1()), 'command' : 'ls'}
properties = pika.BasicProperties(app_id='struct-sender',
                                 content_type='application/json',
                                 headers=mycmd)
channel.basic_publish('',
                      '10.91.38.34',
                      json.dumps(mycmd, ensure_ascii=False),
                      properties)

print " [x] Sent 'ls'"
connection.close()
