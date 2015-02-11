#!/usr/bin/env python
import pika
import subprocess

credentials = pika.PlainCredentials('vadi','vadi')
parameters = pika.ConnectionParameters('tel02',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='10.91.38.34')

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)
    proc = subprocess.Popen(body, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    data, stderr = proc.communicate()
    print "Output of %r is %s" %(body,data,)

channel.basic_consume(callback,
                      queue='queue1',
                      no_ack=True)

channel.start_consuming()
