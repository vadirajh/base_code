#!/usr/bin/env python
import pika
import subprocess
import json

credentials = pika.PlainCredentials('vadi','vadi')
parameters = pika.ConnectionParameters('10.91.38.34',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='10.91.38.34')

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)
    try:
        mydict = json.loads(body)
        print "mydict.command %s" % (mydict['command'])
        print "mydict.uuid %s" % (mydict['task_id'])
    except:
        print 'Failed in eval'
        return

    proc = subprocess.Popen(mydict['command'], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    data, stderr = proc.communicate()
    print "Output of %r is %s" %(mydict['command'],data,)

channel.basic_consume(callback,
                      queue='10.91.38.34',
                      no_ack=True)

channel.start_consuming()
