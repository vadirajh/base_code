#!/usr/bin/env python
import pika
import subprocess
from multiprocessing import Process

USER = 'vadi'
PASSWORD = 'vadi'
MASTER = 'tel02'
MASTER_PORT = 5672
VHOST = '/'

credentials = pika.PlainCredentials(USER,PASSWORD)
parameters = pika.ConnectionParameters(MASTER,MASTER_PORT,VHOST,credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.exchange_declare(exchange='logs',
                         type='fanout')
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='logs',
                   queue=queue_name)
print ' Waiting for commands. To exit press CTRL+C'

def newproc(cmd):
   try:
      proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
      data, stderr = proc.communicate()
   except:
      print '       Bad command' 
      data = "Invalid command or options"
   try:
      proc = subprocess.Popen("hostname", stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
      myid, stderr = proc.communicate()
   except:
      print '       Cant find hostname'
      myid = None
   credentials = pika.PlainCredentials(USER,PASSWORD)
   parameters = pika.ConnectionParameters(MASTER,MASTER_PORT,VHOST,credentials)
   connection = pika.BlockingConnection(parameters)
   channel = connection.channel()
   channel.queue_declare(queue='sink_q')
   channel.basic_publish(exchange='',
                      routing_key='sink_q',
                      body=data,
                      properties=pika.BasicProperties(reply_to=myid))
   connection.close()


def callback(ch, method, properties, body):
   print " [x] %r" % (body,)
   p = Process(target=newproc, args=(body,))
   p.start()

 
channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)
channel.start_consuming()
