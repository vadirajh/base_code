import Queue
import threading
def worker():
    while True:
        item = q.get()
        print item
        q.task_done()

q = Queue.Queue()
for i in range(10):
     t = threading.Thread(target=worker)
     t.daemon = True
     t.start()

for i in range(5):
    q.put(i)

q.join()       # block until all tasks are done

