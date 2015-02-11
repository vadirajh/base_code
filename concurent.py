from tornado import gen, ioloop

@gen.coroutine
def another_async_func(x):
    print "aaf"
    raise gen.Return(x + 1)

@gen.coroutine
def myfunc(x):
    print "myfunc"
    y = yield another_async_func(x)
    print "back"
    raise gen.Return(y)

@gen.coroutine
def main():
    y = yield myfunc(1)
    print "Callback called with %d" % y

if __name__ == "__main__":
    ioloop.IOLoop.instance().run_sync(main)
