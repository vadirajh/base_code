import os
import sys
if sys.argv[1:] is None:
    print 'Need a proc Id'
else: 
    kill_cmd = 'kill -9 2412' 
    os.kill(2412, 9)
    print 'Killed'
