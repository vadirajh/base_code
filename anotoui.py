import csv
import datetime, time
with open('gendata.csv', 'rb') as f:
    reader = csv.reader(f)
    count = 0
    print "["    
    for row in reader:
        if count != 0:
            start_dt = datetime.datetime.strptime(row[0], '%Y-%m-%dT%H:%M:%S%Z')
            sum = 0
            for i in range(1,14):
                sum += int(row[i])
            print "["+str(int(time.mktime(start_dt.timetuple())*1000))+", "+str(sum/14)+"],"
        count += 1

    print "]"    
