import csv
import datetime, time
from query_presto import ClientSession, Query
from collections import OrderedDict
if __name__ == "__main__":
	session = ClientSession(server="localhost:8081", user="hadoop", catalog="hive", schema="default")
	q = Query.start(session, "select unix_ts as ts, category as cat, avg(value) as val from avere_stats where name='read_ops'"
        	                 " group by unix_ts, category order by unix_ts, category");

	ordered_fieldnames = OrderedDict([('ts',None),('drive0',None),('drive1',None),('drive2',None),
                                              ('drive3',None),('drive4',None),('drive5',None),('drive6',None),
                                              ('drive7',None),('drive8',None),('drive9',None),('drive10',None),
                                              ('drive11',None),('drive12',None),('drive13',None),('drive14',None)])
        with open("/tmp/mygendata.csv",'wb') as fou:
                dw = csv.DictWriter(fou, fieldnames=ordered_fieldnames)
                dw.writeheader()
                # continue on to write data
                #r = []
                count = 0
                drive_info = {}
                for row in q.results():
                    str_dt = datetime.datetime.fromtimestamp(row[0]).strftime('%Y-%m-%dT%H:%M:%S%Z')
                    #str = str(int(time.mktime(start_dt.timetuple())*1000))
	            drive_info[int(row[1][len("drive"):])]=dict(value=int(row[2])) 
                    if count == 14:
			dw.writerow({'ts':str_dt, 
				 'drive0':drive_info[0]['value'],
				 'drive1':drive_info[1]['value'],
				 'drive10':drive_info[10]['value'],
				 'drive11':drive_info[11]['value'],
				 'drive12':drive_info[12]['value'],
				 'drive13':drive_info[13]['value'],
				 'drive14':drive_info[14]['value'],
				 'drive2':drive_info[2]['value'],
				 'drive3':drive_info[3]['value'],
				 'drive4':drive_info[4]['value'],
				 'drive5':drive_info[5]['value'],
				 'drive6':drive_info[6]['value'],
				 'drive7':drive_info[7]['value'],
				 'drive8':drive_info[8]['value'],
				 'drive9':drive_info[9]['value']})
			count = 0
                        #drive_info = {}
		    count += 1 
        

	            			
			
