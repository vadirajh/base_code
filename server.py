import argparse
import json
import re
import datetime

from influxdb import InfluxDBClient


def main(host='192.168.101.20', port=8086):
    user = 'root'
    password = 'root'
    dbname = 'hawk_telemetry'

    client = InfluxDBClient(host, port, user, password, dbname)

    #print("Listing series...")
    result = client.query('list series /^Server_Name/')
    #print(result)
    #print
    allseries = result[0]['points']
    serieslist = []
    for item in allseries:
        serieslist.append(str(item[1]))
    #print(serieslist)
    regex_str=r'Server_Name_(?P<token0>[a-zA-Z0-9|-]*)_Device_Name_(?P<token1>[a-zA-Z0-9]*):(?P<token2>[a-zA-Z0-9|-]*)_Metric_Name_(?P<token3>[a-zA-Z0-9|%]*)'
    regex = re.compile(regex_str)
    for series in serieslist:
    #for i in range(1, 4000, 10):
    #    series = serieslist[i]
    #    print(series)
        if (series.find('Metric_Name') > 0):
            search_obj = regex.search(series)
            #print(series)
            query = 'select * from "' + series + '" limit 1'
            result = client.query(query)
            data = []
            columns = result[0]['columns']
            points = result[0]['points']
            for j in range(2, len(columns)):
                for k in range(0, len(points)):
                    ts = datetime.datetime.fromtimestamp(points[k][0]).strftime('%Y-%m-%d %H:%M:%S')
                    print "%s\t%s\t%s\t%s\t%s\t%s\t%s" % (search_obj.group('token0'), search_obj.group('token1'), search_obj.group('token2'), search_obj.group('token3'), points[k][0], ts,  points[k][j])
    #
    #print "series length = " + str(len(serieslist))

def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False, default='192.168.101.20',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port)

