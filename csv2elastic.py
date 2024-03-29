#!/usr/bin/env python3

from argparse import ArgumentParser, REMAINDER
from elasticsearch import helpers, Elasticsearch
#from elasticsearch.helpers import bulk, parallel_bulk

import sys, os, csv, json, re, dateutil.parser, pprint, datetime

#csv.field_size_limit(sys.maxsize)
# added to fix for Windows
csv.field_size_limit(2147483647)

if sys.version_info[0] < 3:
    print(
        'This script requires python3 and python2 was detected. Please run this script with an compatible python interpreter.'
    )
    sys.exit(1)

# Global Variables
start_dt = datetime.datetime.now()
end_dt = start_dt

script_ver = "1.23"

print("[*] CSV 2 Elastic script v" + script_ver)
print("[!] Process started at: " + str(start_dt.strftime("%d/%m/%Y %H:%M:%S")))

parser = ArgumentParser(prog='upload2elastic', description='Push any CSV to ElasticSearch')
parser.add_argument('--server', '-s', dest='elastic_server', action='store', default=os.environ.get('ES_HOSTS', 'http://127.0.0.1:9200'), help='ElasticSearch server(s)')
parser.add_argument('--index',  '-i', dest='elastic_index',  action='store', default='%s' % hex(abs(hash(json.dumps(sys.argv[1:]))))[2:10], help='ElasticSearch index name')
parser.add_argument("paths", nargs=REMAINDER, help='Target audit log file(s)', metavar='paths')
args, extra = parser.parse_known_args(sys.argv[1:])

# Define the ElasticSearch client 
es = Elasticsearch(args.elastic_server, index=args.elastic_index)
print(f"[?] Using server: {args.elastic_server}")
print(f"[?] Using index: {args.elastic_index}")

# Open and yeild each row in the csv
def csv_reader(path, delimiter=','):
    with open(path, errors="ignore") as file_obj:
        reader = csv.DictReader(file_obj)
#        helpers.bulk(es, reader)
        for row in reader:
            print (row)
            doc = {
                "_type": "_doc",
                "_source": row
            }
            #print (doc)
            res = es.index(index=args.elastic_index, body=row)
            print(res['result'])
            #yield row 

if __name__ == "__main__":
    # Loop through each csv in the paths 
    for path in args.paths:
        # Check if the csv file exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"Audit log file {path} not found")
        
        print(f"[!] Importing {path} into memory for conversion ...")
        # Define the actions for each row in the csv
        csv_reader(path)
#        actions = [
#            {
#                "_index": args.elastic_index, 
#                "_id": hex(abs(hash(json.dumps(record, default=str)))), 
#                "_type": "_doc", 
#                "_source": record
#            } for record in csv_reader(path)
#        ]

#        print(f"[!] Pushing {path} into ElasticSearch...")

#        for ok, info in parallel_bulk(es, actions=actions):
#            if not ok: print(f"Error {info}")

    end_dt = datetime.datetime.now()
    duration_full = end_dt - start_dt
    print("[!] Processing finished at: " + str(end_dt.strftime("%d/%m/%Y %H:%M:%S")))
    print("[!] Total duration was: " + str(duration_full))
