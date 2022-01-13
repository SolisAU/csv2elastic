#!/usr/bin/env python3
import math
from argparse import ArgumentParser, REMAINDER
from elasticsearch import helpers, Elasticsearch
import sys, os, csv, json, re, dateutil.parser, pprint, datetime
from alive_progress import alive_bar
import json
import numpy as np
import psutil
import apachelogs
import shlex

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

script_ver = "1.24"

print("[*] CSV 2 Elastic script v" + script_ver)
print("[!] Process started at: " + str(start_dt.strftime("%d/%m/%Y %H:%M:%S")))

parser = ArgumentParser(prog='upload2elastic', description='Push any CSV to ElasticSearch')
parser.add_argument('--server', '-s', dest='elastic_server', action='store', default=os.environ.get('ES_HOSTS', 'http://127.0.0.1:9200'), help='ElasticSearch server(s)')
parser.add_argument('--index',  '-i', dest='elastic_index',  action='store', default='%s' % hex(abs(hash(json.dumps(sys.argv[1:]))))[2:10], help='ElasticSearch index name')
parser.add_argument('--uningested_path',  '-e', dest='uningested_path',  action='store', default='uningested_logs.csv', help='Path and Filename for .csv file to store failed uningested logs')
parser.add_argument('--infile_type', '-t', dest='input_file_type', action='store', default='csv', help='Input file type, default csv, options(csv or json)')
parser.add_argument('--bulk', '-b', dest='bulk_api', action='store_true', help='Set to use ElasticSearch bulk API request vs individual reguest')
parser.add_argument('--pipeline', '-p', dest='pipeline', action='store', help='Set to use ElasticSearch pipeline')
parser.add_argument('--csv_delimiter', '-d', dest='csv_delimiter', action='store', default=',', help='Set the character for input_csv delimitation')
parser.add_argument("paths", nargs=REMAINDER, help='Target audit log file(s)', metavar='paths')
args, extra = parser.parse_known_args(sys.argv[1:])

# Define the ElasticSearch client
es = Elasticsearch(args.elastic_server, index=args.elastic_index)
print(f"[?] Using server: {args.elastic_server}")
print(f"[?] Using index: {args.elastic_index}")

# Constants
MAX_CHUNK_SIZE = 56000
LOG_PARSER_FORMAT = "%h %l %u %t \"%r\" %s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Host}i\" \"%{Cookie}i\" %{index}c"


def find_missing(lst, full_count):
    start, end = 1, full_count
    return sorted(set(range(start, end + 1)).difference(lst))


def handle_combined_log_errors(path, parsed_output, temp_rows, full_count):
    global error_count
    temp_index = []
    for i in parsed_output:
        temp_index.append(int(i.cryptography["index"]))

    error_index = find_missing(temp_index, full_count)
    for i in error_index:
        print(temp_rows[i - 1])
        head, tail = os.path.split(path)
        error_count += 1
        # create entry in error file
        to_csv(tail + '_error.csv', ['error on line', 'error msg', 'original_data'], {'error on line': i, 'error msg': 'Failed, apachelogs could not parse row', 'original_data': temp_rows[i - 1]})


def combined_log_to_json(path, rows):
    global error_count
    temp_rows = []

    print("[!] Removing unwanted data from combined log")
    with alive_bar(len(rows), force_tty=True) as bar:
        counter = 1
        for row in rows:
            try:
                temp_row = shlex.split(row)

                # Remove data from original that we don't want to pass to apachelogs for parsing
                del temp_row[5]
                del temp_row[8]
                del temp_row[8]

                # Building a new string from after removing unwanted data
                temp_rows.append(temp_row[0] + " " + temp_row[1] + " " + temp_row[2] + " " + temp_row[3] + " " +
                                 temp_row[4] + " \"" + temp_row[5] + "\"" + " " + temp_row[6] + " " + temp_row[7] + " \"" +
                                 temp_row[8] + "\"" + " \"" + temp_row[9] + "\"" + " \"" + temp_row[14] + "\"" + " \"" +
                                 temp_row[15] + "\"" + " " + str(counter))
                bar(1)
                counter += 1
            except Exception as e:
                print('failed to remove dup: ', e)
                head, tail = os.path.split(path)
                error_count += 1
                to_csv(tail + '_error.csv', ['error on line', 'error msg', 'original_data'],
                       {'error on line': counter, 'error msg': e,
                        'original_data': row})

    # Parse strings with apachelogs applying the LOG_PARSER_FORMAT

    parsed_output_count = len(list(apachelogs.parse_lines(LOG_PARSER_FORMAT, temp_rows, ignore_invalid=True)))
    parsed_output_errors = apachelogs.parse_lines(LOG_PARSER_FORMAT, temp_rows, ignore_invalid=True)
    parsed_output = apachelogs.parse_lines(LOG_PARSER_FORMAT, temp_rows, ignore_invalid=True)

    # print(parsed_output_count)
    # print(len(temp_rows))

    if parsed_output_count != len(temp_rows):
        handle_combined_log_errors(path, parsed_output_errors, temp_rows, len(temp_rows))


    # sys.exit()

    # Clear temp_rows
    temp_rows = []

    # Build objects to be converted to json strings
    print("[!] Building json data from combined log")
    with alive_bar(len(rows), force_tty=True) as bar:
        counter = 1
        for i in parsed_output:
            # Check if request_line can be split to split i.e GET from /path
            try:
                request_lenth = i.request_line.split(' ', 1)
                if len(request_lenth) > 1:
                    request_line = i.request_line.split(' ', 1)[1]
                    request_method = i.request_line.split(' ', 1)[0]
                else:
                    request_line = ""
                    request_method = ""

                # Printing used for debugging
                # print(i.remote_host, i.remote_logname, i.remote_user, i.request_time_fields["timestamp"], i.request_line,
                #       i.status, i.bytes_sent, i.headers_in["Referer"], i.headers_in["User-Agent"], i.headers_in["Host"],
                #       i.headers_in["Cookie"])
                parsed_object = {"remote_host": i.remote_host, "remote_logname": i.remote_logname, "remote_user": i.remote_user,
                         "@timestamp": i.request_time_fields["timestamp"].strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                         "request_line": request_line, "request_method": request_method,
                         "status": i.status, "bytes_sent": i.bytes_sent, "referer": i.headers_in["Referer"],
                         "user_agent": i.headers_in["User-Agent"], "host": i.headers_in["Host"],
                         "Cookie": i.headers_in["Cookie"]}

                temp_rows.append(json.dumps(parsed_object))
                bar(1)
                counter += 1
            except Exception as e:
                print('Failed to build json: ', e)
                head, tail = os.path.split(path)
                error_count += 1
                to_csv(tail + '_error.csv', ['error on line', 'error msg'],
                       {'error on line': counter, 'error msg': e})
    return temp_rows


# Save row .csv
def to_csv(path, csv_columns, dict_row, delimeter=','):
    with open(path, 'a') as fd:
        writer = csv.DictWriter(fd, fieldnames=csv_columns)
        if fd.tell() == 0:
            writer.writeheader()
        writer.writerow(dict_row)


def parralel_bulk_ingest(path, rows, unigested_path, pipeline):
    global error_count

    # Calculate how many chunks to pass to Elastic
    # chunk_count = math.ceil(len(rows) / MAX_CHUNK_SIZE)
    # chunks = np.array_split(rows, chunk_count)

    with alive_bar(len(rows), force_tty=True) as bar:
        try:
            for success, info in helpers.parallel_bulk(es, rows, index=args.elastic_index, pipeline=pipeline, thread_count=(psutil.cpu_count() * 2)):
                bar(1)
        except Exception as e:
            api_error_count = int(str(e).split(',')[0].split(' ')[0][2:])
            error_count += api_error_count
            bar(len(rows) - api_error_count)
            print(e)

    return error_count


def bulk_ingest(path, rows, unigested_path, pipeline):
    global error_count

    # Calculate how many chunks to pass to Elastic
    chunk_count = math.ceil(len(rows) / MAX_CHUNK_SIZE)
    chunks = np.array_split(rows, chunk_count)

    with alive_bar(len(rows), force_tty=True) as bar:
        for chunk in chunks:
            try:
                result = helpers.bulk(es, chunk, index=args.elastic_index, pipeline=pipeline)
                print(result)
                bar(result[0])
            except Exception as e:
                api_error_count = int(str(e).split(',')[0].split(' ')[0][2:])
                error_count += api_error_count
                bar(len(chunk) - api_error_count)
                print(e)

    return error_count


def single_ingest(path, rows, uningested_path, fieldnames=None, pipeline=None):
    global error_count
    count = 0
    with alive_bar(len(rows), force_tty=True) as bar:
        for row in rows:
            try:
                res = es.index(index=args.elastic_index, document=row, pipeline=pipeline)
                result_success = res['result']
                count += 1
                bar()

            except KeyboardInterrupt:
                print('csv2elastic exit by ctl-c')
                sys.exit(1)

            except Exception as e:
                head, tail = os.path.split(path)
                error_count += 1
                print(e)
                # create entry in error file
                to_csv(tail + '_error.csv', ['error on line', 'error msg'], {'error on line': str(count + 2), 'error msg': e})

                # create entry in uningested file
                if fieldnames:
                    to_csv(tail + '_' + uningested_path, fieldnames, row)
    return error_count


# Open and yeild each row in the csv
def doc_reader(path, uningested_path, infile_type, bulk, pipeline, delimiter=','):
    global error_count
    with open(path, errors='ignore') as file_obj:
        if infile_type == 'csv':
            reader = csv.DictReader(file_obj, delimiter=delimiter)
            rows = list(reader)
        elif infile_type == 'json':
            rows = list(file_obj)
        elif infile_type == 'combined_logs':
            pre_proc_rows = list(file_obj)
            rows = combined_log_to_json(path, pre_proc_rows)

        if bulk:
            error_count = parralel_bulk_ingest(path, rows, uningested_path, pipeline)
        else:
            error_count = single_ingest(path, rows, uningested_path, reader.fieldnames, pipeline)

    return error_count


if __name__ == "__main__":
    global error_count
    error_count = 0

    # Loop through each csv in the paths 
    for path in args.paths:
        # Check if the csv file exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"Audit log file {path} not found")
        
        print(f"[!] Importing {path} into memory for conversion ...")
        # Define the actions for each row in the csv
        error_count = doc_reader(path, args.uningested_path, args.input_file_type, args.bulk_api, args.pipeline, args.csv_delimiter)

    end_dt = datetime.datetime.now()
    duration_full = end_dt - start_dt
    print("[!] Error count: " + str(error_count))
    print("[!] Processing finished at: " + str(end_dt.strftime("%d/%m/%Y %H:%M:%S")))
    print("[!] Total duration was: " + str(duration_full))
