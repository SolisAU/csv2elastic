# csv2elastic

Really simple script to take a CSV file with headers and push to ElasticSearch using HTTP Post.
On the event of an ingestion error a uningested_logs.csv and error.csv file will be created.
(uningested_logs.csv) to store the .csv rows that failed to target reingestion.
(error.csv) to store the error message and the line number of the failed row.

## Usage
csv2elastic.py --index index_name --server http://elasticsearch:9200 inputfile1.csv inputfile2.csv inputfile3.csv

## Dependencies

The following python modules are required:
* dateutil
* elasticsearch
* about-time
* alive-progress
* certifi
* elasticsearch
* grapheme
* python-dateutil
* six
* urllib3

Install with `pip install <modulename>`

Alternitively install all requirement with `pip install requirements.txt`
