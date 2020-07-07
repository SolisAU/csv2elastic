# csv2elastic
Python script to push any CSV file into ElasticSearch via HTTP Post

Really simple script to take a CSV file with headers and push to ElasticSearch using HTTP Post.

Usage
csv2elastic.py --index index_name --server http://elasticsearch:9200 inputfile1.csv inputfile2.csv inputfile3.csv
