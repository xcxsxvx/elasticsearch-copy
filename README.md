# Prerequisites
Required python modules
- [elasticsearch](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)
- argparse 

# Usage
```
usage: elasticsearch_cp.py [-h] [--input INPUT] [--output OUTPUT]
                           [--input-index-name INPUT_INDEX_NAME]
                           [--output-index-name OUTPUT_INDEX_NAME]
                           [--query-string QUERY_STRING]
                           [--num-shards NUM_SHARDS]
                           [--records-per-batch RECORDS_PER_BATCH]
                           [--timeout TIMEOUT] [--optimize]

Copy data from one index to another.

optional arguments:
  -h, --help            show this help message and exit
  --input INPUT         the input source. url of an elasticsearch service or
                        filename
  --output OUTPUT, -o OUTPUT
                        the output source. url of an elasticsearch service or
                        filename
  --input-index-name INPUT_INDEX_NAME, --input-index INPUT_INDEX_NAME
                        the name of the elasticsearch index we're reading from
  --output-index-name OUTPUT_INDEX_NAME, --output-index OUTPUT_INDEX_NAME
                        the name of the elasticsearch index to which we're
                        writing
  --query-string QUERY_STRING, --query QUERY_STRING
                        an arbitrary query string with which to select data
  --num-shards NUM_SHARDS, --shards NUM_SHARDS
                        number of shards for the newly created indexes
  --records-per-batch RECORDS_PER_BATCH, --records RECORDS_PER_BATCH
                        how many records to fetch at a time
  --timeout TIMEOUT     elasticsearch timeout
  --optimize            use this flag to optimize the index after copying it
                        (sets max_num_segments to 1)
```

