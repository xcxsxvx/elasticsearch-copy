#!/usr/bin/env python
#

import os
import sys
import argparse
import re
import elasticsearch
from elasticsearch import helpers
import subprocess
import time
import json

#with open('cloudalytics_mappings.json', 'r') as infile:
#  MAPPING_STR = infile.read()

parser = argparse.ArgumentParser(description='Copy data from one index to another.')
parser.add_argument('--input', default='http://soa-test-hdp2-data1.phx1.jivehosted.com:9200', help='the input source. url of an elasticsearch service or filename')
parser.add_argument('--output', '-o', default='http://soa-test-hdp2-data1.phx1.jivehosted.com:9200', help='the output source. url of an elasticsearch service or filename')
parser.add_argument('--input-index-name', '--input-index', default='DEFAULT', help='the name of the elasticsearch index we\'re reading from')
parser.add_argument('--output-index-name', '--output-index', default='DEFAULT', help='the name of the elasticsearch index to which we\'re writing')
parser.add_argument('--query-string', '--query', help='an arbitrary query string with which to select data')
parser.add_argument('--num-shards', '--shards', default=3, help='number of shards for the newly created indexes')
parser.add_argument('--records-per-batch', '--records', default=100, type=int, help='how many records to fetch at a time')
parser.add_argument('--timeout', default=240, help='elasticsearch timeout')
parser.add_argument('--optimize', action='store_true', help='use this flag to optimize the index after copying it (sets max_num_segments to 1)')

args = parser.parse_args()

input  = dict()
output = dict()
input['from_es']   = True
input['from_file'] = False
output['to_es']    = True
output['to_file']  = False

if re.search('^https?://', args.input):
  input['es'] = elasticsearch.Elasticsearch([args.input],
                        timeout=args.timeout,
                        sniff_on_start=True,
                        sniff_on_connection_fail=True,
                        sniffer_timeout=120)
  if not re.search('/$', args.input):
    input['es_url'] = args.input + '/'
else:
  if not os.path.exists(args.input):
    raise SystemExit("[FATAL] Provided --input is neither a url or a file")
  else:
    input['from_es']   = False
    input['from_file'] = True
    input['filename']  = args.input

if re.search('^https?://', args.output):
  output['es'] = elasticsearch.Elasticsearch([args.output], timeout=args.timeout,
                                             sniff_on_start=True,
                                             sniff_on_connection_fail=True,
                                             sniffer_timeout=120)
  output['i_client'] = elasticsearch.client.IndicesClient(output['es'])
  if not re.search('/$', args.output):
    output['es_url'] = args.output + '/'
else:
  output['to_es']    = False
  output['to_file']  = True
  output['filename'] = args.output

if args.input_index_name == 'DEFAULT' and not input['from_file']:
  raise SystemExit("[FATAL] You must specify --input-index-name")
if args.output_index_name == 'DEFAULT':
  # assume they want the same name
  args.output_index_name = args.input_index_name
if args.input == args.output and args.input_index_name == args.output_index_name:
  raise SystemExit("[FATAL] You must specify a unique --output-index-name when --input and --output are the same")

def get_hits(es, index_name, query_str):
  hits = 0
  try:
    res = es.search(index=index_name, body=query_str, filter_path=['hits.hits.total'])
    hits = res['hits']['total']
    print("[INPUT ] Got %d hits in %s for query: %s" % (hits,
                                                        index_name,
                                                        query_str))
  except (elasticsearch.exceptions.NotFoundError,elasticsearch.exceptions.AuthorizationException) as e:
    print("[INPUT ] No such index '%s' (or it's closed)" % index_name)

  return hits

def prepare_output_file(filename):
  # TODO: here's where a smart person would add code to
  # check for the existence of the output file
  f = open(filename, 'w')
  return f

def prepare_output_index(es, i_client, output_index_name, query_str, index_create_str):
  try:
    res_output = es.search(index=output_index_name, body=query_str, filter_path=['hits.hits.total'])
    total_hits = res_output['hits']['total']
    print("[OUTPUT] Got %d hits in %s"   % (total_hits, output_index_name))
  except elasticsearch.exceptions.NotFoundError as e:
    print("[OUTPUT] No such index '%s'"  % output_index_name)
    print("[OUTPUT] Creating index '%s'" % output_index_name)
    i_client.create(index=output_index_name, body=index_create_str, timeout=args.timeout, master_timeout=args.timeout)

def copy_index_creation(input_es, input_index_name, output_es, output_i_client, output_index_name, query_str):
  try:
    res_output = output_es.search(index=output_index_name, body=query_str)
    total_hits = res_output['hits']['total']
    print("[OUTPUT] Got %d hits in %s"   % (total_hits, output_index_name))
  except elasticsearch.exceptions.NotFoundError as e:
    print("[OUTPUT] No such index '%s'"  % output_index_name)
    print("[OUTPUT] Creating index '%s'" % output_index_name)
    input_index_client = elasticsearch.client.IndicesClient(input_es)
    old_settings = input_index_client.get_settings(input_index_name)
    old_mappings = input_index_client.get_mapping(input_index_name)
    new_settings = old_settings[input_index_name]
    new_mappings = old_mappings[input_index_name]

    new_settings.update(new_mappings)
    if args.num_shards:
      new_settings['settings']['index']['number_of_shards'] = args.num_shards
    index_create_str  = json.dumps(new_settings, sort_keys=True, indent=4, separators=(',', ': '))
    #print index_create_str
    output_i_client.create(index=output_index_name, body=index_create_str, timeout='240s', master_timeout='240s')

def output_es(es, records, start_record_num, end_record_num, total_records, per_batch):
  print("Inserting records %d through %d of %s" % (start_record_num, end_record_num,
    (str(total_records) if total_records > 0 else '???')))
  num_success, error_list = helpers.bulk(es, records, chunk_size=1000)
  if num_success != per_batch:
    print("[ERROR] %d of %d inserts succeeded!" % (num_success,per_batch))
    print("[ERROR] Errors:")
    print error_list

def output_json(filehandle, records, start_record_num, end_record_num, total_records):
  print("Writing records %d through %d of %d" % (start_record_num, end_record_num, total_records))
  for record in records:
    json.dump(record, filehandle)
    filehandle.write("\n")

def copy_index():

  query_str = '{"query":{"match_all":{}}}'
  if args.query_string:
    query_str = args.query_string

  if input['from_file']: input_index_name = 'NOINDEX'
  else                 : input_index_name = args.input_index_name
  
  output_index_name = args.output_index_name
  
  print
  if input['from_file']:
    print("Reading from file %s" % (input['filename']))
  else:
    print("Reading from ES %s" % (input['es_url'] + input_index_name))

  if output['to_file']:
    outfile = output['filename'] + '_' + output_index_name + '.json'
    print("Writing to file %s" % (outfile))
  else:
    print("Writing to ES %s" % (output['es_url'] + output_index_name))
    if output_index_name == 'DEFAULT':
      print("(I will use the _index name from the originating data)")

  resp = raw_input("Continue? ")
  if not re.search('^[Yy]', resp):
    raise SystemExit("Bye") 

  print(input_index_name)
  
  input_hits = 0
  if input['from_es']:
    input_hits = get_hits(input['es'], input_index_name, query_str)
    
    if input_hits < 1:
      return
  
  if output['to_file']:
    filehandle = prepare_output_file(outfile)
  else:
    copy_index_creation(input['es'], input_index_name, output['es'], output['i_client'], output_index_name, query_str)
#    if input['from_file']:
#      filehandle = prepare_output_file(outfile)
#    else:
#      filehandle = prepare_output_file(output['filename'] + '_' + output_index_name + '.json')
# let ES create the index automagically (if it doesn't exist), because who knows what the original
# index looked like?
#  elif args.create_cloudalytics_index:
#    prepare_output_index(output['es'], output['i_client'], output_index_name, query_str, index_create_str)
  
  i = 0
  actions = []
  if input['from_file']:
    # TODO: check for file existence, etc
    input_data = open(input['filename']).readlines()
  else:
    input_data = helpers.scan(input['es'], index=input_index_name, query=query_str, size=args.records_per_batch)
  
  for record in input_data:
  
    # if we're reading from a file, then it's json data
    if isinstance(record, str):
      record = json.loads(record)
  
    i += 1
    if output_index_name != 'DEFAULT':
      record['_index'] = output_index_name
    actions.append(record)
    if i % args.records_per_batch == 0:
      if output['to_file']:
        output_json(filehandle, actions, i-args.records_per_batch+1, i, input_hits)
      else:
        output_es(output['es'], actions, i-args.records_per_batch+1, i, input_hits, args.records_per_batch)
  
      actions = []
  
  # write out the last remaining records
  remaining_records = 0
  remaining_records = len(actions)
  if remaining_records > 0:
    if output['to_file']:
      output_json(filehandle, actions, i-remaining_records+1, i, input_hits)
    else:
      output_es(output['es'], actions, i-remaining_records+1, i, input_hits, remaining_records)
  
  if output['to_file']: filehandle.close
  else:
    if args.optimize:
      print("[OUTPUT] Now optimizing output index %s ..." % output_index_name)
      output['i_client'].optimize(index=output_index_name, max_num_segments=1)

# main
copy_index()
