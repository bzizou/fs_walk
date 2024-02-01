#!/usr/bin/env python3
import opensearchpy
import elasticsearch.helpers
import json
import sys
import ssl
import requests
import datetime
from ssl import create_default_context

# Config
credentials = open('/etc/eli_credentials_fswalk').read().strip('\n')
url="https://"+credentials+"@eli.univ-grenoble-alpes.fr:443/opensearch-gricad-boards-os"
indices = {
  "fswalk_home_luke" : [ "luke", "home" ],
  "fswalk_home" : [ "dahu", "home" ],
  "fswalk_silenus" : [ "silenus", "scratch" ],
  "fswalk_bettik" : [ "bettik", "scratch" ]
}

output_index = "fswalk_hist"
timeout=300

# SSL config
# Disabling cert verification as it does not work, despite eli has
# an official certificate
context = create_default_context(cafile="/etc/ssl/certs/ca-certificates.crt")
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

(user,password)=credentials.split(":")

es = opensearchpy.OpenSearch(
  url,
  basic_auth=(user,password),
  ssl_context=context,
  request_timeout=timeout
)

# Aggregate search by temperatures
def aggregate(index,field):
    if field in ["owner_name","group_name"]:
      field = field + ".keyword"
    body={ 
           "aggs": { "2": { "terms": { 
                              "field": field,
                              "size": 10000,
                            }, 
                            "aggs": { 
                              "1": { "sum": {"field": "size"} }
                            }
                          }
           }, 
           "query": { 
             "bool": { "must": [ {"match_all": {}} ] 
             }
           }
         }
    return es.search(index=index,body=body)

# Main
for index in indices:
  print("\n"+index+" : ")
  for field in ["temperature","owner_name","group_name"]:
    print(field+" ",end='', flush=True)
    try:
      result=aggregate(index,field)
    except elasticsearch.exceptions.NotFoundError:
      print("ERROR: "+index+" not found!")
    except:
      print("Unexpected error:", sys.exc_info()[0]) 
      raise
    body=""
    if "aggregations" in result:
      for row in result['aggregations']['2']['buckets']:
        body+="{ \"create\" : { \"_index\" : \""+output_index+"\" } }\n"
        body+="{ \"type\": \""+field+"\","
        body+=" \"@timestamp\": \""+datetime.datetime.now().replace(microsecond=0).isoformat()+"\","
        body+=" \"host\": \""+indices[index][0]+"\","
        body+=" \"space\": \""+indices[index][1]+"\","
        body+=" \"key\":\""+str(row['key'])+"\","
        body+=" \"size\":"+str(row['1']['value'])+","
        body+=" \"number\":"+str(row['doc_count'])+" }\n"
      res=es.bulk(body=body)
