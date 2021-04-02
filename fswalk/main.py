#!/usr/bin/env python3
from multiprocessing.pool import Pool
import os
import sys
import socket
import re
from optparse import OptionParser
import gzip
import pyjson5
import json
from collections import OrderedDict
import requests

try:
    from .workers import (explore_path, explore_path, parallel_worker, purge_index, unsearched)
except Exception: #ImportError
    from workers import (explore_path, explore_path, parallel_worker, purge_index, unsearched)

# Main program
def main():

    # Options parsing
    parser = OptionParser()
    parser.add_option("-p", "--path",
                      dest="path", default="./",
                      help="Path to scan")
    parser.add_option("-n", "--nproc",
                      dest="nproc", default="4",
                      help="Number of process to launch")
    parser.add_option("-x", "--exclude",
                      dest="exclude_expr", default="",
                      help="Regular expression for path exclusion")
    parser.add_option("-a", "--analyze",
                      dest="analyze_file", default="",
                      help="Creates a summary based on a previously generated json file")
    parser.add_option("-s", "--search",
                      dest="search_string", default="",
                      help="Search a subset of files with syntax: [uid]:[gid]:[path_glob]:[hostname] (--analyze or --elastic-host needed)")
    parser.add_option("--numeric",                                             
                      action="store_true", dest="numeric", default=False,                               
                      help="Output numeric uid/gid instead of names") 
    parser.add_option("--hostname",
                      dest="hostname", default=None,
                      help="Overwrite the value of the hostname string. Defaults to local hostname.")
    parser.add_option("-e", "--elastic-host",
                      dest="elastic_host", default=None,
                      help="Use an elasticsearch server for output. 'Ex: http://localhost:9200'")
    parser.add_option("-P", "--http-credentials",
                      dest="htauth", default=None,
                      help="File containing http credentials for elasticsearch if necessary. Syntax: <user>:<passwd>")
    parser.add_option("--elastic-index", dest='elastic_index', default="fswalk",
                     help="Name of the elasticsearch index")
    parser.add_option("--elastic-bulk-size", dest='max_bulk_size', default=1000,
                     help="Size of the elastic indexing bulks")
    parser.add_option("-g", "--elastic-purge-index",
                      action="store_true", dest="elastic_purge_index", default=False, 
                      help="Purge the elasticsearch index before indexing")
    parser.add_option("--no-check-certificate",
                      action="store_true", dest="no_check_certificate", default=False, 
                      help="Don't check certificates files when using SSL")
    (options, args) = parser.parse_args()
    
    # Analyze json file
    if options.analyze_file:
        import pwd
        import grp
        try:
            with gzip.open(options.analyze_file, mode='r') as json_data:
              data = pyjson5.load(json_data)
        except:
            with open(options.analyze_file) as json_data:
              data = pyjson5.load(json_data)
        finally:
            # Search
            if options.search_string:
                import fnmatch
                s_uid,s_gid,s_path=options.search_string.split(":")
                for file in data:
                    path=file['path']
                    if (s_uid == "*" or file['owner'] == int(s_uid)) and \
                       (s_gid == "*" or file['group'] == int(s_gid)) and \
                       fnmatch.fnmatch(path,s_path):
                           print(path)
            
            # or Sum
            else:
                users={}
                groups={}
                for file in data:
                    if file['owner'] in users:
                        users[file['owner']]['size'] += file['size']
                        users[file['owner']]['count'] += 1
                    else:
                        users[file['owner']]={ 'size' : file['size'] , 'count' : 1 }
                    if file['group'] in groups:
                        groups[file['group']]['size'] += file['size']
                        groups[file['group']]['count'] += 1
                    else:
                        groups[file['group']]={ 'size' : file['size'] , 'count' : 1 }
                # Users table
                print("{:<30} {:>16} {:>16}".format("User","Size","Count"))
                print("=================================================================")
                size=0
                count=0
                for user in sorted(users, key=lambda k: users[k]['size'], reverse=True):
                    if not options.numeric:
                        try:
                            user_name=pwd.getpwuid(user)[0]
                        except:
                            user_name=user
                    else:
                        user_name=user
                    print("{:<30} {:>16} {:>16}".format(user_name,users[user]['size'],users[user]['count']))
                    size += users[user]['size']
                    count += users[user]['count']
                # Groups table
                print("\n{:<30} {:>16} {:>16}".format("Group","Size","Count"))
                print("=================================================================")
                for group in sorted(groups, key=lambda k: groups[k]['size'], reverse=True):
                    if not options.numeric:
                        try:
                            group_name=grp.getgrgid(group)[0]
                        except:
                            group_name=group
                    else:
                        group_name=group
                    print("{:<30} {:>16} {:>16}".format(group_name,groups[group]['size'],groups[group]['count']))
                print("\nTOTAL SIZE: {}\nTOTAL FILES: {}".format(size,count))
        exit(0)
   
    # Search (elastic backend)
    if options.elastic_host and options.search_string:
        import fnmatch
        import elasticsearch
        import elasticsearch.helpers
        s_uid,s_gid,s_path,hostname=options.search_string.split(":")
        query_string="owner:{} AND group:{} AND path:{} AND hostname:{}".format(s_uid,s_gid,s_path,hostname)
        query = {
                "query": {
                    "query_string" : {
                        "query": query_string
                        }
                    }
                }
        if options.htauth is not None:
            string = open(options.htauth, 'r').read()
            htuser,htpassword=string.rstrip().split(":",1)
            http_auth=(htuser,htpassword)
        else:
            http_auth=None
        context = None
        if options.no_check_certificate:
            from ssl import create_default_context
            context = create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
        es = elasticsearch.Elasticsearch([options.elastic_host],http_auth=http_auth,context=context)
        results = elasticsearch.helpers.scan(es,
            index=options.elastic_index,
            size=int(options.max_bulk_size),
            query=query
        )
        for item in results:
            print(item["_source"]["path"])
        exit(0)


    # Main program (directory scan)
    if options.elastic_host:
        session = requests.Session()
        if options.no_check_certificate:
            session.verify=False
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
        if options.htauth is not None:
            string = open(options.htauth, 'r').read()
            htuser,htpassword=string.strip().split(":",1)
            session.auth = (htuser,htpassword)
        if options.elastic_purge_index : purge_index(options,session)
    else:
        session = None
        print("[")
    unsearched.put(options.path)
    if options.hostname:
        hostname = options.hostname
    else:
        hostname = socket.gethostname()
    pool = Pool(int(options.nproc))
    for i in range(int(options.nproc)):
        pool.apply_async(parallel_worker, args=(options,hostname,session))
    unsearched.join()
    if options.elastic_host : 
        session.close() 
    else : 
        print("]")

if __name__ == '__main__':
        main()
