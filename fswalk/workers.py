#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
from multiprocessing import JoinableQueue as Queue 
unsearched = Queue()                               
import logging
import sys
import os
import pyjson5
import json
import requests
import re
import time
import pwd
import grp
import datetime



# Setup a logger as a thread-safe output
# as we can't use directly stdout, because threads may mix their outputs
log = logging.getLogger()
errlog = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

# Set the temperature of the data
def get_temp(age):
    if age > 3600*24*365*5:
        return 1 # > 5 years
    if 3600*24*365*2 <= age < 3600*24*365*5:
        return 2 # < 5 years
    if 3600*24*365 <= age < 3600*24*365*2:
        return 3 # < 2 years
    if 3600*24*30*6 <= age < 3600*24*365:
        return 4 # < 1 year
    if 3600*24*30 <= age < 3600*24*30*6:
        return 5 # < 6 months
    if 3600*24*7 <= age < 3600*24*30:
        return 6 # < One month
    if 0 <= age < 3600*24*7:
        return 7 # Less than a week
  
# Scans a directory and prints stats in json 
def explore_path(path,options,hostname,session):
    if options.exclude_expr:
        if re.match(options.exclude_expr, path):
            return []
    data={}
    directories = []
    nondirectories = []
    bulk=''
    bulk_size=0
    max_bulk_size = int(options.max_bulk_size)
    elastic = True if options.elastic_host is not None else False
    elastic_index = options.elastic_index
    users={}
    groups={}
    timestamp=datetime.datetime.now().isoformat()
    try:
        for entry in os.scandir(path):
            is_dir=0
            fullname = os.path.join(path, entry.name)
            elems=fullname.split('/',6)
            l1="/"+elems[1]
            l2=l1
            l3=l1
            l4=l1
            l5=l1
            if len(elems) == 3:
              l2=l1+"/"+elems[2]
              l3=l2
              l4=l2
              l5=l2
            if len(elems) == 4:
              l2=l1+"/"+elems[2]
              l3=l2+"/"+elems[3]
              l4=l3
              l5=l3
            if len(elems) == 5:
              l2=l1+"/"+elems[2]
              l3=l2+"/"+elems[3]
              l4=l3+"/"+elems[4]
              l5=l4
            if len(elems) == 6 or len(elems) == 7 :
              l2=l1+"/"+elems[2]
              l3=l2+"/"+elems[3]
              l4=l3+"/"+elems[4]
              l5=l4+"/"+elems[5]
            if not entry.is_symlink():
                if entry.is_dir():
                    is_dir=1
                    directories.append(fullname)
                    statinfo = entry.stat() 
                else:
                    nondirectories.append(fullname)
                    statinfo = entry.stat()
                if not statinfo.st_uid in users:
                    try:
                        users[statinfo.st_uid]=pwd.getpwuid(statinfo.st_uid)[0]
                    except:
                        users[statinfo.st_uid]=str(statinfo.st_uid)
                if not statinfo.st_gid in groups:
                    try:
                        groups[statinfo.st_gid]=grp.getgrgid(statinfo.st_gid)[0]
                    except:
                        groups[statinfo.st_gid]=str(statinfo.st_gid)
                data={
                    "path" : fullname.encode('utf-8','replace'),
                    "path_l1" : l1.encode('utf-8','replace'),
                    "path_l2" : l2.encode('utf-8','replace'),
                    "path_l3" : l3.encode('utf-8','replace'),
                    "path_l4" : l4.encode('utf-8','replace'),
                    "path_l5" : l5.encode('utf-8','replace'),
                    "owner" : statinfo.st_uid,
                    "owner_name" : users[statinfo.st_uid],
                    "group" : statinfo.st_gid,
                    "group_name" : groups[statinfo.st_gid],
                    "mode" : statinfo.st_mode,
                    "size" : statinfo.st_size,
                    "atime" : statinfo.st_atime,
                    "mtime" : statinfo.st_mtime,
                    "ctime" : statinfo.st_ctime,
                    "last_amc_time" : max(statinfo.st_atime,statinfo.st_mtime,statinfo.st_ctime),
                    "hostname" : hostname,
                    "temperature" : get_temp(datetime.datetime.now().timestamp()-max(statinfo.st_atime,statinfo.st_mtime,statinfo.st_ctime)),
                    "is_dir" : is_dir,
                    "@timestamp" : timestamp
                }
                if elastic:
                    bulk+='{ "create" : { "_index" : "'+elastic_index+'" }}\n'
                    bulk+=pyjson5.dumps(data,indent=1)+'\n'
                    bulk_size+=1
                    if bulk_size >= max_bulk_size:
                        index_bulk(bulk,options,session)
                        bulk_size=0
                        bulk=''
                else:
                    log.info(pyjson5.dumps(data,indent=1)+",")
        if elastic and bulk != '':
            index_bulk(bulk,options,session)
        if elastic:
            session.close()
    except Exception as e:
        print(path + ": ",e,file=sys.stderr)
        sys.stderr.flush()

    sys.stdout.flush()
    return directories

# Worker for multiprocessing search of files
def parallel_worker(options,hostname,session):
    while True:
        path = unsearched.get()
        dirs = explore_path(path,options,hostname,session)
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()

# Elastisearch bulk indexation
def index_bulk(bulk,options,session):
    """Do a bulk indexing into elasticsearch"""
    elastic_host = options.elastic_host
    url = "{elastic_host}/_bulk/".format(elastic_host=elastic_host)
    headers = {"Content-Type": "application/x-ndjson"}
    tries=0
    while True:
        try:
            r = session.post(url=url, headers=headers, data=bulk)
        except requests.exceptions.ConnectionError:
            errlog.warning("Connection error, retrying in 5s...")
            time.sleep(5)
            tries+=1
            if tries > 50:
                errlog.error("Too many connection errors: %s %s", r.status_code, r.text)
                break
        else:
            break
    if r.status_code != 200:
        errlog.warning("Got http error from elastic: %s %s" , r.status_code , r.text)
    response=json.loads(r.text)
    if response["errors"]:
        errlog.warning("Elastic status is ERROR!")
        for item in response["items"]:
            for key in item:
                it=item[key]
                if it["status"] != 201 :
                    errlog.warning("Status %s for %s action:", it["status"], key)
                    errlog.warning(json.dumps(item[key]))
    else:
        errlog.debug("Elastic bulk push ok: took %s ms" , response["took"])

# Purge elasticsearch index
def purge_index(options,s):
    r = s.delete(url=options.elastic_host + "/" + options.elastic_index)
