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
            fullname = os.path.join(path, entry.name)
            if not entry.is_symlink():
                if entry.is_dir():
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
                    "owner" : statinfo.st_uid,
                    "owner_name" : users[statinfo.st_uid],
                    "group" : statinfo.st_gid,
                    "group_name" : groups[statinfo.st_gid],
                    "mode" : statinfo.st_mode,
                    "size" : statinfo.st_size,
                    "atime" : statinfo.st_atime,
                    "hostname" : hostname,
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
