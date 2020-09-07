#!/usr/bin/env python3

# fs_walk scans a directory recursively, in parallel
# and outputs a json list of files meta-data 
# (name, size, owner, group and atime)
#
# Example:
#   fs_walk.py -p /home -x '^/home/\.snapshot/' -n 8 |gzip > /home/home_walk.json.gz
#   fs_walk.py -a /home/home_walk.json.gz
#
# This scans /home with 8 process and excludes /home/.snapshot/ from the scan
# Then it generates and prints a summary
#
# Warning: an extraneous "," sign might break json compatibility. It can't 
# be removed because of the parallelisation optimization. So you might have
# to use a json5 decoder, to allow such syntax.

from multiprocessing.pool import Pool
from multiprocessing import JoinableQueue as Queue 
#from multiprocessing import Manager
import os
import sys
import json
import socket
import re
from optparse import OptionParser
import gzip

# Scans a directory and prints stats in json 
def explore_path(path):
    global options
    if options.exclude_expr:
        if re.match(options.exclude_expr, path):
            return []
    data={}
    directories = []
    nondirectories = []
    global hostname
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
                data={
                    "path" : str(fullname.encode('utf-8')),
                    "owner" : statinfo.st_uid,
                    "group" : statinfo.st_gid,
                    "mode" : statinfo.st_mode,
                    "size" : statinfo.st_size,
                    "atime" : statinfo.st_atime,
                    "hostname" : hostname
                }
                print(json.dumps(data,indent=1)+",")
    except:
        print("Error in {} (mssing file?)".format(path),file=sys.stderr)
        sys.stderr.flush()

    sys.stdout.flush()
    return directories

# Worker for multiprocessing search of files
def parallel_worker():
    while True:
        path = unsearched.get()
        dirs = explore_path(path)
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()

# Probably never called as we catch exceptions inside the worker
def print_error(err):
    print(err)

if __name__ == "__main__":

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
                      help="Creates a summary based on a previously generated file")
    parser.add_option("--numeric",                                             
                      action="store_true", dest="numeric", default=False,                               
                      help="Output numeric uid/gid instead of names") 
    (options, args) = parser.parse_args()
    
    # Analyze
    if options.analyze_file:
        import pyjson5
        import pwd
        import grp
        try:
            with gzip.open(options.analyze_file) as json_data:
              data = pyjson5.load(json_data)
        except:
            with open(options.analyze_file) as json_data:
              data = pyjson5.load(json_data)
        finally:
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
    
    # Main program (directory scan)
    print("[")
    unsearched = Queue()
    unsearched.put(options.path)
    hostname = socket.gethostname()
    pool = Pool(int(options.nproc))
    for i in range(int(options.nproc)):
        pool.apply_async(parallel_worker,error_callback=print_error)
    unsearched.join()
    print("]")
