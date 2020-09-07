# fs_walk
An efficient multiprocessing directory walk and analyze tool

## Introduction
fs_walk is a simple python script that recursively walks through a filesystem 
directory to gather files meta-data and collect them into a json file.
It runs several processes, each responsible of doing the list of the files
contained into a subdirectory.
Collected meta-data are `filename, path, uid, gid, size` and `atime`.

The script aslo provides an option to do a quick analyze of the resulting
output file.

## Example

Start a walk into the `/home/bzizou` directory with 8 process, excluding 
the `.snapshot`subdirectory:

```
bzizou@f-dahu:~/git/fs_walk$ ./fs_walk.py -p /home/bzizou -x '^/home/bzizou/\.snapshot/' -n 8 |gzip > /tmp/out.gz    
```

Analyze the output:

```
bzizou@f-dahu:~/git/fs_walk$ ./fs_walk.py -a /tmp/out.gz
User                                       Size            Count
=================================================================
bzizou                               2749804131            11125
root                                 1030651826             1351
1000                                  390705282              476
11610                                    726417                7

Group                                      Size            Count
=================================================================
realuser                             2749795275            11119
root                                 1030660332             1356
1000                                  390705282              476
2222                                     726417                7
staff                                       350                1

TOTAL SIZE: 4171887656
TOTAL FILES: 12959
```

## Usage
```
Usage: fs_walk.py [options]

Options:
  -h, --help            show this help message and exit
  -p PATH, --path=PATH  Path to scan
  -n NPROC, --nproc=NPROC
                        Number of process to launch
  -x EXCLUDE_EXPR, --exclude=EXCLUDE_EXPR
                        Regular expression for path exclusion
  -a ANALYZE_FILE, --analyze=ANALYZE_FILE
                        Creates a summary based on a previously generated file
  --numeric             Output numeric uid/gid instead of names
```

The `ANALYZE_FILE` parameter may be a gzip compressed json file or a plain-text json file.
