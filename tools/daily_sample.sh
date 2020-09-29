#!/usr/bin/env bash

export ELASTIC_HOST=http://eli.u-ga.fr:9200
export SNAPSHOTS=eli

source ./rename_index.sh


fswalk -p /home -x '^/home/\.snapshot/' -n 8 --elastic-host=$ELASTIC_HOST --elastic-index=fswalk_home_new -g --elastic-bulk-size=5000
delete fswalk_home
rename fswalk_home_new fswalk_home