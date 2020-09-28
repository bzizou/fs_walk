#!/usr/bin/env bash

if [ "$ELASTIC_HOST" = "" ]
then
    ELASTIC_HOST=http://localhost:9200
fi
if [ "$SNAPSHOTS" = "" ]
then
    SNAPSHOTS=backup
fi

set -e

function delete() {
    curl -s -XDELETE $ELASTIC_HOST/$1 > /dev/null
    curl -s -XDELETE $ELASTIC_HOST/_snapshot/$SNAPSHOTS/$1_snapshot > /dev/null
}

function snapshot() {
    tmp=`mktemp`
    curl -s -XPUT -H'content-type: application/json' \
         $ELASTIC_HOST/_snapshot/$SNAPSHOTS/$1_snapshot?wait_for_completion=true \
         -d "{\"indices\": \"$1\"}" > $tmp
    status=`jq -r '.snapshot.state' $tmp`
    if [ "$status" \!= "SUCCESS" ]
    then
        echo "Error status : $status"
        cat $tmp
        rm $tmp
        exit 3 
    fi
}

function rename() {
    tmp=`mktemp`
    snapshot $1
    curl -s -XPOST -H'content-type: application/json' \
         $ELASTIC_HOST/_snapshot/$SNAPSHOTS/$1_snapshot/_restore?wait_for_completion=true \
         -d "{
               \"indices\": \"$1\",
               \"ignore_unavailable\": \"true\",
               \"include_global_state\": false,
               \"rename_pattern\": \"$1\",
               \"rename_replacement\": \"$2\" }" > $tmp
    if [ "`jq -r '.snapshot.shards.successful' $tmp`" = "1" ]
    then
       delete $1
    else
      echo "Error while restoring snapshot"
      cat $tmp
      rm $tmp
      exit 3
    fi
}


# Allow sourcing functions
if [ "${BASH_SOURCE[0]}" = "${0}" ]
then

    if [ "$#" \!= "2" ]
    then
      echo "Usage: $0 old_index_name new_index_name"
      exit 1
    fi

    rename $1 $2
    rm $tmp
fi
