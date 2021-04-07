#!/usr/bin/env bash

set -e

function delete() {                                                                           
    curl $INSECURE -s -XDELETE $ELASTIC_HOST/$1 > /dev/null                                   
}                                                                                             

function get_old_index() {
  tmp=`mktemp`
  curl $INSECURE -s -XGET -H'content-type: application/json' \
       $ELASTIC_HOST/$1?pretty > $tmp
  old_index=`cat $tmp|jq -r ".[].settings.index.provided_name" 2>/dev/null||true`
  rm $tmp
  echo $old_index
}

function delete_alias() {
    old_index=`get_old_index $1`
    if [ "$old_index" = "null" ]
    then
      echo "Warning: old index not found"
    else
      curl $INSECURE -s -XPOST -H'content-type: application/json' \
         $ELASTIC_HOST/_aliases \
         -d "{
               \"actions\": [
                 { \"remove\" : { \"index\" : \"$old_index\", \"alias\": \"$1\" } }
               ]
             }" > $tmp
      if [ "`jq -r '.acknowledged' $tmp`" \!= "true" ]
      then
        echo "Warning: Error while deleting old alias"
        cat $tmp
        rm $tmp
      fi
    fi
}

function re_alias() {
    tmp=`mktemp`
    delete_alias $2
    curl $INSECURE -s -XPOST -H'content-type: application/json' \
         $ELASTIC_HOST/_aliases \
         -d "{
               \"actions\": [
                 { \"add\" : { \"index\" : \"$1\", \"alias\": \"$2\" } }
               ]
             }" > $tmp
    if [ "`jq -r '.acknowledged' $tmp`" \!= "true" ]
    then
      echo "Error while creating alias"
      cat $tmp
      rm $tmp
      exit 3
    fi
}


# Allow sourcing functions
if [ "${BASH_SOURCE[0]}" = "${0}" ]
then

    if [ "$#" \!= "1" ]
    then
      echo "Usage: $0 config_file"
      exit 1
    fi

    source $1
 
    # Main script
    DATE=`date +%Y-%m-%d`
    INDEX=${ELASTIC_INDEX}_$DATE
    ALIAS=$ELASTIC_INDEX

    if [ "$INSECURE" = "1" ]
    then
      INSECURE="--insecure"
      NOCHECK="--no-check-certificate"
    fi

    if [ "$EXCLUDE" \!= "" ]
    then
      X="-x"
    fi
    
    fswalk -p $PATH_TO_SCAN $X "$EXCLUDE" -n $THREADS --elastic-host=$ELASTIC_HOST --elastic-index=$INDEX -g --elastic-bulk-size=$BULK_SIZE --hostname=$HOSTNAME -P $CREDENTIALS_FILE $NOCHECK
    
    OLD_INDEX=`get_old_index $ALIAS`
    re_alias $INDEX $ALIAS
    if [ "$OLD_INDEX" \!= "null" -a "$OLD_INDEX" \!= "$INDEX" ]
    then
      delete $OLD_INDEX
    fi
fi
