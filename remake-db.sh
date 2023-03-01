#!/bin/bash
#
#  Remake the sqlilte DB
#
DB=desdm-test.db
if [ -e "$DB" ] ; then 
    echo removing $DD
    rm $DB ;
   fi

for csv in *.csv ; do
    cmd="./ingest.py --db $DB create $csv"
    echo $cmd
    time $cmd
    done

for csv in *.csv ; do
    cmd="./ingest.py --db $DB ingest $csv"
    echo $cmd
    time $cmd
    done

for csv in *.csv ; do
    cmd="./ingest.py --db $DB index  $csv"
    echo $cmd
    time $cmd
    done
