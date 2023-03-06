#!/bin/bash
#
#  Remake the sqlilte DB
#

ROOT=desdm-test
DB=$ROOT.db
LOG=$ROOT.log

rm -f "$DB"
rm -f "$LOG"
#logtime() { echo `date` >> $LOG  }
#logsize() { echo ls -lh $DB >> $LOG }
#logtext() { echo $* >> $LOG }

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

cat $LOG
