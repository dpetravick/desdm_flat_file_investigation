#!/bin/bash


dist_root=~/public_html/parquet/2023-10-04-10:00/

ingest() {
for dir in $dist_root/*  ; 
    do 
    if test -d $dir ; then 
	table=`basename $dir` 
	echo $table
	time ./parquet_export.py  sqlite  -s d2_schemas/$table.schema $dist_root $table 
    fi
done
}

index() {
db=desdm_pruned_indexed_files.db
for dir in $dist_root/*  ; 
    do 
    if test -d $dir ; then 
	table=`basename $dir` 
	index_def=./d6_indicies/$table.def
	echo $table
	time ./ingest.py   -db $db  index  $index_def
    fi
done
}

#ingest
index  

