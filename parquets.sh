#!/bin/bash

set -x 
output_root=./new_parquet
mkdir -p $output_root
memory=300_000_000


st -x 
for f in d1_imports/*.import  ; 
    do 
    table=`basename $f` 
    table=`echo  $table | cut -d. -f1`
    echo $table
    time ./parquet_export.py  parquet  -o $output_root -m $memory $table
done
