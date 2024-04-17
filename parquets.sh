#!/bin/bash

set -x 
output_root=./new_parquet
mkdir -p $output_root
memory=30_000_000_000


set -x  
for f in Y6_GOLD_2_2 ;
#Y6A1_FINALCUT_OBJECT Y6A2_COADD_OBJECT_SUMMARY Y6A2_COADD_OBJECT_BAND_G Y6A2_COADD_OBJECT_BAND_R Y6A2_COADD_OBJECT_BAND_I Y6A2_COADD_OBJECT_BAND_Z Y6A2_COADD_OBJECT_BAND_Y; 
    do 
    table=`basename $f` 
    table=`echo  $table | cut -d. -f1`
    echo $table
    time ./parquet_export.py  parquet  -o $output_root -m $memory $table
done
