!#/bin/sh
set -x
ROOT=desdm-test
DB=$ROOT.db
test_log=test.log
./ingest.py test_db | tee $test_log
stamp=`date "+%y-%m-%d-%H-%M"`
dist_dir=$HOME/public_html/desdm-db-$stamp
mkdir  $dist_dir
cp $DB $dist_dir
cp $test_log $dist_dir
cp ingest.toml $dist_dir
ls  $dist_dir

