#!/usr/bin/env python
"""
for main progams, write the documentation out in manpage format.
It will be part of the help, wiht quic help on command line
argumenta and options absent inpydoc but present in help.

so conclude this docstring with

Optons available via <command> --help
"""
import sqlite3 
import pandas as pd
import argparse
import logging

prefix_template = """
set pagesize 0   
set trimspool on 
set headsep off  
set linesize 300   
set numw  300 
spool {}.csv
"""

sqlinfo = {}
sqlinfo["y6a1_proctag"]={}
sqlinfo["y6a1_proctag"]["select"] = """
   SELECT
      TRIM(tag) || ','  ||  pfw_attempt_id from y6a1_proctag;
   """
sqlinfo["y6a1_proctag"]["create"] = """
     CREATE TABLE IF NOT Exists
         y6a1_proctag
     VALUES
         (
         fw_attempt_id  BIGINT,
          tag             TEXT
         ) ;
     CREATE y6a1_proctag_attempt_indev INDEX on y6a1_proctag (y6a1_proctag);
     commit; 
"""    

sql = """
     CREATE TABLE IF NOT Exists
y6a1_proctag
     Values( 
     fw_attempt_id  BIGINT,
     tag             TEXT
)
"""

sql == """
    CREATE TABLE IF NOT Exists 
    y6a1_image    
   Values (
    pfw_attempt_id TEXT,
    tilename       TEXT,
    coadd_nwgint   TEXT,
    expnum         INT,
    ccdnum 
   ) 
"""

sql = """ 
    CREATE TABLE IF NOT Exists
      pfw_attempt_id
      VALUES (
+       tilename      TEXT,
+       coadd_nwgint  TEXT,
+       expnum        INT,
+       ccdnun        INT
)
"""
sql = """
    CREATE TABLE IF NOT Exists
-    y6a1_file_archive_info 
VALUES (
+    filetype  TEXT,   
+    ccdnum    TEXT,
+    tag       TEXT
      )
"""

sql = """
CREATE TABLE IF NOT Exists
   y6a1_image
VALUES (
   pfw_attempt_id  TEXT,
   tilename        TEXT,
   coadd_nwgint    TEXT,
   expnum          INT,
   ccdnum          INT
   )
"""
def export(arg):
   "get a CSV of data from ORACLE"
   table = "y6a1_proctag"
   prefix = prefix_template.format(table)
   select = sqlinfo[table]["select"]
   sql = prefix + select
   print (sql)
   
   
def create(args):
   "make schema for table" 
   conn = sqlite3 .connect(args.db)
   table = "y6a1_proctag"
   conn.execute(sqlinfo[table]["create"]

def ingest(args):
   df = pd.read_csv('filepaths-for-y6a1_image.csv')
   conn = sqlite3.connect(args.db)

if __name__ == "__main__":

    #main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
     description=__doc__,
     formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument('--loglevel','-l',
                             help='loglevel NONE, "INFO",  DEBUG',
                             default="INFO")
    main_parser.add_argument('--db',  default='desdm-test',
             help='the sqlite DB udder test')
        
    subparsers = main_parser.add_subparsers()   

    #list but not execute. 
    parser = subparsers.add_parser('export', help=export.__doc__)
    parser.set_defaults(func=export)
    parser.add_argument("table", help = "a table of interest")


    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

