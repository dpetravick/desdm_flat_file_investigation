#!/usr/bin/env python3
"""
A small framework to assess the feasiblity of post-desdm databases.
and if DBs are feasible to inform a selection.

the companion ".toml" file holf
- Mapping from one db to another.
- queries used in the evalution.

Optons available via <command> --help
"""
import sqlite3 
import argparse
import logging
import toml 
import pprint
import os

prefix_template = """
set pagesize 0   
sef colsep ,
set trimspool on 
set headsep off  
set linesize 120
set numw  120
spool {}.csv
"""

def clean_tablename(table):
   "for shell progaming  get table name from tablenmme.case, etc"
   table = os.path.splitext(table)[0]
   logging.info(f"using table {table}")
   return table

def get_config(args):
   "return toml configuration as a dict"
   with open("ingest.toml",'r') as f :
      config = toml.load(f)
   return config

##############################
#
# sub command implementatiom
#
#############################

def list(args):
   "print list of known tables"
   config = get_config(args)
   for key in config:
      pprint.pprint(key)


def export(arg):
   "get a CSV of data from ORACLE"
   table = clean_tablename(args.table)
   config = get_config(args)[table]
   columns  = [c[0] for c in config["columns"]]
   columns = " || ',' || ".join(columns)
   prefix = prefix_template.format(table)
   select = config["select"].format(columns)
   sql = prefix + select
   print (sql)
   
   
def create(args):
   "make schema for table" 
   config = get_config(args)
   conn = sqlite3 .connect(args.db)
   table = clean_tablename(args.table)
   config = get_config(args)[table]
   columns =  config["columns"]
   values = ["{} {}".format(c[0], c[1]) for c in columns]
   values = ",".join(values)
   values = "({})".format(values)
   sql = config["create"].format(values)
   print (sql)
   conn.execute(sql)


def show(args):
   "print high level information about the db"
   config = get_config(args)
   conn = sqlite3 .connect(args.db)
   cur = conn.cursor()
   result = cur.execute("SELECT name FROM sqlite_master;")
   for r in result: print(r)
   
def ingest(args):
   "ingest a csv into  sqlite"
   import pandas as pd
   logging.info(f"about to read {args.csv}")
   df = pd.read_csv(args.csv)
   print (df)
   import pdb; pdb.set_trace()
   table = clean_tablename(args.csv)
   logging.info(f"about to ingest into table {table}")
   conn = sqlite3.connect(args.db)
   df.to_sql(table, conn, if_exists='append', index = False)

def index(args):
   "indexe the DB"
   table = clean_tablename(args.table)
   config = get_config(args)[table]
   columns =  config["columns"]
   conn = sqlite3.connect(args.db)
   for column  in config["indexes"]:
      sql = "create index {}__{}_idx on {} ({}) ;".format(
         table, column, table, column)
      print (sql)
      conn.execute(sql)


def query(args):
   "perform an example query specified in the toml file"
   config = get_config(args)[args.query]
   doc = config["doc"]
   query = config["query"]
   print (doc)
   print (query)
   conn = sqlite3.connect(args.db)
   cur = conn.cursor()
   result = cur.execute(query)
   for r in result: print(r)

def explain(args):
   "perform an explain of a query"
   config = get_config(args)[args.query]
   doc = config["doc"]
   query = config["query"]
   query = "EXPLAIN QUERY PLAN " + query
   print (query)
   conn = sqlite3.connect(args.db)
   cur = conn.cursor()
   result = cur.execute(query)
   for r in result: print(r)

if __name__ == "__main__":

    #main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
     description=__doc__,
     formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument('--loglevel','-l',
                             help='loglevel NONE, "INFO",  DEBUG',
                             default="INFO")
    main_parser.add_argument('--db',  default='desdm-test.db',
             help='the sqlite DB udner test')
        
    subparsers = main_parser.add_subparsers()   

    # export from Oracle
    parser = subparsers.add_parser('export', help=export.__doc__)
    parser.set_defaults(func=export)
    parser.add_argument("table", help = "a table of interest")

    # create tables in sqlite2
    parser = subparsers.add_parser('create', help=create.__doc__)
    parser.set_defaults(func=create)
    parser.add_argument("table", help = "a table of interest")

    # ingest csv into  sqlite2
    parser = subparsers.add_parser('ingest', help=ingest.__doc__)
    parser.set_defaults(func=ingest)
    parser.add_argument("csv", help = "CSV to ingest")

    # builld indexes 
    parser = subparsers.add_parser('index', help=index.__doc__)
    parser.set_defaults(func=index)
    parser.add_argument("table", help = "table to index")

    # list known tables in toml file 
    parser = subparsers.add_parser('list', help=list.__doc__)
    parser.set_defaults(func=list)

    # descrbe the DB 
    parser = subparsers.add_parser('show', help=show.__doc__)
    parser.set_defaults(func=show)

    # execute named  canned demo query from toml file
    parser = subparsers.add_parser('query', help=query.__doc__)
    parser.set_defaults(func=query)
    parser.add_argument("query", help = "query to try") 

    # explaing  named  canned demo query from toml file
    parser = subparsers.add_parser('explain', help=explain.__doc__)
    parser.set_defaults(func=explain)
    parser.add_argument("query", help = "query to explain") 

    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

