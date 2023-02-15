#!/usr/bin/env python3
"""
for main progams, write the documentation out in manpage format.
It will be part of the help, wiht quic help on command line
argumenta and options absent inpydoc but present in help.

so conclude this docstring with

Optons available via <command> --help
"""
import sqlite3 
import argparse
import logging
import toml 
import pprint

prefix_template = """
set pagesize 0   
sef colsep ,
set trimspool on 
set headsep off  
set linesize 120
set numw  120
spool {}.csv
"""

def get_config(args):
   with open("ingest.toml",'r') as f :
      config = toml.load(f)
   return config

def list(args):
   "print list of known tables"
   config = get_config(args)
   for key in config:
      pprint.pprint(key)


def export(arg):
   "get a CSV of data from ORACLE"
   table = args.table
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
   table = args.table
   config = get_config(args)[table]
   columns =  config["columns"]
   values = ["{} {}".format(c[0], c[1]) for c in columns]
   values = ",".join(values)
   values = "({})".format(values)
   sql = config["create"].format(values)
   print (sql)
   conn.execute(sql)
   for column  in config["indexes"]:
      sql = "create index {}__{}_idx on {} ({}) ;".format(
         table, column, table, column)
      print (sql)
      conn.execute(sql)

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

    # export from Oracle
    parser = subparsers.add_parser('export', help=export.__doc__)
    parser.set_defaults(func=export)
    parser.add_argument("table", help = "a table of interest")

    # create tables in sqlite2
    parser = subparsers.add_parser('create', help=create.__doc__)
    parser.set_defaults(func=create)
    parser.add_argument("table", help = "a table of interest")


    # list known tables 
    parser = subparsers.add_parser('list', help=list.__doc__)
    parser.set_defaults(func=list)

    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

