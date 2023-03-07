#!/usr/bin/env python3
"""
 small framework to assess the feasiblity of post-desdm databases.
and if DBs are feasible to inform a selection.

the companion ".toml" file holf
-  from one db to another.
- queries used in the evalution.

Optons available via <command> --help
"""
import sqlite3 
import argparse
import logging
import toml 
import pprint
import os
import sys 
import pandas as pd

prefix_template = """
set pagesize 0   
set colsep ,
set trimspool on 
set headsep off  
set linesize 120
set numw  120
set echo off
set term off
set feed off
set markup csv on
spool {}
{}
exit
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

def define(args):
   """
   Emit a CSV listing columns in an ORACLE table as a .schema file.
   
   
   The CSV can be used to designate columns to be 
   carried forward to SQLite.

   """
   table_name = os.path.splitext(os.path.basename(args.import_file))[0]
   output_file = os.path.join(args.output_dir, f"{table_name}.schema")
   body_sql = f"SELECT COLUMN_NAME, DATA_TYPE,'t' INCLUDE FROM all_tab_columns WHERE table_name =  '{table_name}' ;"
   sql_script = prefix_template.format(output_file, body_sql)
   print (sql_script)

def export(arg):
   "get a CSV of data from ORACLE"
   table_name = os.path.splitext(os.path.basename(args.schema_file))[0]
   output_file = os.path.join(args.output_dir, f"{table_name}.export")
   conn = sqlite3 .connect(args.db)
   schema = pd.read_csv(args.schema_file)
   columns  = ["TRIM({}) {}".format(row.COLUMN_NAME, row.COLUMN_NAME) for _ , row  in schema.iterrows() if row.INCLUDE == 't']
   items = ",".join(columns)
   body_sql = f"SELECT {items} FROM {table_name} WHERE ROWNUM < 20;"
   logging.info(body_sql)
   #make the stuff we need to spool the answer
   sql_script  = prefix_template.format(output_file, body_sql)
   print (sql_script)
   
   
def create(args):
   "make schema for table"
   table_name = os.path.splitext(os.path.basename(args.schema_file))[0]
   output_file = os.path.join(args.output_dir, f"{table_name}.create")
   conn = sqlite3 .connect(args.db)
   schema = pd.read_csv(args.schema_file)
   values = ["{} {}".format(row.COLUMN_NAME, row.DATA_TYPE) for _ , row  in schema.iterrows() if row.INCLUDE == 't']
   values = ",".join(values)
   values = f"({values})"
   drop_sql = f"DROP TABLE IF EXISTS {table_name}"
   create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} {values}"
   conn.execute(drop_sql)
   conn.execute(create_sql)
   with open(output_file,"w") as f:
      f.write(drop_sql + "\n")
      f.write(create_sql + "\n")


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
   table = clean_tablename(args.csv)
   logging.info(f"about to ingest into table {table}")
   conn = sqlite3.connect(args.db)
   df.to_sql(table, conn, if_exists='append', index = False)

def index(args):
   "indexe the DB"
   table = clean_tablename(args.table)
   config = get_config(args)[table]
   conn = sqlite3.connect(args.db)
   for indexed_columns  in config["indexes"]:
      indexname = [col.strip() for col in indexed_columns .split(",")]
      indexname = "_".join(indexname)
      indexname = f"{table}__{indexname}_idx"
      sql = f"CREATE INDEX IF NOT EXISTS {indexname} ON {table} ({indexed_columns}) ;"
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

def plan(args):
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

def progress(args):
   "printn a dot every 1000 lines"
   count = 0
   while sys.stdin.readline():
      if count % 1000 == 0 : 
         sys.stdout.write('.')
         sys.stdout.flush()
      count += 1
   sys.stdout.write('\n')

def shell(args):
   "start  an sqlilte shell against DB"
   cmd = f"rlwrap sqlite3 {args.db}"
   import sys
   import subprocess
   help = """                                                                      
.excel                   Display the output of next command in spreadsheet      
.headers on|off          Turn display of headers on or off                      
.indexes ?TABLE?         Show names of indexes                                  
.log FILE|off            Turn logging on or off.  FILE can be stderr/stdout     
.output ?FILE?           Send output to FILE or stdout if FILE is omitted       
.progress N              Invoke progress handler after every N opcodes          
.schema ?PATTERN?        Show the CREATE statements matching PATTERN            
.shell CMD ARGS...       Run CMD ARGS... in a system shell                      
.tables ?TABLE?          List names of tables matching LIKE pattern TABLE       
** Do not terminate any with  a ;                                                    
"""
   print(help)
   subprocess.run(cmd, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, shell=True)
   

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
    parser.add_argument("schema_file", help = "schema file")
    parser.add_argument("-o", "--output_dir", help = "def ./exports", default="./exports") 

    # create tables in sqlite2
    parser = subparsers.add_parser('create', help=create.__doc__)
    parser.set_defaults(func=create)
    parser.add_argument("schema_file", help = "schema_file")
    parser.add_argument("-o", "--output_dir", help = "def ./schemas", default="./creates") 

    # ingest csv into  sqlite2
    parser = subparsers.add_parser('ingest', help=ingest.__doc__)
    parser.set_defaults(func=ingest)
    parser.add_argument("csv", help = "CSV to ingest")

    # builld indexes 
    parser = subparsers.add_parser('index', help=index.__doc__)
    parser.set_defaults(func=index)
    parser.add_argument("table", help = "table to index")
    parser.add_argument("-o", "--output_dir", help = "def ./schemas", default="./schemas") 

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

    #  show plan for   named  canned demo query from toml file
    parser = subparsers.add_parser('plan', help=plan.__doc__)
    parser.set_defaults(func=plan)
    parser.add_argument("query", help = "query to explain") 

    #  show plan for   named  canned demo query from toml file
    parser = subparsers.add_parser('shell', help=shell.__doc__)
    parser.set_defaults(func=shell)

    #  show plan for   named  canned demo query from toml file
    parser = subparsers.add_parser('progress', help=progress.__doc__)
    parser.set_defaults(func=progress)

    # make a CSV defining which columsn shod be carried forward to SQLITE
    parser = subparsers.add_parser('define', help=define.__doc__)
    parser.set_defaults(func=define)
    parser.add_argument("import_file", help = "table ") 
    parser.add_argument("-o", "--output_dir", help = "def ./schemas", default="./schemas") 


    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

