#!//bin/env python3
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
import time 
import pandas as pd
import tabulate
import configparser
import subprocess

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

def wrap(item):
   "format a prose to fit into a column"
   import textwrap
   w =  textwrap.TextWrapper(subsequent_indent=" ",
                             break_long_words = True,
                             width = 35
   )
   if type(item) == type("") : item = [item]
   stanza = "\n".join( ["\n".join(w.wrap(i)) for i in item])
   return stanza

def format_plan_item(line):
   " crude parser to make plans more readable"
   import shlex
   out = []
   for token in shlex.split(line):
      if token == "TABLE" : token = token + "\n"
      if token == "INDEX" : token = token + "\n"
      if token[0] == "("  : token = "\n " + token
      out.append(token)

   return " ".join(out)

def format_sql(sql):
   import sqlparse
   return sqlparse.format(sql, reindent=True, keyword_case='upper')


def is_analyzed(args):
   """
   Assess if all indicies  have all been Analyzed

   Right now, this is a test for a non empty table
   tha tholds assessments.  
   """
   sql = "select  * from sqlite_stat1;"
   conn = sqlite3.connect(args.db)
   cur = conn.cursor()
   result = cur.execute(sql)
   results = [r for r in result]
   if len(results) : return True
   return False

def make_temp_support_tables(conn, config):
   "temp tables last as long as the connection"
   #import pdb; pdb.set_trace()
   for c in config: 
      if config[c]["type"] != "temp support" : continue
      logging.info(f"about to make temp support table {c} and its index")
      sql = config[c]["query"]
      cur = conn.cursor()
      result = cur.execute(sql)
      sql = config[c]["index_query"]
      result = cur.execute(sql)
      logging.info(f"{c} table and index built")
   return 

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
   logging.info(f"Obtainings schema for {table_name} from ORACLE")
   output_file = os.path.join(args.output_dir, f"{table_name}.schema")
   body_sql = f"SELECT COLUMN_NAME, DATA_TYPE,'t' INCLUDE FROM all_tab_columns WHERE table_name =  '{table_name}' ;"
   sql_script = prefix_template.format(output_file, body_sql)
   print (sql_script)

def export(arg):
   "get a CSV of data from ORACLE"
   table_name = os.path.splitext(os.path.basename(args.schema_file))[0]
   logging.info(f"getting {table_name} as CSV from ORACLE")
   output_file = os.path.join(args.output_dir, f"{table_name}.export")
   conn = sqlite3 .connect(args.db)
   schema = pd.read_csv(args.schema_file)
   columns  = ["TRIM({}) {}".format(row.COLUMN_NAME, row.COLUMN_NAME) for _ , row  in schema.iterrows() if row.INCLUDE == 't']
   items = ",".join(columns)
   hack = ""
   #hack = " WHERE rownum < 10000 "
   body_sql = f"SELECT {items} FROM {table_name} {hack};"
   logging.info(body_sql)
   #make the stuff we need to spool the answer
   sql_script  = prefix_template.format(output_file, body_sql)
   print (sql_script)
   
   
def create(args):
   "make schema for table"
   table_name = os.path.splitext(os.path.basename(args.schema_file))[0]
   logging.info(f"making table {table_name} in SQLITE3")
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
   conn = sqlite3.connect(args.db)
   cur = conn.cursor()
   cur2 = conn.cursor()
   result = cur.execute("SELECT type, name FROM sqlite_master;").fetchall()
   info = []
   for ttype, table in result:
      if ttype == "table":
         # for tables the stat is nrow.
         sql = f"select max(RowId) from {table} ;"
         nrow = cur.execute(sql).fetchall()[0][0]
         stats = nrow
      else:
         # for indexed the stat is the rsoling power of the index.
         # this is available if the corresplding table was analyzed.
         # else print if there is is no analysis 
         # import pdb; pdb.set_trace()
         sql2 = f"SELECT stat from sqlite_stat1 where idx = '{table}';"
         result = cur2.execute(sql2).fetchone()
         if result :
            stats = result[0]
         else:
            stats = "not analyzed"
      info.append((ttype, table, stats))
   report  = tabulate.tabulate(info, 
                               headers=["type", "name", "stats"],
                               tablefmt="simple_grid")
   print (report)

def ingest(args):
   "ingest a csv into sqlite"
   logging.info(f"about to read {args.csv}")
   table = os.path.splitext(os.path.basename(args.csv))[0]
   db_name = args.db
   result = subprocess.run(['sqlite3',
                         str(db_name),
                         '-cmd',
                         '.mode csv',
                         '.import  --skip 2 ' + str(args.csv)
                                 + f' {table}' ,
                            f'ANALYZE {table};'])

def deindex(args):
   "drop all indexes"
   logging.info(f"about to drop all indexes")
   conn = sqlite3 .connect(args.db)
   cur = conn.cursor()
   for type, name in result:
      sql = f"drop index {name};"
      logging.info(sql)
      cur.execute(sql)


def index(args):
   """
   For a table, Drop indexes and then  apply indexes specified in the def file
   """
   table_name = os.path.splitext(os.path.basename(args.def_file))[0]
   conn = sqlite3 .connect(args.db)
   cur=conn.cursor()

   # drop any existing indexes on table
   sql = f"SELECT type, name FROM sqlite_master where type = 'index' and  tbl_name = '{table_name}'"
   result = cur.execute(sql)
   for ttype, name in result:
      sql = f"DROP INDEX {name};"
      logging.info(sql) 
      cur.execute(sql) 

   # apply new indexes
   with open(args.def_file, 'r') as f: lines = f.read()
   for indexed_columns  in lines.split("\n"):

      #discard comments and blank lines 
      indexed_columns = indexed_columns.split("#")[0]
      indexed_columns = indexed_columns.strip()
      if not indexed_columns : continue
      indexname = [col.strip() for col in indexed_columns .split(",")]
      indexname = "_".join(indexname)
      indexname = f"{table_name}__{indexname}_idx"
      logging.info(f"making index {indexname} in sqlite ")
      sql = f"CREATE INDEX IF NOT EXISTS {indexname} ON {table_name} ({indexed_columns}) ;"
      print (sql)
      conn.execute(sql)

   # re-analyse the table and all of its indices.
      logging.info(f"Indicies for {table_name} finished buiding analysis") 
      sql = f"ANALYZE {table_name} ;"
      conn.execute(sql)
      logging.info(f"Analsys for {table_name} finished") 

def query(args):
   "perform an example query specified in the toml file"
   config = get_config(args)
   doc = config[args.query]["doc"]
   query = config[args.query]["query"]
   print (doc)
   print (query)
   conn = sqlite3.connect(args.db)
   cur = conn.cursor()
   make_temp_support_tables(conn, config)
   result = cur.execute(query)
   for r in result: print(r)

def test_db(args):
   "test the db by doing all the test queries"
   import tabulate
   config = get_config(args)
   conn = sqlite3.connect(args.db)
   make_temp_support_tables(conn, config)
   table=[]
   db_analyzed = is_analyzed(args)
   for key in config:
      if config[key]['type'] != 'query' : continue
      print(f'{key}:{config[key]["doc"]}')
      query = config[key]["query"]
      explain_query = "EXPLAIN QUERY PLAN " + query 
      logging.debug(f'{query}')
      cur = conn.cursor()
      plan = cur.execute(explain_query)
      plans = "\n".join([format_plan_item(f"{p[3]}") for p in plan])
      t0 =  time.time()
      result = cur.execute(query)
      n_items  = len([r for r in result])
      duration = time.time()-t0
      table.append( [key, db_analyzed, f"{duration:.2f}", n_items, format_sql(query), plans] ) 
      print (f"{n_items} returned in {time.time()-t0} seconds")

   headers=["name", "azed", "sec","rows","query","plan"]
   print(tabulate.tabulate(table, tablefmt="simple_grid",headers=headers))
      

def plan(args):
   "perform an explain of a query"
   config = get_config(args)
   doc = config[args.query]["doc"]
   query = config[args.query]["query"]
   query = "EXPLAIN QUERY PLAN " + query
   print (query)
   conn = sqlite3.connect(args.db)
   make_temp_support_tables(conn, config)
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
   "start an sqlilte shell against DB"
   cmd = f"sqlite3 {args.db}"
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
   

def sqlplus(args):
   "start an sqlplus session using info from  $HOME/.desservices.ini"

   # get info from desservices.inifile for connection
   home = os.environ["HOME"]
   config = configparser.ConfigParser()
   config.read(os.path.join(home, ".desservices.ini"))
   service=args.service
   user= config[service]["user"]
   passwd = config[service]["passwd"]
   server = config[service]["server"]

   # process any arguments to sqlplus
   cmd_args = ""
   if args.cmd_args : cmd_args = " ".join(args.cmd_args)
   cmd=f'rlwrap sqlplus {user}/{passwd}@{server}/{service} {cmd_args}'
   logging.info(cmd)
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
    main_parser.set_defaults(func=None)
        
    subparsers = main_parser.add_subparsers()   

    # export from Oracle
    parser = subparsers.add_parser('export', help=export.__doc__)
    parser.set_defaults(func=export)
    parser.add_argument("schema_file", help = "schema file")
    parser.add_argument("-o", "--output_dir", help = "def ./exports", default="./d2_exports") 

    # create tables in sqlite2
    parser = subparsers.add_parser('create', help=create.__doc__)
    parser.set_defaults(func=create)
    parser.add_argument("schema_file", help = "schema_file")
    parser.add_argument("-o", "--output_dir", help = "def ./schemas", default="./d3_creates") 

    # ingest csv into  sqlite2
    parser = subparsers.add_parser('ingest', help=ingest.__doc__)
    parser.set_defaults(func=ingest)
    parser.add_argument("csv", help = "CSV to ingest")

    # builld indexes 
    parser = subparsers.add_parser('index', help=index.__doc__)
    parser.set_defaults(func=index)
    parser.add_argument("def_file", help = "file named by table with index defnintions")


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
    parser.add_argument("import_file", help = "table") 
    parser.add_argument("-o", "--output_dir", help = "def ./schemas", default="./d2_schemas") 

    # test the DB
    parser = subparsers.add_parser('test_db', help=test_db.__doc__)
    parser.set_defaults(func=test_db)

    # test the DB
    parser = subparsers.add_parser('deindex', help=deindex.__doc__)
    parser.set_defaults(func=deindex)

    # sqlplus
    parser = subparsers.add_parser('sqlplus', help=sqlplus.__doc__)
    parser.set_defaults(func=sqlplus)
    parser.add_argument("-s", "--service", help = "which service", default="dessci") 
    parser.add_argument("cmd_args", help = "any args to sqlplus", nargs='*')


    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)

    args.func(args)

