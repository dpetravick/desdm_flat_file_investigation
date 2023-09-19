#!/usr/bin/env python3 
""" 
Knowledge of all tables is in the root of
the parquet distribution. Naively it is the
directory name. A more evolved distribution would
have a machine readable data structure.  

There is a directory root for the SQLITE database
and related data are delivered to.  Its name is 
computed from a small number  to support multiple runs.
this support re-running in the case of failure. 

We use the SQLITE database for bookkeeping we make
all_tables, table begun, procesing attempt records
and table_finished records.

Cleanup phase: 
we first look for table_beguns w/out table_finshed.
For each begun record we delete any artifacts from a 
previous run, then delete the table_begun record.

Creation phase
for each table lacking a begun record..
we ingest the table.
"""
import os
import glob
import sqlite3
import pandas as pd 
import argparse
import logging

class Manager:
    def __init__(self, args):
        self.parquet_root  = args.parquet_root
        self.run_number    = 0
        self.delivery_root = os.path.join(args.delivery_root,f"_{self.run_number}")
        self.database_file = os.path.join(self.delivery_root,"files.db")

    def initialize(self):
        self.conn = sqlite3.connect(self.database_file)
        self.cur = self.conn.cursor()
    
    def initiate(self):
        "make the one-time content"
        os.system(f"mkdir -p {self.delivery_root}")
        self.initialize()
        self.q("CREATE TABLE tables (tab TEXT, begun INTEGER, done INTEGER)")
        self.q("CREATE TABLE attempts (tab TEXT)")
        for table in glob.glob(self.parquet_root):
            self.q(f"INSERT INTO tables VALUES ('{table}', 0, 0)")

   

#   common functions
#
    def q(self, sql):
        "log and execute queries"
        logging.debug(sql)
        return self.cur.execute(sql)

    def parquet_table_root(seld,table):
        return os.path.join( self.parquet_root, table)

    def shell(self, cmd):
        logging.debug(cmd)
        os.system(cmd)

#
#   Track and inform about state of table ingest
#
    def state_get_crashed_tables(self):
        sql = "SELECT tab FROM tables WHERE begun = 1 AND done = 0"
        return self.q(sql)
    def state_reset_crashed_table(self, table):
        sql = f"UPDATE tables SET begun = 0 WHERE tab = '{table}'"
        return self.q(sql)
    def state_begin_table(self, table):
        sql = f"UPDATE tables SET begun = 1 WHERE tab = '{table}'"
        return self.q(sql)
    def state_get_table_to_ingest(self):
        sql = f"SELECT  tab FROM tables WHERE begun = 0"
        return self.q(sql)
    def state_mark_table_ingested(self, table):
        sql = f"UPDATE tables SET done = 1 where tab = '{table}'"
        return self.q(sql)

#
#  clean actions
#
    def act_clean_crashed_runs(self):
        "undo any actions for tables partially processed."
        for table in self.state_get_crashed_tables():
            act_clean_crashed_run(table)
            self.state_reset_crashed_table(table)

    def act_clean_crashed_run(self, table):
        "remove any half-built state"
        sql = "DROP TABLE {table} IF EXISTS"
        self.q(sql)

    #
    # ingest actons
    #
    def act_ingest_tables(self):
        "ingest all  tables"
        for table in self.state_get_table_to_ingest():
            self.state_begin_table(table)            
            self.act_ingest_table(table)
            # Assume any error is a fatal exception
            self.state_mark_table_ingested(table)

    def act_ingest_table(self, table):
        "ingest one set o fparquet files into table"
        for p_file in os.path.join(self.parquet_table_root(),"*.parquet"):
            logging.info(f"{p_file} begin")
            df = pd.read_parquet(p_file, engine='pyarrow')
            df.to_sql(name=table, con=self.conn, if_exists='append')
            logging.info(f"{p_file} done")

def ingest(args):
    m = Manager(args)
    m.initialize()
    m.act_clean_crashed_runs()
    m.act_ingest_tables()

def init(args):
    m = Manager(args)
    m.initiate()


if __name__ == "__main__" :
    main_parser = argparse.ArgumentParser(
     description=__doc__,
     formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument('--loglevel','-l',
                             help='loglevel NONE, "INFO",  DEBUG',
                             default="INFO")
    main_parser.set_defaults(func=None)

    subparsers = main_parser.add_subparsers()

    parser = subparsers.add_parser('ingest', help=ingest.__doc__)
    parser.set_defaults(func=ingest)
    parser.add_argument("-p", "--parquet_root", help = "root of parquet files")   
    parser.add_argument("-d", "--delivery_root", help = "root of the sqlpuls delivery ", default="./")

    parser = subparsers.add_parser('init', help=init.__doc__)
    parser.set_defaults(func=init)
    parser.add_argument("-p", "--parquet_root", help = "root of parquet files")   
    parser.add_argument("-d", "--delivery_root", help = "root of the sqlpuls delivery ", default="./")
    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions                                                                                                                                         
        main_parser.print_help()
        exit(1)

    args.func(args)



