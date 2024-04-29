#!/usr//bin/env python3                                                                                                                                                                    
"""
Utility function suporting oracle -> parquet-> sqlite

Dump an Oracle DB table to parquet files.

Files created under the root directory specifed by the command line. 
For large tables. One of more parquet files are created.
Files for a table are created under <root>/<table-name>/
The columns in the parquet file  are in low-to-high cardnality oder.

The -n flag gises estimates, but does not ETL the data. 
"""
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import os
import stat
import argparse
import pandas as pd
import pandas.io.sql as psql
import oracledb
import logging
import time
import hashlib
import math
import datetime
import glob
import sys
import json 

class Bulk:
    def __init__ (self, args, conn):
        self.args = args
        self.conn = conn
        self.table = args.table
        self.key_sequence  = []
    def x(self):
        pass
        #key_col = args.key_col 
        #count_sql = f"select distinct(count{key_col}) from {table}"
        #sequence_sql = f"select distinct {key_col} from  {table} order by {key_col}"
        # load self.sequence

    def next_set(self):
        pass
        """
        for next_key self.key_sequence:
            # make test path for done flag.
            if flgged as done :
                #log stuff
                continue
            sql_for_key = f""
            self.paquet_dir  = "" # where 
            path_for_parquet = ""
            path_for_done_flag = ""
            path_for_parquet_file = "" # Keep unique 
            yield all_this_info

        """

class Monitor:
    """ 
    acquire and hold misc information to guide, report
    and summarize the parquet-ification of a table 
    """

    def __init__(self, args, conn, key="", where=""):
        self.args = args
        self.conn = conn
        self.key = key
        self.where = where
        self.get_column_info()
        self.get_table_info()
        self.files = []
        self.df_schema = pd.DataFrame()
        self.begin = self._now()
        self.file_dict = {}
        self.num_parquet_files = 0
        self.pandas_column_info = {}
        self.parquet_column_info = {}

    def get_table_info(self):
        """"
        compute quantities relevant to size of parquet files, 

        Idea -- while users can grab the columns they need and 
        minimize memory in other ways, We cannot. we need 
        memory to hold the row-ordered fetch and the column-oriented 
        transpose of fetch.

        Choose a number of rows that will fit into memory as 
        given by the memory command line parameter.

    """
        cur = self.conn.cursor()
        sql = f"""
        SELECT  
             avg_row_len, num_rows
        FROM 
            dba_tables 
        WHERE 
              table_name = '{self.args.table}' """
        self.table_info_df = psql.read_sql(sql, con=self.conn)
        logging.debug(f"table info\n{self.table_info_df}")
        if len(self.table_info_df) != 1 : 
            logging.fatal("errror query for {args.table} did not return one row")
            exit(1)
        self.bytes_per_row = self.table_info_df["AVG_ROW_LEN"][0]
        self.mem_max = self.args.mem_max_bytes
        self.fetch_max = int(self.mem_max / self.bytes_per_row)

        if self.where : 
            sql =f"""
            SELECT
               count(*) num_row
            FROM
               {self.args.table}
               {self.where} """
            logging.info(f"{sql}")
            temp_df =  psql.read_sql(sql, con=self.conn)
            self.num_rows = temp_df["NUM_ROW"][0]
        else:
            self.num_rows = self.table_info_df["NUM_ROW"][0]
        self.num_files =   math.ceil(self.num_rows/self.fetch_max)
        logging.info(f"table, total_rows: {self.args.table}, {self.num_rows}")
        logging.info(f"table, bytes/row: {self.args.table}, {self.bytes_per_row}")
        logging.info(f"table, rows fetched/file: {self.args.table}, {self.fetch_max}")
        logging.info(f"table, num_files: {self.args.table}, {self.num_files}")

    def get_column_info(self):
        cur = self.conn.cursor()
        self.oracle_attributes = ["column_name", "num_distinct", "data_type", "data_length", "data_precision"]
        sql = f"""
            SELECT 
                {",".join(self.oracle_attributes)} 
            FROM  
             all_tab_columns
            WHERE 
                table_name  =  '{self.args.table}'
            ORDER BY 
                num_distinct
        """
        self.column_info_df = psql.read_sql(sql, con=self.conn)
        logging.debug(f"column info\n {self.column_info_df}")
        self.column_names = self.column_info_df['COLUMN_NAME'].tolist()      
        self.set_oracle_column_info()
    
    def set_oracle_column_info(self):
        dout = {}
        for d in json.loads(self.column_info_df.to_json(orient="records")):
            dout[d["COLUMN_NAME"]] = d
            del d["COLUMN_NAME"]
        self.oracle_column_info = dout

#
#  Pandas methods
#
    def record_df_info(self, df):
        self.df_schema = df
        d = { l[0] : f"{l[1]}"  for l in zip(self.df_schema, self.df_schema.dtypes)}
        self.pandas_column_info = d

#
# parquet methods
#
    def record_file(self, file_name):
        import hashlib
        self.num_files += 1
        file_size = os.stat(file_name).st_size
        file_base = os.path.basename(file_name)
        with open(file_name, 'rb') as f: md5sum = hashlib.md5(f.read()).hexdigest()
        self.file_dict[file_base] = {
            "file"   : f"{file_name}",
            "md5sum" : md5sum,
            "size"   : file_size,
            "nth"    : self.num_files
        }

    def get_output_dir(self):
        self.output_dir = os.path.join(self.args.output_root, args.table)
        os.system (f"mkdir -p {self.output_dir}")
        return self.output_dir

    def record_parquet_column_info(self, path):
        import pyarrow.parquet as pq
        # record types, column names.
        schema = pq.read_schema(path, memory_map=True)
        names = schema.names
        types = [str(pa_dtype) for pa_dtype in schema.types]
        self.parquet_column_info  = {i[0]:i[1] for i in zip(names, types)}


    def parquet_column_info_as_dict():
        return self.parquet_types 
        
        
#
# Reporting methods
#

    def mk_final_report(self):
        import getpass
        import platform
        import sys
        import json

        key = self.key
        if self.key :key = f"-{self.key}"
        file_name =  os.path.join(self.args.output_root, self.args.table, f"{self.args.table}{key}.json")
        logging.info(f"writing report to {file_name}")
        meta_info = {
            "who"          : f"{getpass.getuser()}",
            "table"        : f"{self.args.table}",
            "key"          : f"{self.key}",
            "where_clause" : f"{self.where}",
            "what"         : f"Parquet file production report for table {self.args.table}",
            "start"        : f"{self.begin}",
            "end"          : f"{self._now()}",
            "machine"      : f"{platform.node()}",
            "how"          : sys.argv
            }
        file_info = {
            "number_files"      : f"{self.num_files}",
            "max_rows_per_file" : f"{self.fetch_max}",
            "files"             : self.file_dict
            }
        oracle_info = {
            "table_size"      : f"{self.table_info_df}",
            "num_rows"        : f"{self.num_rows}",
        }

        all_info = {
            "meta_info"   : meta_info,
            "file_info"   : file_info, 
            "oracle_info" : oracle_info,
            "type_info" : self.merged_type_info()
        }
        
        
        

        text = json.dumps(all_info, sort_keys=True, indent=4)
        with open(file_name, "w") as filew: filew.write(text)

    def merged_type_info(self):
        """ 
        build a dict for each column  with detailed info about types, etc
        for each stage of data handling (oracle, panads, parquet).
        """
        type_info = {}
        for key in self.oracle_column_info.keys():
             type_info[key] = {
                "oracle" : self.oracle_column_info.get(key, "n/a"),
                "pandas" : self.pandas_column_info.get(key, "n/a"),
                "parquet" :self.parquet_column_info.get(key, "n/a")
               }
        return type_info
#
# Utility methods
#
    def _now(self):
        now =datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return now 
    

def ora_connect(args):
    """ 
    Connect to DESSCI or DESOPER oracle databases 

    passwords are assumed to be in the environment as
    OPER_PASS or SCI_PASS.   
    """
    user = "donaldp"
    if args.database == "sci" :
        password = os.environ["SCI_PASS"]
        dsn = "desdb-sci.ncsa.illinois.edu/dessci"
    elif args.database == "oper":
        password = os.environ["OPER_PASS"]
        dsn = "desdb-oper.ncsa.illinois.edu/desoper"
    else:
        logging.fatal(f"database {args.database} is not known")
        exit(1)
    conn = oracledb.connect(user=user, password=password,
                               dsn="desdb-sci.ncsa.illinois.edu/dessci",
                               encoding="UTF-8")
    return conn


def mk_parquet(args):
    """
    make parquet file(s) from a table.

    For large tables make a number of parquet 
    files, for the query data to not overly fill
    memory.
    """
    conn = ora_connect(args)
    cur = conn.cursor()
    monitor = Monitor(args, conn)
    column_names = monitor.column_names
    max_rows  = monitor.fetch_max
    sql = f"select {','.join(column_names)} from {args.table}"
    logging.debug(sql)
    if not args.noop :
        rows = cur.execute(sql)
        for file_number in range(1000):  
            t0 = time.time()
            batch = cur.fetchmany(max_rows)
            df =  pd.DataFrame(batch, columns=column_names)
            if not len(df) : break
            monitor.record_df_info(df)
            output_dir = monitor.get_output_dir()
            file_name = os.path.join(output_dir, f"{args.table}_{file_number:04}.parquet")
            logging.info(f"beginning build of {file_name} ({max_rows} rows)")
            df.to_parquet(file_name, engine='pyarrow',compression='snappy')
            file_size = os.stat(file_name).st_size
            logging.info(f"end of build of {file_name} {file_size:,d} bytes -- {(time.time()-t0):0.2f} seconds ")
            monitor.record_file(file_name)
    logging.info(f"{args.table} processing finished")
    monitor.mk_final_report()

def mk_parquet2(args):
    """
    make parquet file(s) from a table, and special arguments

    For large tables make a number of parquet 
    files, for the query data to not overly fill
    memory.
    """
    conn = ora_connect(args)
    cur = conn.cursor()
    monitor = Monitor(args, conn, key=args.key, where=args.where)
    column_names = monitor.column_names
    max_rows  = monitor.fetch_max
    sql = f"select {','.join(column_names)} from {args.table}  {args.where}"
    logging.debug(sql)
    rows = cur.execute(sql)
    if not args.noop :
        for file_number in range(1000):  
            t0 = time.time()
            batch = cur.fetchmany(max_rows)
            df =  pd.DataFrame(batch, columns=column_names)
            if not len(df) : break
            monitor.record_df_info(df)
            output_dir = monitor.get_output_dir()
            file_name = os.path.join(output_dir, f"{args.table}-{args.key}-{file_number:04}.parquet")
            logging.info(f"beginning build of {file_name} ({max_rows} rows)")
            df.to_parquet(file_name, engine='pyarrow',compression='snappy')
            file_size = os.stat(file_name).st_size
            logging.info(f"end of build of {file_name} {file_size:,d} bytes -- {(time.time()-t0):0.2f} seconds ")
            monitor.record_file(file_name)
            monitor.record_parquet_column_info(file_name) # 
    logging.info(f"{args.table} processing finished")
    monitor.mk_final_report()



def add_and_write (df, data):
    columns = df.columns
    df_tmp = pd.DataFrame([data], columns=columns)
    df = pd.concat([df,df_tmp], ignore_index= True)
    df.to_csv("dog.csv") #overwrite to preserve what can be
    return df



def sqlite(args):
    """ingest parquet files respresenting one table into sqlite"""
    import sqlite3
    
    # locate files, if none complain and exit
    logging.info(f"Path to parquet distribution is {args.parquet_dist}") 
    logging.info(f"Schema file showing needed columns is {args.schema_file}")
    logging.info(f"Database is {args.database}")
    logging.info(f"Table to build is {args.table}")
    parquet_path = os.path.join(args.parquet_dist, args.table, "*.parquet")
    parquet_files  = [f for f in glob.glob(parquet_path)]
    total_files = len(parquet_files)
    logging.info(f"{total_files} Parquet files to ingest for {args.table}")
    if total_files == 0 : 
        logging.error(f"No parquet files files under {parquet_path}")
        exit(1)


    # stufff Need inside the per-parquet-file loop
    #  DB collumns not accires forward.
    #  Db connection.
    #  Data frame to keep books. 
    drop_list = []
    if args.schema_file:
        schema = pd.read_csv(args.schema_file)
        drop_list = [row[1].COLUMN_NAME for row  in schema.iterrows() if row[1].INCLUDE != 't']
        logging.info(f"Omitting columns {','.join(drop_list)}")
    cnx = sqlite3.connect(f'{args.database}')
    
    book_keep_df = pd.DataFrame(columns=["table","duration", "file"])
    if_exists_option = 'fail'
    n_files_seen = 0

    for p_file in parquet_files:
        n_files_seen += 1

        t0 = time.time()
        df = pd.read_parquet(p_file, engine='pyarrow')
        logging.info(f"Read {p_file} in {time.time() - t0}, dropping columns")
        df = df.drop(columns=drop_list)
        t0 = time.time()
        df.to_sql(name=args.table, con=cnx, 
                  index=False,
                  if_exists=if_exists_option)
        cnx.commit()
        logging.info(f"Ingested  {p_file} in {time.time() - t0} seconds")
        if_exists_option = 'append'
        book_keep_df = add_and_write (book_keep_df, [args.table, time.time()-t0, p_file])

    #now record 
    sql = f"SELECT count(*) from {args.table}"
    cur = cnx.cursor()
    ans = cur.execute(sql).fetchone()[0]
    logging.info(f"Total rows in {args.table} is {ans}")

def _directories(args):
    """
    return the list of directories at args.path/*

    based on the assumption that every directory under the 
    pasrtquet file root indicates it is a table to ingest.
    """
    path = os.path.join(args.root,"*")
    files = glob.glob('/path/to/folder/*')
    directories = [f for f in files is os.stat(f).stat.S_ISDIR]
    logging.info (f"Directories: [directories]")
    return directories


def exit_with_help(args):
    args.help(sys.stderr)
    exit(1)


if __name__ == "__main__" :

    main_parser = argparse.ArgumentParser(
     description=__doc__,
     formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument('--loglevel','-l',
                             help='loglevel NONE, "INFO",  DEBUG',
                             default="INFO")
    main_parser.set_defaults(func=exit_with_help)
    main_parser.set_defaults(help=main_parser.print_help)
 
    subparsers = main_parser.add_subparsers()

    #parquet =- make parquet files                                                                                                                                                  
    parser = subparsers.add_parser('parquet', help=mk_parquet.__doc__)
    parser.set_defaults(func=mk_parquet)
    parser.add_argument("table", help = "oracle table")
    parser.add_argument("-o", "--output_root", help = "def ./d1_parquet", default="./d2_parquet")
    parser.add_argument("-m", "--mem_max_bytes", help = "memory for connversion of table", default=50_000_000, type=int)
    parser.add_argument("-db", "--database", choices=["sci", "oper"], 
                        help="sci or oper data bases", default="sci")
    parser.add_argument ("-n", "--noop", help = "tell me about the job w/out doing it",  action="store_true", default=False)


    #parquet2 =- make parquet files                                                                                                                                                
    parser = subparsers.add_parser('parquet2', help=mk_parquet.__doc__)
    parser.set_defaults(func=mk_parquet2)
    parser.add_argument("table", help = "oracle table")
    parser.add_argument("-o", "--output_root", help = "def ./d1_parquet", default="./d2_parquet")
    parser.add_argument("-m", "--mem_max_bytes", help = "memory for connversion of table", default=50_000_000, type=int)
    parser.add_argument("-db", "--database", choices=["sci", "oper"], 
                        help="sci or oper data bases", default="sci")
    parser.add_argument ("-n", "--noop", help = "tell me about the job w/out doing it",  action="store_true", default=False)
    parser.add_argument("key", help = "a stringn naming what's included in the where clause, eg. healpix key")                     
    parser.add_argument("where", help = "where clause, including the listeral WHERE ")


    #sqlite -- build a sqlite table from parquet                                                                                                                                   
    parser = subparsers.add_parser('sqlite', help=sqlite.__doc__)
    parser.set_defaults(func=sqlite)
    parser.add_argument("-db","--database",  help = "sqlite_database", default = "desdm_files.db")
    parser.add_argument("parquet_dist", help = "root of parquet distribution")
    parser.add_argument("table", help = "table to buildn")
    parser.add_argument("-s", "--schema_file",  help = "CSV file indicating which columns to bring forward", default = None)

    args = main_parser.parse_args()

    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])
    args.func(args)


