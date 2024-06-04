#!/usr//bin/env python3                                                                                                                                                                    
"""
Utility function suporting oracle -> parquet-> sqlite

Dump an Oracle DB table to parquet files.

Files created under the root directory specifed by the command line. 
For large tables. One of more parquet files are created.
Files for a table are created under <root>/<table-name>/
The columns in the parquet file  are in low-to-high cardnality oder.

The -n flag gives estimates, but does not ETL the data. 
"""
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import os
import stat
import argparse
import oracledb
import logging
import time
import hashlib
import math
import datetime
import glob
import sys
import json 
import psutil
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pandas.io.sql as psql
import math 
import numpy as np

class Process_monitor:
    """
    a class to keep track fo execution times, cpu memory use.
    - log events bit the logger.
    - build and present a summery
    """
    def __init__(self):
        self.t0 = time.time()
        self.state0 =  self._get_state()
    
    def _get_state(self):
        #return psutil.Process().as_dict(attrs=['memory_percent','cpu_times'])
        usec = f"{psutil.Process().as_dict()['cpu_times'].user}"
        mem  =  f"{psutil.Process().as_dict()['memory_percent']:.2f}%"
        return f"mem={mem}, sec(user) = {usec}"

    def _get_elapsed_wall_time(self):
        return time.time() - self.t0

    def log_status(self, where=""):
        state = self._get_state()
        t = self._get_elapsed_wall_time()
        logging.info(f"wall sec:{t:.3f} {where}: {state}")

    def mk_report(self):
        report = {
            'elapsed_sec' : self._get_elapsed_wall_time(),
            'initial_system_state' : {
                'cpu'            :self.state0['cpu_times'],
                'memory_percent' :self.state0['memory_percent']
                },
            'initial_system_state' : {
                'cpu'            :self._get_state()['cpu_times'],
                'memory_percent' :self._get_state()['memory_percent']
                }
        }  


class Monitor:
    """ 
    acquire and hold misc information to guide, report
    and summarize the parquet-ification of a table 
    """

    def __init__(self, args, conn, key="", where=""):
        self.args = args
        self.conn = conn
        self.db = args.database
        self.key = key
        self.where = where
        self.pm = Process_monitor()
        self.get_column_info()
        self.get_table_info()
        self.files = []
        self.df_schema = pd.DataFrame()
        self.begin = self._now()
        self.file_dict = {}
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
        self.pm.log_status(where="begin sizing queries")
        cur = self.conn.cursor()
        sql = f"""
        SELECT  
             avg_row_len, num_rows
        FROM 
            dba_tables 
        WHERE 
              table_name = '{self.args.table}'
         """
        logging.debug(sql)
        self.table_info_df = psql.read_sql(sql, con=self.conn)
        logging.debug(f"table info\n{self.table_info_df}")
        if len(self.table_info_df) != 1 : 
            logging.fatal("errror query for {args.table} did not return one row")
            exit(1)
        self.average_row_bytes = self.table_info_df["AVG_ROW_LEN"][0]
        self.num_rows = self.table_info_df["NUM_ROWS"][0]
        if self.where : 
            sql =f"""
            SELECT
               count(*) num_rows
            FROM
               {self.args.table}
               {self.where} """
            logging.info(f"{sql}")
            temp_df =  psql.read_sql(sql, con=self.conn)
            self.num_rows = temp_df["NUM_ROWS"][0]
        
        #compute and log run parameters.
        self.total_number_of_rows = self.num_rows
        self.average_row_bytes = self.table_info_df["AVG_ROW_LEN"][0]
        self.data_streaming_batch_size = 10_000_000  #bytes for one in memory copy.
        self.rows_per_streaming_batch  = int(self.data_streaming_batch_size / self.average_row_bytes) 
        self.output_file_size          = 20_000_000_000 # appox bytes
        self.max_batches_per_file      = int(self.output_file_size / (self.average_row_bytes *  self.rows_per_streaming_batch))
        self.total_batches_per_query   = math.ceil(self.num_rows / self.rows_per_streaming_batch)
        self.num_files                 = math.ceil(self.total_batches_per_query/self.max_batches_per_file)
        
        logging.info(f"table, nrows : {self.args.table}, {self.total_number_of_rows}")
        logging.info(f"table, bytes/row: {self.args.table}, {self.average_row_bytes}")
        logging.info(f"table, num_files: {self.args.table}, {self.num_files}")
        logging.info(f"table, est max_file_size {self.args.table}, {self.output_file_size}")
        logging.info(f"table, rows_per_batch: {self.args.table}, {self.rows_per_streaming_batch}")
        logging.info(f"table, max_batches_per_File: {self.args.table}, {self.max_batches_per_file}")
        logging.info(f"table, total_batches_per_query {self.args.table}, {self.total_batches_per_query}")
        self.pm.log_status(where="end sizing queries")

    def get_column_info(self):
        """"
        get _once_ information about columns in this table.
            - the list of colums from least cardinality to most that is canonical order fo cols.
            - a dictionay, keyed by column name, of needed attributes.
        """
        cur = self.conn.cursor()
        self.oracle_attributes = ["column_name", "num_distinct", "data_type", "data_length", "data_precision", "data_scale"]
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
        self.column_info_df.set_index('COLUMN_NAME')
        logging.debug(f"column info\n {self.column_info_df}")
        self.column_names = self.column_info_df['COLUMN_NAME'].tolist()
        tmp_dict =  self.column_info_df.to_dict('index')
        self.column_info_dict = {tmp_dict[k]['COLUMN_NAME']:tmp_dict[k] for k in tmp_dict}
        self.set_oracle_column_info()
    
    def set_oracle_column_info(self):
        dout = {}
        for d in json.loads(self.column_info_df.to_json(orient="records")):
            dout[d["COLUMN_NAME"]] = d
            del d["COLUMN_NAME"]
        self.oracle_column_info = dout


#
# parquet methods
#
    def record_parquet_file(self, file_name, file_number):
        import hashlib
        file_size = os.stat(file_name).st_size
        file_base = os.path.basename(file_name)
        md5sum = self.md5hash(file_name)
        self.file_dict[file_base] = {
            "file"   : f"{file_name}",
            "md5sum" : md5sum,
            "size"   : file_size,
            "nth"    : file_number
        }
        self.record_parquet_column_info(file_name)
        what = f"file_name, size: {file_name, file_size}"
        self.pm.log_status(where=what)

    def get_output_dir(self):
        self.output_dir = os.path.join(self.args.output_root, args.table)
        os.system (f"mkdir -p {self.output_dir}")
        return self.output_dir

    def arrow_schema_from_oracle(self, sql):
        "get arrow schema by analyzing oracle query result"
        schema_info = []
        for c in self.column_names:
            field = c
            arrow_type = self._type_map(self.column_info_dict[c])
            schema_info.append((field, arrow_type))
        arrow_schema = pa.schema(schema_info)
        return arrow_schema

    def _type_map(self, item_info):
        type_dict = {'DB_TYPE_BINARY_FLOAT':  pa.float32,
         'VARCHAR2': pa.string,
         'BINARY_DOUBLE' : pa.float64,
         'BINARY_FLOAT'  : pa.float32
        }
        oracle_type = item_info['DATA_TYPE']
        if oracle_type in type_dict :
            result = type_dict[oracle_type]()
        elif oracle_type == 'NUMBER' :
            if item_info['DATA_SCALE'] == 0 :
                result = pa.int64() 
            else: 
                result = pa.float64() 
        else:
            logging.error(f"{oracle_type} is not in the type dict, please add it")
            exit (10)
        return result 

        
    def record_parquet_column_info(self, path):
        import pyarrow.parquet as pq
        # record types, column names.
        schema = pq.read_schema(path, memory_map=True)
        names = schema.names
        types = [str(pa_dtype) for pa_dtype in schema.types]
        self.parquet_column_info  = {i[0]:i[1] for i in zip(names, types)}
        del schema 

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
            "run_begin"        : f"{self.begin}",
            "run_end"          : f"{self._now()}",
            "run_machine"      : f"{platform.node()}",
            "how"          : sys.argv
            }
        file_info = {
            "number_files"      : self.num_files,
            "max_rows_batches_any_file" : self.max_batches_per_file,
            "max_rows_any_file" : self.rows_per_streaming_batch * self.max_batches_per_file ,
            "files"             : self.file_dict
            }
        oracle_info = {
            "total_table_stats"  : f"{self.table_info_df}",
            "rows_this_query"    : f"{self.num_rows}",
            "oracle_database"    : f"{self.db}",
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
                "parquet" :self.parquet_column_info.get(key, "n/a")
               }
        return type_info
#
# Utility methods
#
    def _now(self):
        now =datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return now 
    
    def md5hash(self, file_name):
        h = hashlib.md5()
        with open(file_name, 'rb') as f :
            while 1:
                buffer = f.read(1000)
                if not buffer :  break
                hash = h.update(buffer)
        return h.hexdigest()


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
    max_rows  = monitor.max_rows_per_parquet
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
            monitor.record_file(file_name, file_number)
    logging.info(f"{args.table} processing finished")
    monitor.mk_final_report()

def mk_parquet3(args):
    """
    make parquet file(s) from a table, and special arguments

    For large tables make a number of parquet 
    files,     """
    conn = ora_connect(args)
    cur = conn.cursor()
    monitor = Monitor(args, conn, key=args.key, where=args.where)
    column_names = monitor.column_names
    output_dir = monitor.get_output_dir()

    # now the work, query  all the data consistent w/ where clause  
    if args.noop : return  
    sql = f"select {','.join(column_names)} from {args.table}  {args.where}"
    schema = monitor.arrow_schema_from_oracle(sql)
    cur.prefetchrows = 8000
    cur.execute(sql)  
    # place the data into files of maximum size
    for file_number in range(monitor.num_files):
        parquet_file_name = os.path.join(output_dir, f"{args.table}-{args.key}-{file_number:04}.parquet")
        logging.info(f"beginning build of {parquet_file_name} max Batches:(monitor.max_batches_per_file)")
        
        # write any one file in smaller batches
        batch_number = 0
        with pq.ParquetWriter(parquet_file_name, schema) as writer:
            for batch_number  in range(monitor.max_batches_per_file):
                batch_number += 1 
                batch_str = f"file {file_number}:{batch_number}/{monitor.max_batches_per_file}"
                monitor.pm.log_status(where=f"about to fetch")                
                rows = cur.fetchmany(monitor.rows_per_streaming_batch)
                batch_size = len(rows)
                if not rows : break
                monitor.pm.log_status(where=f"about to write {batch_size} rows")
                npcols = np.array(rows).transpose() 
                arrays = [pa.array(col) for col in npcols]
                batch = pa.record_batch(arrays, schema)
                table = pa.Table.from_batches([batch])
                writer.write_table(table)
            writer.close()
            monitor.record_parquet_file(parquet_file_name, file_number)
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


    # stuffed Need inside the per-parquet-file loop
    #  DB columns not accires forward.
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


    #parquet3 =- make parquet files                                                                                                                                                
    parser = subparsers.add_parser('parquet3', help=mk_parquet.__doc__)
    parser.set_defaults(func=mk_parquet3)
    parser.add_argument("table", help = "oracle table")
    parser.add_argument("-o", "--output_root", help = "def ./tmp", default="./tmp")
    parser.add_argument("-m", "--mem_max_bytes", help = "memory for connversion of table", default=50_000_000, type=int)
    parser.add_argument("-db", "--database", choices=["sci", "oper"], 
                        help="sci or oper data bases", default="sci")
    parser.add_argument ("-n", "--noop", help = "tell me about the job w/out doing it",  action="store_true", default=False)
    parser.add_argument("key", help = "a stringn naming what's included in the where clause, eg. healpix key")                     
    parser.add_argument("where", help = "where clause, including the listeral WHERE ")


    #sqlite -- build a sqlite table from parquet                                                                                                                                   
    parser = subparsers.add_parser('sqlite', help=sqlite.__doc__)
    parser.set_defaults(func=sqlite)
    parser.add_argument("-df","--database_file",  help = "sqlite_database", default = "desdm_files.db")
    parser.add_argument("parquet_dist", help = "root of parquet distribution")
    parser.add_argument("table", help = "table to buildn")
    parser.add_argument("-s", "--schema_file",  help = "CSV file indicating which columns to bring forward", default = None)

    args = main_parser.parse_args()

    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])
    args.func(args)

