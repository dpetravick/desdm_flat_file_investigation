#!/usr//bin/env python3                                                                                                                                                                    
"""
Utility function suporting oracle -> parquet-> sqlite

Dump an Oracle DB table to parquet files.

Files created under the root directory specifed by the command line. 
For large tables, one of more parqet files are created.
Files for a tabel are created under <root>/<table-name>/
The columns in the parqet tables are in low-to his cardnality oder. 
"""

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

class Monitor:
    """ 
    acquire and hold misc information to guide, report
    and summarize teh parquet-ification of a table 
    """

    def __init__(self, args, conn):
        self.args = args
        self.conn = conn
        self.get_column_info()
        self.get_table_info()
        self.files = []
        self.begin = self._now()
        self.df_schema = None
    
    def _now(self):
        now =datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return now 
    
    def get_table_info(self):
        """"
        compute quantities relevnt to size of parquet files, 

        Idea -- while users can grab the columns they need and 
        minimize memory in other ways, We cannot. we need 
        memory to hold the fetch and the transpose of fetch.
        Chose a number of rows that will fit into memory 
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
        logging.info(f"table info\n{self.table_info_df}")
        if len(self.table_info_df) != 1 : 
            logging.fatal("errror query for {args.table} did not return one row")
            exit(1)

        self.bytes_per_row = self.table_info_df["AVG_ROW_LEN"][0]
        self.mem_max = self.args.mem_max_bytes
        self.fetch_max = int(self.mem_max / self.bytes_per_row)
        self.num_files =   math.ceil(self.table_info_df["NUM_ROWS"][0]/self.fetch_max)
        logging.info(f"table, bytes/row: {self.args.table}, {self.bytes_per_row}")
        logging.info(f"table, rows fetched/file: {self.args.table}, {self.fetch_max}")
        logging.info(f"table, num_files: {self.args.table}, {self.num_files}")

    def get_column_info(self):
        cur = self.conn.cursor()
        sql = f"""
            SELECT 
                column_name,  num_distinct, data_type, data_length, data_precision
            FROM  
             all_tab_columns
            WHERE 
                table_name  =  '{self.args.table}'
            ORDER BY 
                num_distinct
        """
        self.column_info_df = psql.read_sql(sql, con=self.conn)
        logging.info(f"column info\n {self.column_info_df}")
        self.column_names = self.column_info_df['COLUMN_NAME'].tolist()      
    
    def mk_final_report(self):
        file_name =  os.path.join(self.args.output_root, self.args.table, f"{self.args.table}.report")
        logging.info(f"writing report to {file_name}")
        with open(file_name, "w") as filew:
            text = f"""
Parquet file production report for table {self.args.table}
run start/stop : {self.begin}, {self._now()} 
Expected number of files {self.num_files}
Expected rows per file   {self.fetch_max}
Table info\n{self.table_info_df}
Column info\n{self.column_info_df}

Intermediate Data Frame Types\n{self.df_schema}

Parquet File info\n{pd.DataFrame(self.files, columns=["file name", "file size"])}
            """
            filew.write(text)
    def record_file(self, file_name):
        file_size = os.stat(file_name).st_size
        file_name= os.path.basename(file_name)
        self.files.append([file_name, f"{file_size:,d}"] )

    def record_df_info(self, df):
        "dig into the data frame and record the deep data types." 
        s = ""
        for c in df.columns: s = s + f" {c} {type(df[c][0])} \n"
        self.df_schema = s
    
def mk_md5(filename):
    "make md5 file correspoding to file named by filename"

    with open(filename,"rb") as filer:
        value = hashlib.md5(filer.read())
        hexvalue = value.hexdigest()
    with open(filename + ".md5","w") as filew:
        filew.write(f"{hexvalue}\n")

def ora_connect(args):
    """ 
    Connect to DESSCI or DESOPER oracle databases 

    passwords are assumed to be in the environment as
    OPER_APSS or SCI_PASS.   
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
    files, for the query data to no overly fill
    memory.
    """
    conn = ora_connect(args)
    cur = conn.cursor()
    monitor = Monitor(args, conn)
    column_names = monitor.column_names
    max_rows  = monitor.fetch_max
    sql = f"select {','.join(column_names)} from {args.table}"
    logging.info(sql)
    rows = cur.execute(sql)
    for file_number in range(1000):  
        t0 = time.time()
        batch = cur.fetchmany(max_rows)
        df =  pd.DataFrame(batch, columns=column_names)
        if not len(df) : break
        monitor.record_df_info(df)
        dir_name = os.path.join(args.output_root, args.table)
        os.system (f"mkdir -p {dir_name}")
        file_name = os.path.join(dir_name, f"{args.table}_{file_number:04}.parquet")
        logging.info(f"beginning build of {file_name} ({max_rows} rows)")
        df.to_parquet(file_name, engine='pyarrow',compression='snappy')
        mk_md5(file_name)
        file_size = os.stat(file_name).st_size
        logging.info(f"end of build of {file_name} {file_size:,d} bytes -- {(time.time()-t0):0.2f} seconds ")
        monitor.record_file(file_name)
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

if __name__ == "__main__" :

    main_parser = argparse.ArgumentParser(
     description=__doc__,
     formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument('--loglevel','-l',
                             help='loglevel NONE, "INFO",  DEBUG',
                             default="INFO")
    main_parser.set_defaults(func=main_parser.print_help)

    subparsers = main_parser.add_subparsers()

    #parquet =- make parquet files                                                                                                                                                         
    parser = subparsers.add_parser('parquet', help=mk_parquet.__doc__)
    parser.set_defaults(func=mk_parquet)
    parser.add_argument("table", help = "oracle table")
    parser.add_argument("-o", "--output_root", help = "def ./d1_parquet", default="./d2_parquet")
    parser.add_argument("-m", "--mem_max_bytes", help = "memory for connversion of table", default=50_000_000, type=int)
    parser.add_argument("-db", "--database", choices=["sci", "oper"], 
                        help="sci or oper data bases", default="sci")
    parser.set_defaults(func=main_parser.print_help)

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


