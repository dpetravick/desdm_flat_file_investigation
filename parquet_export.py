#!/usr//bin/env python3                                                                                                                                                                    
"""
Dump an Oracle DB table to parquet files.

Files created under the root directory specifed by the command line. 
For large tables, one of more parqet files are created.
Files for a tabel are created under <root>/<table-name>/
The columns in the parqet tables are in low-to his cardnality oder. 
"""

import os
import argparse
import pandas as pd
import pandas.io.sql as psql
import oracledb
import logging
import time
import hashlib
import math

    
def plan_fetch_max(args, conn):
    """"
    return the maximun number of rows for each parquet file.

    Idea -- while users can grab the columns they need and 
    minimize memory in other ways, We cannot. we need 
    memory to hold the fetch and the transpose of fetch.
    Chhose a number of rows that will fit into memory 
    """
    cur = conn.cursor()
    breakpoint()
    sql = f"""
    SELECT  
         avg_row_len, num_rows
    FROM 
         dba_tables 
    WHERE 
          table_name = '{args.table}' """
    df = psql.read_sql(sql, con=conn)
    logging.info(f"{df}")
    if len(df) != 1 : 
        logging.fatal("errror query for {args.table} did not return one row")
        exit(1)

    bytes_per_row = df["AVG_ROW_LEN"][0]
    mem_max = args.mem_max_bytes
    fetch_max = int(mem_max / bytes_per_row)
    num_files =   math.ceil(df["NUM_ROWS"][0]/fetch_max)
    logging.info(f"table, bytes/row: {args.table}, {bytes_per_row}")
    logging.info(f"table, rows fetched/file: {args.table}, {fetch_max}")
    logging.info(f"table, num_files: {args.table}, {num_files}")
       
    return fetch_max 

def mk_md5(filename):
    "make md5 file correspoding to file named by filename"

    with open(filename,"rb") as filer:
        value = hashlib.md5(filer.read())
        hexvalue = value.hexdigest()
    with open(filename + ".md5") as filew:
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
    sql = f"""
         SELECT 
           column_name,  num_distinct, data_type, data_length, data_precision
         FROM  
           all_tab_columns
         WHERE 
           table_name  =  '{args.table}'
         ORDER BY 
           num_distinct
     """
    df = psql.read_sql(sql, con=conn)
    logging.info(f"{df}")
    column_names = df['COLUMN_NAME'].tolist()
    max_rows  = plan_fetch_max(args, conn)
    sql = f"select {','.join(column_names)} from {args.table}"
    logging.info(sql)
    rows = cur.execute(sql)
    for file_number in range(1000):  
        t0 = time.time()
        batch = cur.fetchmany(max_rows)
        df =  pd.DataFrame(batch, columns=column_names)
        if not len(df) : exit()
        dir_name = os.path.join(args.output_root, args.table)
        os.system (f"mkdir -p {dir_name}")
        file_name = os.path.join(dir_name, f"{args.table}_{file_number:04}.parquet")
        logging.info(f"beginning build of {file_name} ({max_rows} rows)")
        df.to_parquet(file_name, engine='pyarrow',compression='snappy')
        logging.info(f"end of build of {file_name} -- {(time.time()-t0):0.2f} seconds")

if __name__ == "__main__" :

    main_parser = argparse.ArgumentParser(
     description=__doc__,
     formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument('--loglevel','-l',
                             help='loglevel NONE, "INFO",  DEBUG',
                             default="INFO")
    main_parser.set_defaults(func=None)

    subparsers = main_parser.add_subparsers()

    #parquet =- make parquet files                                                                                                                                                         
    parser = subparsers.add_parser('parquet', help=mk_parquet.__doc__)
    parser.set_defaults(func=mk_parquet)
    parser.add_argument("table", help = "oracle table")
    parser.add_argument("-o", "--output_root", help = "def ./d1_parquet", default="./d2_parquet")
    parser.add_argument("-m", "--mem_max_bytes", help = "memory for connversion of table", default=50_000_000, type=int)
    parser.add_argument("-d", "--database", choices=["sci", "oper"], 
                        help="sci or oper data bases", default="sci")

    args = main_parser.parse_args()
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    logging.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions                                                                                                                                         
        main_parser.print_help()
        exit(1)

    args.func(args)



