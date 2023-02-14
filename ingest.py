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
 
oracle_selects = {
""  : ""
""  : ""
""  ;  ""
}

def export(arge):
   
def schema(args):
   conn = sqlite3.connect(args.db)
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
    parser.add_argument('--db',  default='desdm-test',
             help='the sqlite DB udder test')
        
    subparsers = parser.add_subparsers()   

    #list but not execute. 
    parset = subparsers.add_parser('export', help=export.__doc__)
    parser.set_defaults(func=export)
    parser.add_argument("table", help = "a table of interest")

 
    loglevel=logging.__dict__[args.loglevel]
    assert type(loglevel) == type(1)
    loggling.basicConfig(level=logging.__dict__[args.loglevel])

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

