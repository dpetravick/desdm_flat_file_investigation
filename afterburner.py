import oracledb
import json 
import os 
import glob


def ora_connect():
    """ 
    Connect to DESSCI or DESOPER oracle databases 

    passwords are assumed to be in the environment as
    OPER_PASS or SCI_PASS.   
    """
    user = "donaldp"
    database = "sci"
    if database == "sci" :
        password = os.environ["SCI_PASS"]
        dsn = "desdb-sci.ncsa.illinois.edu/dessci"
    elif database == "oper":
        password = os.environ["OPER_PASS"]
        dsn = "desdb-oper.ncsa.illinois.edu/desoper"
    else:
        logging.fatal(f"database {database} is not known")
        exit(1)
    conn = oracledb.connect(user=user, password=password,
                               dsn="desdb-sci.ncsa.illinois.edu/dessci",
                               encoding="UTF-8")
    return conn

print("about to conect")
files = ["test.json"]
conn = ora_connect()
cur = conn.cursor()
print ("connected") 
files = glob.glob("d2_parquet/Y6_GOLD_2_2/Y6_GOLD_2_2*.json")

for  f in files:
    print(f)
    j = json.load(open(f,"r"))
    j["meta_info"]["afterburner_ran"] = "True"  #indcate aftereburned rand
    if int(j["file_info"]["number_files"]) == 0 :
        # prune junk
        #print("deleting type info")
        del j["type_info"]
        continue 
    else:
        # give the users some RA, DEC clue
        #pathup counting bug IF number_Files != 0.
        #breakpoint()
        # fix off by one
        fixed_num_files = int(j["file_info"]["number_files"]) -1 
        j["file_info"]["number_files"] = fixed_num_files
        # clarify name 
        j["file_info"]["max_rows_any_file"] = j["file_info"]["max_rows_per_file"]
        del  j["file_info"]["max_rows_per_file"]

        # Claify machine we ran production
        j["meta_info"]["run_machine"] = j["meta_info"]["machine"]
        del j["meta_info"]["machine"]
        j["meta_info"]["HPX_8_index"] = j["meta_info"]["key"]
        del j["meta_info"]["key"]
        j["meta_info"]["run_begin"] = j["meta_info"]["start"]
        del j["meta_info"]["start"]
        j["meta_info"]["run_end"] = j["meta_info"]["end"]
        del j["meta_info"]["end"]

        # "nth is wrong, and not relevant, -- delete ie.
        for pfile in  j["file_info"]["files"]:
            del  j["file_info"]["files"][pfile]["nth"]

        # claritfy meangin of oracle keys, add oracel DB name.
        j["oracle_info"]["rows_this_query"] = j["oracle_info"]["num_rows"]
        del j["oracle_info"]["num_rows"]
        j["oracle_info"]["db_machine"] ="dessci"
        j["oracle_info"]["total_table_stats"] = j["oracle_info"]["table_size"]
        del j["oracle_info"]["table_size"]

        # add helpfil RA, DECs.
        sql = f'SELECT MIN(RA) minra, MAX(RA) maxra, MIN(DEC) mindec, MAX(DEC)  maxdec  from {j["meta_info"]["table"]} {j["meta_info"]["where_clause"]}'
        print (sql)    
        cur.execute(sql)
        (minra, maxra, mindec, maxdec) = cur.fetchone()
        j["file_info"]["min_ra"]  = minra
        j["file_info"]["max_ra"]  = maxra
        j["file_info"]["min_dec"] = mindec
        j["file_info"]["max_dec"] = maxdec
        #import pprint
        #pprint.pprint (j["file_info"])
        #pprint.pprint (j["meta_info"])
        #pprint.pprint ( j["oracle_info"])

        out = f"{f}.after"
        print(out)
        with open(out, 'w') as fp:
            json.dump(j, fp,  indent=4, sort_keys=True)

