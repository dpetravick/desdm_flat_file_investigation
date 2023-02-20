# Esperiment one
``` 
(base) [donaldp@deslogin desdm_flat_file_investigation]$ ls -lh *.csv
-rw-rw-r-- 1 donaldp desdm 1.5G Feb 15 10:42 y6a1_file_archive_info.csv
-rw-rw-r-- 1 donaldp desdm 1.7G Feb 15 08:30 y6a1_image.csv
-rw-rw-r-- 1 donaldp desdm 5.1M Feb 15 09:43 y6a1_proctag.csv
```

## Ingest one
 did my ingest after creating tables and indextees. Very sloe-- hours.


## Ingest two -- redo  , making indices lates.
 - Thsi is order(s?) of magnitude faster than creating indices after inget. about 20 minutes.
 - encoded a sample query in  ingest.toml. (to keep tings consistent). uc-se-into-coadd
 - verifies thet all terms has indices
 - verified (and fixed DB so that all terms have indidces 
-- SLow (an hour?)  -- I cut it off.


## started a VACUUUM to see if that woud help.

   - The VACUUM command rebuilds the database file, repacking it into a minimal amount of disk space
   - not quick -- running for leke an hour.
   - did not record db  size ahaed of time.
   - after: -rw-r--r-- 1 donaldp desdm 6.9G Feb 20 09:16 desdm-test.db
   - reddi samople query  @ 9:17
   - seems faster, not fast enougn 6 retured rows in 4 minutes. 
   - likked after 20 minutes -- 37 line of output in 20 minutes
  

## read manual on SQL querying

  - here [https://www.sqlite.org/queryplanner.html]
  - and here [https://www.sqlite.org/queryplanner-ng.html]
  - PRAGMA analysis_limit=400 PRAGMA optimize;
  - Avoid creating low-quality indexes.
  - If you must use a low-quality index, be sure to run ANALYZE. the way the query planner knows that an indes is loe  is by the content of the SQLITE_STAT1 table,
  - Use unlikely() and likelihood() SQL functions to encode prior about truefalse.
  - Use the CROSS JOIN syntax to enforce a particular loop nesting order on queries that might use low-quality indexes in an unanalyzed database. 
  - Use unary “+” operators to disqualify WHERE clause terms.
  - Use the INDEXED BY syntax to enforce the selection of particular indexes on problem queries. A

## Look at uc-se-into-coadd given this advice.

```
select fai.path,fai.filename
from y6a1_proctag t, y6a1_image i, y6a1_image j, y6a1_file_archive_info fai
where t.tag='Y6A1_COADD'
    and t.pfw_attempt_id=i.pfw_attempt_id
    and i.tilename='DES0000+0209'    
    and i.filetype='coadd_nwgint'       #low quality index
    and i.expnum=j.expnum and i.ccdnum=j.ccdnum
    and j.filetype='red_immask'        # low quality index
    and j.filename=fai.filename;
```

investigate
Better qulityu indies woudl be to make index joinlty 
over (filename, filetypee)

-- low qualityy indices are 
-- i,.filetype, j.filetype
-- i. ccdnum, j.ccdum


Change TOML file to  mitigate isues
```
[y6a1_image]

columns = [
           ["pfw_attempt_id", "INTEGER"],
           ["filetype",       "TEXT"],
           ["filename",       "TEXT"],
           ["expnum",         "INTEGER"],
           ["ccdnum",         "INTEGER"],
           ["tilename",       "TEXT"]
          ]
#indexes = ["pfw_attempt_id", "filetype", "filename", "tilename", "expnum", "ccdnum"]
indexes = ["pfw_attempt_id",  "filename, filetype", "tilename", "expnum, ccdnum"]

select = "SELECT  {} FROM y6a1_image ;"
create = "CREATE TABLE IF NOT EXISTS y6a1_image {} ;"

so make indices likes..

CREATE INDEX IF NOT EXISTS y6a1_image__pfw_attempt_id_idx ON y6a1_image (pfw_attempt_id) ;
CREATE INDEX IF NOT EXISTS y6a1_image__filename_filetype_idx ON y6a1_image (filename, filetype) ;
CREATE INDEX IF NOT EXISTS y6a1_image__tilename_idx ON y6a1_image (tilename) ;
CREATE INDEX IF NOT EXISTS y6a1_image__expnum_ccdnum_idx ON y6a1_image (expnum, ccdnum) ;
```

Wihtout deletind the old fielname, filetype indices, speed increased. ~680 (all) commponent 
CCD were returned in unuder 1 minute 18 seconds 

``` 
EXPLAIN QUERY PLAN select fai.path,fai.filename
from y6a1_proctag t, y6a1_image i, y6a1_image j, y6a1_file_archive_info fai
where t.tag='Y6A1_COADD'
    and t.pfw_attempt_id=i.pfw_attempt_id
    and i.tilename='DES0000+0209'
    and i.filetype='coadd_nwgint'
    and i.expnum=j.expnum and i.ccdnum=j.ccdnum
    and j.filetype='red_immask'
    and j.filename=fai.filename;

(9, 0, 0, 'SEARCH TABLE y6a1_image AS i USING INDEX y6a1_image__tilename_idx (tilename=?)')
(16, 0, 0, 'SEARCH TABLE y6a1_proctag AS t USING INDEX y6a1_proctag__pfw_attempt_id_idx (pfw_attempt_id=?)')
(25, 0, 0, 'SEARCH TABLE y6a1_image AS j USING INDEX y6a1_image__expnum_ccdnum_idx (expnum=? AND ccdnum=?)')
(36, 0, 0, 'SEARCH TABLE y6a1_file_archive_info AS fai USING INDEX y6a1_file_archive_info__filename_idx (filename=?)')
(base) [donaldp@deslogin desdm_flat_file_investigation]$ ./ingest.py query uc-se-into-coadd
How to find the single epoch images that went into a tile
select fai.path,fai.filename
from y6a1_proctag t, y6a1_image i, y6a1_image j, y6a1_file_archive_info fai
where t.tag='Y6A1_COADD'
    and t.pfw_attempt_id=i.pfw_attempt_id
    and i.tilename='DES0000+0209'
    and i.filetype='coadd_nwgint'
    and i.expnum=j.expnum and i.ccdnum=j.ccdnum
    and j.filetype='red_immask'
    and j.filename=fai.filename;

``
 

