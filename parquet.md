# parquet file releases

A parquet release holds data from a number of DESDM ORACLE DB tables under a root directory.
Data related to a table is held under a directory named for the table.

Parquet files are the persisent record of data from the DESDM Oracle databses.   Only tables chosen by the  working group were saved in this way. All columns and all row from a given table  are saved.  

A parquet release holds data from a number of DESDM ORACLE DB tables under a root directory.
The root directory has sub directories indicating, which database was used to extract the table.
For example data obtained from DESSCI are held under a DESSCI directoy.  A copy of thie file is
placed at the root of the distribion as well.

PANDAS is used as the basis for data handling, in particlar PANDAS handles the Type conversions from the ORACLE type system to the parquet type system.

## parquet files

Data from an ORACLE table is placed in parquet files  in a directory named for table.
 Data from large tables is split row-wise into a number of parquet files.   Parquet reccomendations are followed, columns are packed left-to-right, wich columns with ordered by cardinality, lowest cardinality to the left of the highest cardinality columns.  Within the prquet file, each column is stored under the root  Parquet files are compressed with zstandard method.

An MD5 files are generated for each parquet files, and a report file is places in the directory documenting the processes used to generate the data. The report includes  Oracle -> pandas -> parquet  type mapping information.
