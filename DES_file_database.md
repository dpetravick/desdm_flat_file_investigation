# The SQLITE file database

The DES file database is a SQLITE file containing information about DES files. The database file can be queried to obtain the paths to DESDM files of files of interest. For example the data base supports queries identifying the single epoch images used to create a coadded image.

The main componets of the database are subsets of tables from DESDM and
database indexes to support queries. Tables are subsetted to discard columns less likely to to be used locating files.  The subsetting is motivated by experiments with complete tables failing to produce performant querites.

The principal use case for this database is to support Matt Becker's TBD tool.  

The database is distributed compressed with zstandard, and must be decompressed before use.  The distribution includes md5 checksums, and a report giving reference timing and index utilization for a number of test queries.
