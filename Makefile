

#
# make all the list of files needed. 
#
IMPORT_FILES     := $(wildcard imports/*.import)
# schema files allow us to say which columsn to ingest
SCHEMA_FILES     := $(patsubst imports/%.import,schemas/%.schema ,$(IMPORT_FILES))
# create files  indicatet taht create statmensts ahev been issue to the DB
CREATE_FILES     := $(patsubst imports/%.import,creates/%.create ,$(IMPORT_FILES))
# export fiels are teh data expored from oracel (intenrally CSV format)
EXPORT_FILES     := $(patsubst imports/%.import,exports/%.export ,$(IMPORT_FILES))
# ingest files are tiemstamps indicating when an .import files was ingested into the DB
INGEST_FILES     := $(patsubst imports/%.import,ingests/%.ingest ,$(IMPORT_FILES))
# Index .def files define the indices to be put on a table  when indices on a table were created.
INDEX_DEF_FILES  := $(patsubst imports/%.import,indicies/%.def   ,$(IMPORT_FILES))
INDEX_TIME_FILES := $(patsubst imports/%.import,indicies/%.time   ,$(IMPORT_FILES))

# from a file naming a table to import to ..
# a CSV alloing us to say what columns to import
./schemas/%.schema:./imports/%.import 
	./ingest.py define $< > temp.sql
	sqlplus donaldp/don70chips2@desdb-sci.ncsa.illinois.edu/dessci @temp.sql
	rm temp.sql

./creates/%.create:./schemas/%.schema 
	./ingest.py create $<

./exports/%.export:./schemas/%.schema 
	./ingest.py export $< > temp.sql
	cat temp.sql
	sqlplus donaldp/don70chips2@desdb-sci.ncsa.illinois.edu/dessci @temp.sql
	rm temp.sql

./ingests/%.ingest:./exports/%.export
	./ingest.py ingest $< 
	touch $@

./indicies/%.time: ./indicies/%.def
	echo updating time $< and making index 
	touch $@
	(file=$@; ./ingest.py index "$${file%.*}.def")

./indicies/%.def : ./ingests/%.ingest
	true 
	#./ingest.py index $@
	#touch $@ # update or create the def file 


all : all_creates all_indicies

all_indicies : $(INDEX_TIME_FILES)
	echo 

all_schema : $(SCHEMA_FILES)

all_creates : $(CREATE_FILES)

all_exported : $(EXPORT_FILES)

all_ingests : $(INGEST_FILES)



clean:
	rm -f exports/*
	rm -f creates/*
	rm -f exports/*
	rm -f ingests/*
	rm -f indicies/*.time

scrub: clean
	rm -f desdm-test.db
	rm -f schemas/*

#.schema:.csv 
#	.ingest.py export $<  > tmp.sql
#	dessci @temp.sql
#	rm temp.sql
#
#.csv:.dbtim:
#	./ingest.py  ingest  $< > tmp.sql
#	dessci @temp.sql
#	rm temp.sql
#



