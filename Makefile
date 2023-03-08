IMPORT_DIR  := ./d1_imports
SCHEMA_DIR  := ./d2_schemas
CREATE_DIR  := ./d3_creates
EXPORT_DIR  := ./d4_exports
INGEST_DIR  := ./d5_ingests
INDEX_DIR   := ./d6_indicies
#
# make all the list of files needed. 
#
IMPORT_FILES     := $(wildcard $(IMPORT_DIR)/*.import)

# schema files allow us to say which columsn to ingest
SCHEMA_FILES     := $(patsubst $(IMPORT_DIR)/%.import,$(SCHEMA_DIR)/%.schema ,$(IMPORT_FILES))

# create files  indicatet taht create statmensts ahev been issue to the DB
CREATE_FILES     := $(patsubst $(IMPORT_DIR)/%.import,$(CREATE_DIR)/%.create ,$(IMPORT_FILES))

# export fiels are teh data expored from oracel (intenrally CSV format)
EXPORT_FILES     := $(patsubst $(IMPORT_DIR)/%.import,$(EXPORT_DIR)/%.export ,$(IMPORT_FILES))

# ingest files are tiemstamps indicating when an .import files was ingested into the DB
INGEST_FILES     := $(patsubst $(IMPORT_DIR)/%.import,$(INGEST_DIR)/%.ingest ,$(IMPORT_FILES))

# Index .def files define the indices to be put on a table  when indices on a table were created.
INDEX_DEF_FILES  := $(patsubst $(IMPORT_DIR)/%.import,$(INDEX_DIR)/%.def   ,$(IMPORT_FILES))
INDEX_TIME_FILES := $(patsubst $(IMPORT_DIR)/%.import,$(INDEX_DIR)/%.time   ,$(IMPORT_FILES))

# from a file naming a table to import to ..
# a CSV alloing us to say what columns to import
./$(SCHEMA_DIR)/%.schema:./$(IMPORT_DIR)/%.import 
	echo $@ $<
	./ingest.py define $< > temp.sql   # make schema CSV files 
	sqlplus donaldp/don70chips2@desdb-sci.ncsa.illinois.edu/dessci @temp.sql  > /dev/null
	rm temp.sql

$(CREATE_DIR)/%.create:./$(SCHEMA_DIR)/%.schema 
	./ingest.py create $< -o $(CREATE_DIR) # make tables in SQLITE 

$(EXPORT_DIR)/%.export:./$(SCHEMA_DIR)/%.schema 
	./ingest.py export $< > temp.sql -o $(EXPORT_DIR)  # ORACLE DATA -> csv
	cat temp.sql
	sqlplus donaldp/don70chips2@desdb-sci.ncsa.illinois.edu/dessci @temp.sql > /dev/null
	rm temp.sql

$(INGEST_DIR)/%.ingest:$(EXPORT_DIR)/%.export
	./ingest.py ingest $<     # CSV -> sqlite 
	touch $@

$(INDEX_DIR)/%.time: $(INDEX_DIR)/%.def
	echo updating time $< and making index 
	touch $@ #hey
	(file=$@; ./ingest.py index "$${file%.*}.def") # make index in sqlite

$(INDEX_DIR)/%.def : $(INGEST_DIR)/%.ingest
	true   
	#./ingest.py index $@
	#touch $@ # update or create the def file 

test: 
	./ingest.py test_db

all : all_creates all_indicies

all_indicies : $(INDEX_TIME_FILES)
	echo 

all_schema : $(SCHEMA_FILES)

all_creates : $(CREATE_FILES)

all_exported : $(EXPORT_FILES)

all_ingests : $(INGEST_FILES)



clean:
	rm -f $(CREATE_DIR)/*
	rm -f $(EXPORT_DIR)/*
	rm -f $(INGEST_DIR)/*
	rm -f $(INDEX_DIR)/*.time
	rm -f desdm-test.db

scrub: clean
	rm -f $(SCHEMA_DIR)/*

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


