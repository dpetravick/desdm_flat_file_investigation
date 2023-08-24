IMPORT_DIR  := ./d1_imports
SCHEMA_DIR  := ./d2_schemas
EXPORT_DIR  := ./d3_exports
CREATE_DIR  := ./d4_creates
INGEST_DIR  := ./d5_ingests
INDEX_DIR   := ./d6_indicies

DB            := desdm-test.db 
ANALYSIS_LOG  := desdm-test.analysis.log
TEST_LOG      := desdm-test-queries.log
SHOW_LOG      := desdm-test-tables.log
DIST_ROOT     := /home/donaldp/public_html/desdm-file-db-$(shell date "+%y-%m-%d-%H-%M")/
INGEST_FLAGS  := --loglevel INFO --db ${DB}
DIST_MD5      := ${DIST_ROOT}desdm-test.db.md5

# make all the list of files needed. 
#
IMPORT_FILES     := $(wildcard $(IMPORT_DIR)/*.import)

# schema files allow us to say which columsn to ingest
SCHEMA_FILES     := $(patsubst $(IMPORT_DIR)/%.import,$(SCHEMA_DIR)/%.schema ,$(IMPORT_FILES))

# create files  indicatet taht create statmensts ahev been issue to the DB
CREATE_FILES     := $(patsubst $(IMPORT_DIR)/%.import,$(CREATE_DIR)/%.create ,$(IMPORT_FILES))

# export files are teh data expored from oracel (intenrally CSV format)
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
	./ingest.py sqlpus @temp.sql  > /dev/null
$(CREATE_DIR)/%.create:./$(SCHEMA_DIR)/%.schema 
	./ingest.py create $< -o $(CREATE_DIR) # make tables in SQLITE 

$(EXPORT_DIR)/%.export:./$(SCHEMA_DIR)/%.schema 
	./ingest.py export $< > temp.sql -o $(EXPORT_DIR)  # ORACLE DATA -> csv
	cat temp.sql
	./ingest.py sqlpus @temp.sql  > /dev/null
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


all : creates indicies


indicies : $(INDEX_TIME_FILES)

schema : $(SCHEMA_FILES)

creates : $(CREATE_FILES)

exported : $(EXPORT_FILES)

ingests : $(INGEST_FILES)

############
#  tests 
############
test :
	./ingest.py  ${INGEST_FLAGS} test_db | tee ${TEST_LOG}

analysis: ${ANALYSIS_LOG} 

${ANALYSIS_LOG} : ${DB}
	sqlite3_analyzer ${DB} > ${ANALYSIS_LOG}

############
#  copy to web area
############
publish :
	mkdir -p          ${DIST_ROOT}
	./ingest.py compress desdm-test.db  ${DIST_ROOT}desdm-test.db.zst
	md5sum dog | cut -f 1 -d' ' > ${DIST_MD5}
	cp ${TEST_LOG}    ${DIST_ROOT}
	cp ingest.toml    ${DIST_ROOT}
	./ingest.py show > ${SHOW_LOG}
	ls  ${DIST_ROOT}

#############
# Cleaning
#############


# all timestamps in the index dir
clean_indicies:
	rm -f $(INDEX_DIR)/*.time  #remove index created time stame

# the Sqlite databse and associated time stamos but not
#  the stuff from ORACLE  remove the dababase and re-stuff
clean : clean_indicies
	rm -f $(CREATE_DIR)/*  #create time stamps
	rm -f $(INGEST_DIR)/*  #ingest time stamps
	rm -f desdm-test.db    #database file 
	rm ${TEST_LOG}

scrub: clean
	rm -f $(SCHEMA_DIR)/*
	rm -f $(EXPORT_DIR)/*




