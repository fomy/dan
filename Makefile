DB_HOME = /usr/local/BerkeleyDB.6.1

INCLUDE = -I ${DB_HOME}/include
LIBS = -L ${DB_HOME}/lib -ldb

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector 
	$(MAKE) refs_distribution_exporter refs_locality_exporter
	$(MAKE) chunk_refs_distance_analyzer chunk_size_analyzer 
	$(MAKE) file_refs_source_analyzer file_size_analyzer file_type_analyzer

collector:store data collector.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) collector.c -o collector store.o data.o libhashfile.o $(LIBS) -lcrypto

# reference analyzer
refs_distribution_exporter:store data refs_distribution_exporter.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_distribution_exporter.c -o refs_distribution_exporter store.o data.o $(LIBS)

refs_locality_exporter:store data refs_locality_exporter.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) refs_locality_exporter.c -o refs_locality_exporter store.o data.o libhashfile.o $(LIBS)

# chunk analyzer
chunk_refs_distance_analyzer:store data chunk_refs_distance_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_refs_distance_analyzer.c -o chunk_refs_distance_analyzer store.o data.o $(LIBS)

chunk_size_analyzer:store data chunk_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_size_analyzer.c -o chunk_size_analyzer store.o data.o $(LIBS)

# file analyzer
file_refs_source_analyzer:store data file_refs_source_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_refs_source_analyzer.c -o file_refs_source_analyzer store.o data.o $(LIBS)

file_size_analyzer:store data file_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_size_analyzer.c -o file_size_analyzer store.o data.o $(LIBS) -lglib

file_type_analyzer:store data file_type_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_type_analyzer.c -o file_type_analyzer store.o data.o $(LIBS) -lglib

store:store.c
	$(CC) $(CFLAGS) -c store.c $(INCLUDE)

data:data.c
	$(CC) $(CFLAGS) -c data.c

libhashfile:libhashfile.c
	$(CC) $(CFLAGS) -c libhashfile.c

clean:
	rm *.o 
	rm collector 
	rm refs_distribution_exporter refs_locality_exporter
	rm chunk_refs_distance_analyzer chunk_size_analyzer 
	rm file_refs_source_analyzer file_size_analyzer file_type_analyzer
