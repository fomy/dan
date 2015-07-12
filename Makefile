DB_HOME = /usr/local/BerkeleyDB.6.1

INCLUDE = -I ${DB_HOME}/include
DBLIBS = -L ${DB_HOME}/lib -ldb
LIBS = -L . -lstore

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector 
	$(MAKE) refs_distribution_exporter refs_locality_exporter
	$(MAKE) chunk_refs_distance_analyzer chunk_size_analyzer 
	$(MAKE) file_refs_source_analyzer file_size_analyzer file_type_analyzer file_exporter

collector:store collector.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) collector.c -o collector libhashfile.o -lcrypto $(LIBS)

# reference analyzer
refs_distribution_exporter:store refs_distribution_exporter.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_distribution_exporter.c -o refs_distribution_exporter $(LIBS)

refs_locality_exporter:store refs_locality_exporter.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) refs_locality_exporter.c -o refs_locality_exporter libhashfile.o $(LIBS)

# chunk analyzer
chunk_refs_distance_analyzer:store chunk_refs_distance_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_refs_distance_analyzer.c -o chunk_refs_distance_analyzer $(LIBS)

chunk_size_analyzer:store chunk_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_size_analyzer.c -o chunk_size_analyzer $(LIBS)

# file analyzer
file_refs_source_analyzer:store file_refs_source_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_refs_source_analyzer.c -o file_refs_source_analyzer $(LIBS)

file_size_analyzer:store file_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_size_analyzer.c -o file_size_analyzer -lglib $(LIBS)

file_type_analyzer:store file_type_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_type_analyzer.c -o file_type_analyzer  -lglib $(LIBS)

file_exporter:store file_exporter.c
	$(CC) $(CFLAGS) $(INCLUDE) file_exporter.c -o file_exporter  -lglib $(LIBS)

store:store.c data.c
	$(CC) $(CFLAGS) -shared store.c data.c -o libstore.so $(INCLUDE) $(DBLIBS)

libhashfile:libhashfile.c
	$(CC) $(CFLAGS) -c libhashfile.c

clean:
	rm *.o libstore.so 
	rm collector 
	rm refs_distribution_exporter refs_locality_exporter
	rm chunk_refs_distance_analyzer chunk_size_analyzer 
	rm file_refs_source_analyzer file_size_analyzer file_type_analyzer file_exporter
