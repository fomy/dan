#DB_HOME = /usr/local/BerkeleyDB.6.1

#INCLUDE = -I ${DB_HOME}/include
#DBLIBS = -L ${DB_HOME}/lib -ldb

DBLIBS = -lhiredis -lglib

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector 
	$(MAKE) refs_distribution_exporter refs_locality_exporter simd_exporter
	$(MAKE) chunk_refs_distance_analyzer chunk_size_analyzer 
	$(MAKE) file_refs_source_analyzer file_size_analyzer file_type_analyzer file_exporter

collector:store collector.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) collector.c store.o data.o -o collector libhashfile.o -lcrypto $(DBLIBS)

# reference analyzer
refs_distribution_exporter:store refs_distribution_exporter.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_distribution_exporter.c store.o data.o -o refs_distribution_exporter $(DBLIBS)

refs_locality_exporter:store refs_locality_exporter.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) refs_locality_exporter.c store.o data.o -o refs_locality_exporter libhashfile.o $(DBLIBS)

# chunk analyzer
chunk_refs_distance_analyzer:store chunk_refs_distance_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_refs_distance_analyzer.c store.o data.o -o chunk_refs_distance_analyzer $(DBLIBS)

chunk_size_analyzer:store chunk_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_size_analyzer.c store.o data.o -o chunk_size_analyzer $(DBLIBS)

# file analyzer
file_refs_source_analyzer:store file_refs_source_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_refs_source_analyzer.c store.o data.o -o file_refs_source_analyzer $(DBLIBS)

file_size_analyzer:store file_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_size_analyzer.c store.o data.o -o file_size_analyzer -lglib $(DBLIBS) -lm

file_type_analyzer:store file_type_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) file_type_analyzer.c store.o data.o -o file_type_analyzer -lglib $(DBLIBS)

file_exporter:store file_exporter.c
	$(CC) $(CFLAGS) $(INCLUDE) file_exporter.c store.o data.o -o file_exporter -lglib $(DBLIBS)

# SIMD trace exportr
simd_exporter:store simd_exporter.c
	$(CC) $(CFLAGS) $(INCLUDE) simd_exporter.c store.o data.o -o simd_exporter $(DBLIBS)

store:store.c data.c
	$(CC) $(CFLAGS) -c store.c -o store.o $(INCLUDE)
	$(CC) $(CFLAGS) -c data.c -o data.o $(INCLUDE)

libhashfile:libhashfile.c
	$(CC) $(CFLAGS) -c libhashfile.c

clean:
	rm *.o
	rm collector 
	rm refs_distribution_exporter refs_locality_exporter
	rm chunk_refs_distance_analyzer chunk_size_analyzer 
	rm file_refs_source_analyzer file_size_analyzer file_type_analyzer file_exporter
	rm simd_exporter
