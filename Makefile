#DB_HOME = /usr/local/BerkeleyDB.6.1

#INCLUDE = -I ${DB_HOME}/include
#DBLIBS = -L ${DB_HOME}/lib -ldb

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector 
	$(MAKE) chunk_size_analyzer reference_analyzer refs_source_analyzer
	#$(MAKE) collision_detector 
	#$(MAKE) refs_distribution_exporter refs_locality_exporter simd_exporter simd_reverse_exporter
	#$(MAKE) chunk_refs_distance_analyzer chunk_size_analyzer 
	#$(MAKE) file_refs_source_analyzer file_size_analyzer file_type_analyzer file_exporter

collector:store collector.c libhashfile store_stat.c
	$(CC) $(CFLAGS) $(INCLUDE) collector.c -o collector libhashfile.o store.o data.o lru_cache.o -lcrypto -lglib -L/usr/local/BerkeleyDB.6.1/lib -ldb
	$(CC) $(CFLAGS) $(INCLUDE) store_stat.c -o store_stat libhashfile.o store.o data.o lru_cache.o -lcrypto -lglib -L/usr/local/BerkeleyDB.6.1/lib -ldb

#collision_detector:collision_detector.c libhashfile store
	#$(CC) $(CFLAGS) $(INCLUDE) collision_detector.c -o collision_detector data.o libhashfile.o -lglib

# reference analyzer
reference_analyzer:store reference_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) reference_analyzer.c -o reference_analyzer store.o data.o lru_cache.o -lcrypto -lglib -L/usr/local/BerkeleyDB.6.1/lib -ldb

#refs_locality_exporter:store refs_locality_exporter.c libhashfile
	#$(CC) $(CFLAGS) $(INCLUDE) refs_locality_exporter.c store.o data.o -o refs_locality_exporter libhashfile.o $(DBLIBS)

# chunk analyzer
#chunk_refs_distance_analyzer:store chunk_refs_distance_analyzer.c
	#$(CC) $(CFLAGS) $(INCLUDE) chunk_refs_distance_analyzer.c store.o data.o -o chunk_refs_distance_analyzer $(DBLIBS)

chunk_size_analyzer:store chunk_size_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunk_size_analyzer.c -o chunk_size_analyzer store.o data.o lru_cache.o -lcrypto -lglib -L/usr/local/BerkeleyDB.6.1/lib -ldb

# file analyzer
refs_source_analyzer:store refs_source_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_source_analyzer.c -o refs_source_analyzer store.o data.o lru_cache.o -lcrypto -lglib -L/usr/local/BerkeleyDB.6.1/lib -ldb

#file_size_analyzer:store file_size_analyzer.c
	#$(CC) $(CFLAGS) $(INCLUDE) file_size_analyzer.c store.o data.o -o file_size_analyzer -lglib $(DBLIBS) -lm

#file_type_analyzer:store file_type_analyzer.c
	#$(CC) $(CFLAGS) $(INCLUDE) file_type_analyzer.c store.o data.o -o file_type_analyzer -lglib $(DBLIBS)

#file_exporter:store file_exporter.c
	#$(CC) $(CFLAGS) $(INCLUDE) file_exporter.c store.o data.o -o file_exporter -lglib $(DBLIBS)

# SIMD trace exportr
#simd_exporter:store simd_exporter.c libhashfile
	#$(CC) $(CFLAGS) $(INCLUDE) simd_exporter.c store.o data.o -o simd_exporter libhashfile.o -lglib  $(DBLIBS)

#simd_reverse_exporter:store simd_reverse_exporter.c libhashfile
	#$(CC) $(CFLAGS) $(INCLUDE) simd_reverse_exporter.c store.o data.o -o simd_reverse_exporter libhashfile.o -lglib  $(DBLIBS)

store:store.c data.c lru_cache.c
	$(CC) $(CFLAGS) -c store.c -I /usr/local/BerkeleyDB.6.1/include/
	$(CC) $(CFLAGS) -c data.c
	$(CC) $(CFLAGS) -c lru_cache.c

libhashfile:libhashfile.c
	$(CC) $(CFLAGS) -c libhashfile.c

clean:
	rm *.o
	rm collector 
	#rm collision_detector
	#rm refs_distribution_exporter refs_locality_exporter
	#rm chunk_refs_distance_analyzer chunk_size_analyzer 
	#rm file_refs_source_analyzer file_size_analyzer file_type_analyzer file_exporter
	#rm simd_exporter simd_reverse_exporter
