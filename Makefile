DB_HOME = /usr/local/BerkeleyDB.6.1

INCLUDE = -I ${DB_HOME}/include
LIBS = -L ${DB_HOME}/lib -ldb

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector refs_analyzer locality_analyzer chunksize_analyzer filenum_analyzer filesize_analyzer sequence

collector:store data collector.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) collector.c -o collector store.o data.o libhashfile.o $(LIBS)

refs_analyzer:store data refs_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_analyzer.c -o refs_analyzer store.o data.o $(LIBS)

locality_analyzer:store data locality_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) locality_analyzer.c -o locality_analyzer store.o data.o $(LIBS)

chunksize_analyzer:store data chunksize_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunksize_analyzer.c -o chunksize_analyzer store.o data.o $(LIBS)

filenum_analyzer:store data filenum_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) filenum_analyzer.c -o filenum_analyzer store.o data.o $(LIBS)

filesize_analyzer:store data filesize_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) filesize_analyzer.c -o filesize_analyzer store.o data.o $(LIBS) -lglib

sequence:store data sequence.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) sequence.c -o sequence store.o data.o libhashfile.o $(LIBS)

store:store.c
	$(CC) $(CFLAGS) -c store.c $(INCLUDE)

data:data.c
	$(CC) $(CFLAGS) -c data.c

libhashfile:libhashfile.c
	$(CC) $(CFLAGS) -c libhashfile.c

clean:
	rm *.o collector refs_analyzer locality_analyzer chunksize_analyzer filenum_analyzer filesize_analyzer sequence
