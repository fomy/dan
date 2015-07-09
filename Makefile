DB_HOME = /usr/local/BerkeleyDB.6.1

INCLUDE = -I ${DB_HOME}/include
LIBS = -L ${DB_HOME}/lib -ldb

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector refs_distribution distance_analyzer chunksize_analyzer refs_analyzer filesize_analyzer locality filetype_analyzer

collector:store data collector.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) collector.c -o collector store.o data.o libhashfile.o $(LIBS) -lcrypto

refs_distribution:store data refs_distribution.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_distribution.c -o refs_distribution store.o data.o $(LIBS)

distance_analyzer:store data distance_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) distance_analyzer.c -o distance_analyzer store.o data.o $(LIBS)

chunksize_analyzer:store data chunksize_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunksize_analyzer.c -o chunksize_analyzer store.o data.o $(LIBS)

refs_analyzer:store data refs_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_analyzer.c -o refs_analyzer store.o data.o $(LIBS)

filesize_analyzer:store data filesize_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) filesize_analyzer.c -o filesize_analyzer store.o data.o $(LIBS) -lglib

locality:store data locality.c libhashfile
	$(CC) $(CFLAGS) $(INCLUDE) locality.c -o locality store.o data.o libhashfile.o $(LIBS)

filetype_analyzer:store data filetype_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) filetype_analyzer.c -o filetype_analyzer store.o data.o $(LIBS) -lglib

store:store.c
	$(CC) $(CFLAGS) -c store.c $(INCLUDE)

data:data.c
	$(CC) $(CFLAGS) -c data.c

libhashfile:libhashfile.c
	$(CC) $(CFLAGS) -c libhashfile.c

clean:
	rm *.o collector refs_distribution distance_analyzer chunksize_analyzer refs_analyzer filesize_analyzer locality filetype_analyzer
