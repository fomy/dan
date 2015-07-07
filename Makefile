DB_HOME = /usr/local/BerkeleyDB.6.1

INCLUDE = -I ${DB_HOME}/include
LIBS = -L ${DB_HOME}/lib -ldb

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector refs_analyzer locality_analyzer chunksize_analyzer

collector:store data collector.c libhashfile.c
	$(CC) $(CFLAGS) $(INCLUDE) collector.c libhashfile.c -o collector store.o data.o $(LIBS)

refs_analyzer:store data refs_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) refs_analyzer.c -o refs_analyzer store.o data.o $(LIBS)

locality_analyzer:store data locality_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) locality_analyzer.c -o locality_analyzer store.o data.o $(LIBS)

chunksize_analyzer:store data chunksize_analyzer.c
	$(CC) $(CFLAGS) $(INCLUDE) chunksize_analyzer.c -o chunksize_analyzer store.o data.o $(LIBS)

store:store.c
	$(CC) $(CFLAGS) -c store.c $(INCLUDE)

data:data.c
	$(CC) $(CFLAGS) -c data.c

clean:
	rm *.o collector refs_analyzer locality_analyzer chunksize_analyzer
