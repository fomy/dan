DB_HOME = /usr/local/BerkeleyDB.6.1

INCLUDE = -I ${DB_HOME}/include
LIBS = -L ${DB_HOME}/lib -ldb

CFLAGS = -O2 -g
CC = gcc
MAKE = make

all:
	$(MAKE) collector

collector:store data collector.c libhashfile.c
	$(CC) $(CFLAGS) $(INCLUDE) collector.c libhashfile.c -o collector store.o data.o $(LIBS)

store:store.c
	$(CC) $(CFLAGS) -c store.c $(INCLUDE)

data:data.c
	$(CC) $(CFLAGS) -c data.c

clean:
	rm *.o collector
