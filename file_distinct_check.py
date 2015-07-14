#!/usr/bin/python
import sys
import getopt
import itertools

def how_many_files(trace):
    fileset = {}
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        dflines = list(itertools.islice(trace, fnum))

        for file in dflines:
            id = file.split()[1]
            if id in fileset:
                fileset[id] += 1
            else:
                fileset[id] = 1

    #print fileset
    print len(fileset)

def get_file_size_distribution(trace):
    sum = 0;
    count = 0
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        sflines = list(itertools.islice(trace, fnum))

        for line in sflines:
            size = int(line.split()[2])
            sum += size
            count += 1
            print size 

    print >>sys.stderr, "mean = %8.5f" % (1.0*sum/count)


# find all identical files
# -n check names
# -s check file size distribution 
# -p output popular suffix
if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "fd")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-f"]:
            how_many_files(trace)
        elif o in ["-d"]:
            get_file_size_distribution(trace)

    trace.close()
