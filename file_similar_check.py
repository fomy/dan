#!/usr/bin/python
import sys
import getopt
import itertools
import math

def get_file_size_coefficient(trace):
    co = 0
    co_count = 0
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        assert(fnum > 1)
        sflines = list(itertools.islice(trace, fnum))

        m = 0
        m_2 = 0
        c = 0
        for line in sflines:
            size = int(line.split()[2])
            m += size
            m_2 += size * size
            c += 1

        m = 1.0*m/c
        m_2 = 1.0*m_2/c
        d_2 = m_2 - m*m
        d = math.sqrt(d_2)
        print "%8.5f" % (d/m)
        co += d/m
        co_count+=1

    print >>sys.stderr, "mean = %8.5f" % (1.0*co/co_count)

def get_file_size_distribution(trace):
    mean = 0;
    count = 0
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        assert(fnum > 1)
        sflines = list(itertools.islice(trace, fnum))

        for line in sflines:
            size = int(line.split()[2])
            mean += size
            count += 1
            print size 

    print >>sys.stderr, "mean = %8.5f" % (1.0*mean/count)

def get_popular_types(trace):
    # {'suffix' : [file number, file size], ...}
    suffixset = {}
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        iflines = list(itertools.islice(trace, fnum))

        files = [ifl.split() for ifl in iflines]
        for file in files: 
            if file[-2] in suffixset:
                suffixset[file[-2]][0] += 1 
                suffixset[file[-2]][1] += int(file[2])
            else:
                suffixset[file[-2]] = [1, int(file[2])]

    print "In number:"
    top_suffix = []
    for suf in suffixset:
        top_suffix.append((suffixset[suf][0], suf))
        top_suffix = sorted(top_suffix, reverse=True)
        if(len(top_suffix) > 10):
            top_suffix.pop()

    print "In size:"
    print top_suffix

    top_suffix = []
    for suf in suffixset:
        top_suffix.append((suffixset[suf][1], suf))
        top_suffix = sorted(top_suffix, reverse=True)
        if(len(top_suffix) > 10):
            top_suffix.pop()

    print top_suffix

if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "dcp")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-d"]:
            get_file_size_distribution(trace)
        elif o in ["-c"]:
            get_file_size_coefficient(trace)
        elif o in ["-p"]:
            get_popular_types(trace)

    trace.close()
