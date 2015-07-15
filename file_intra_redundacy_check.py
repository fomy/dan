#!/usr/bin/python
import sys
import getopt
import itertools

def get_file_size(trace):
    mean = 0;
    count = 0
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;

        size = int(line[0].split()[2])
        mean += size
        count += 1
        print size 

    print >>sys.stderr, "mean = %.2f" % (1.0*mean/count)

def get_popular_types(trace):
    # {suffix : [file num, file size, redundant chunk size]}
    suffixset = {}
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;

        print line
        file = line[0].split()
        if file[3] in suffixset:
            suffixset[file[3]][0] += 1 
            suffixset[file[3]][1] += int(file[2])
            suffixset[file[3]][2] += int(file[5])
        else:
            suffixset[file[3]] = [1, int(file[2]), int(file[5])]

    top_suffix = []
    for suf in suffixset:
        top_suffix.append((suffixset[suf][0], suf))
        top_suffix = sorted(top_suffix, reverse=True)
        if(len(top_suffix) > 10):
            top_suffix.pop()

    print "In number:"
    print top_suffix

    top_suffix = []
    for suf in suffixset:
        top_suffix.append((suffixset[suf][1], suf))
        top_suffix = sorted(top_suffix, reverse=True)
        if(len(top_suffix) > 10):
            top_suffix.pop()

    print "In size:"
    print top_suffix

    print "In number:"
    print top_suffix

    top_suffix = []
    for suf in suffixset:
        top_suffix.append((suffixset[suf][2], suf))
        top_suffix = sorted(top_suffix, reverse=True)
        if(len(top_suffix) > 10):
            top_suffix.pop()

    print "In redundant chunk size:"
    print top_suffix

# find all identical files
# -s check file size distribution 
# -p output popular suffix
if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "ps")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-p"]:
            get_popular_types(trace)
        elif o in ["-s"]:
            get_file_size(trace)

    trace.close()
