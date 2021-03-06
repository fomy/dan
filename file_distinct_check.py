#!/usr/bin/python
import sys
import getopt
import itertools
import math

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
    fileset = {}
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        dflines = list(itertools.islice(trace, fnum))

        for file in dflines:
            params = file.split()
            id = params[1]
            if id not in fileset:
                fileset[id] = int(params[2])

    sum = 0
    count = 0
    for file in fileset:
        sum += fileset[file]
        count += 1
        print fileset[file]

    print >>sys.stderr, "mean = %8.5f" % (1.0*sum/count)


def check_each_pair(trace):
    # (fid1,fid2) : chunknum
    # fid1 < fid2
    same_suffix = 0
    # {(suf1, suf2) : num}
    diff_suffix = {}
    coefficient = 0
    num = 0

    suffixset = {}
    fileset = {}
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break
        fnum = int(line[0].split()[1])
        dflines = list(itertools.islice(trace, fnum))

        chunk = line[0].split()[2]

        for i in xrange(len(dflines)):
            file_i = dflines[i].split()
            if int(file_i[1]) not in fileset:
                fileset[int(file_i[1])] = None
            if file_i[4] not in suffixset:
                suffixset[file_i[4]] = None

            for j in xrange(i+1, len(dflines)):
                num += 1
                file_j = dflines[j].split()
                if file_i[-1] == file_j[-1]:
                    # a pair of similar/identical files; skip
                    continue
                assert(int(file_i[1]) < int(file_j[1]))

                # suffix
                if file_i[4] != file_j[4]:
                    if file_i[4] < file_j[4]:
                        diff = (file_i[4], file_j[4])
                    else:
                        diff = (file_j[4], file_i[4])

                    if diff not in diff_suffix:
                        diff_suffix[diff] = 0
                    diff_suffix[diff] += 1

                f1size = int(file_i[2])
                f2size = int(file_j[2])
                mean = (f1size + f2size)/2.0
                dev = ((f1size - mean)*(f1size - mean) + (f2size - mean)*(f2size - mean))/2.0
                co = math.sqrt(dev)/mean
                print "%.4f" % co
                coefficient += co 

    coefficient /= num
    print >>sys.stderr, "%d files, %d suffixes,  %d pairs" % (len(fileset), len(suffixset), num)
    print >>sys.stderr, "The coefficient of sizes of distinct files: %10f" % coefficient

    top_diff = []
    for diff in diff_suffix:
        top_diff.append((diff_suffix[diff], diff))
        top_diff = sorted(top_diff, reverse=True)
        if(len(top_diff) > 10):
            top_diff.pop()

    print >>sys.stderr, top_diff


if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "fsc")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-f"]:
            how_many_files(trace)
        elif o in ["-s"]:
            get_file_size_distribution(trace)
        elif o in ["-c"]:
            check_each_pair(trace)

    trace.close()
