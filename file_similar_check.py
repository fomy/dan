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
    sum = 0;
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
            sum += size
            count += 1
            print size 

    print >>sys.stderr, "mean = %8.5f" % (1.0*sum/count)

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

def check_each_similar_pair(sufs, names, hash):
    same_name = 0
    same_suffix = 0
    pair_num = 0
    for i in range(len(sufs)):
        for j in range(len(sufs)):
            if hash[i] == hash[j]:
                continue
            pair_num += 1
            if(sufs[i] == sufs[j]):
                same_suffix += 1
            if(names[i] == names[j]):
                same_name += 1
    return (same_name, same_suffix, pair_num)


# Are similar files with identical suffixes
def check_name_and_type(trace):
    bin_num = 0
    file_num = 0
    suffix_num = 0
    name_num = 0

    total_same_name = 0
    total_same_suffix = 0
    total_pair_num = 0

    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])

        bin_num += 1
        file_num += fnum

        iflines = list(itertools.islice(trace, fnum))
        sufs = [ifl.split()[-2] for ifl in iflines]
        names = [ifl.split()[3] for ifl in iflines]
        hash = [ifl.split()[-1] for ifl in iflines]

        (same_name, same_suffix, pair_num) = check_each_similar_pair(sufs, names, hash)
        total_same_name += same_name
        total_same_suffix += same_suffix
        total_pair_num += pair_num

        suffixset = {}
        for suf in sufs:
            if suf in suffixset:
                suffixset[suf] += 1
            else:
                suffixset[suf] = 1

        nameset = {}
        for name in names:
            if name in nameset:
                nameset[name] += 1
            else:
                nameset[name] = 1

        suffix_num += len(suffixset)
        name_num += len(nameset)
        if len(suffixset) > 1:
            print suffixset

    print >>sys.stderr, "%10s %10s %10s %10s" % ("Bins", "Files", "Name", "Suffix")
    print >>sys.stderr, "%10d %10d %10d %10d" % (bin_num, file_num, name_num, suffix_num)
    print >>sys.stderr, "Probability of same name and same type: %10f and  %10f in %d pairs" % (1.0*total_same_name/total_pair_num, 
            1.0*total_same_suffix/total_pair_num, total_pair_num)

def get_popular_types(trace):
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

if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "dcpt")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-d"]:
            get_file_size_distribution(trace)
        elif o in ["-c"]:
            get_file_size_coefficient(trace)
        elif o in ["-p"]:
            get_popular_types(trace)
        elif o in ["-t"]:
            check_name_and_type(trace)

    trace.close()
