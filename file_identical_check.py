#!/usr/bin/python
import sys
import getopt
import itertools

def get_file_refs_distribution(trace):

    total_refs = 0
    total_files = 0
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        iflines = list(itertools.islice(trace, fnum))
        print fnum
        total_files += 1
        total_refs += fnum
    print >>sys.stderr, "mean = %8.5f" % (1.0*total_refs/total_files)

def get_file_size(trace):
    mean = 0;
    count = 0
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        iflines = list(itertools.islice(trace, fnum))

        for line in iflines:
            size = int(line.split()[2])
            mean += size
            count += 1
            print size 

    print >>sys.stderr, "mean = %.2f" % (1.0*mean/count)

def check_each_identical_pair(sufs, names):
    same_name = 0
    same_suffix = 0
    pair_num = 0
    for i in range(len(sufs)):
        for j in range(len(sufs)):
            if i == j:
                continue
            pair_num += 1
            if(sufs[i] == sufs[j]):
                same_suffix += 1
            if(names[i] == names[j]):
                same_name += 1
    return (same_name, same_suffix, pair_num)

def check_name_and_type(trace):
    logical_files = 0
    physical_files = 0
    total_names = 0
    total_suffix = 0

    total_same_name = 0
    total_same_suffix = 0
    total_pair_num = 0

    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        physical_files += 1
        logical_files += fnum
        iflines = list(itertools.islice(trace, fnum))

        names = [ifl.split()[3] for ifl in iflines]
        sufs = [ifl.split()[-1] for ifl in iflines]

        (same_name, same_suffix, pair_num) = check_each_identical_pair(sufs, names)
        total_same_name += same_name
        total_same_suffix += same_suffix
        total_pair_num += pair_num

        nameset = {}
        for name in names:
            if name in nameset:
                nameset[name] += 1
            else:
                nameset[name] = 1

        suffixset = {}
        for suf in sufs:
            if suf in suffixset:
                suffixset[suf] += 1
            else:
                suffixset[suf] = 1

        total_names += len(nameset)
        total_suffix += len(suffixset)

    print >>sys.stderr, "%10s %10s %10s %10s" % ("Logical", "Physical", "Names", "Suffix")
    print >>sys.stderr, "%10d %10d %10d %10d" % (logical_files, physical_files, total_names, total_suffix)
    print >>sys.stderr, "Probability of same name and same type: %10f and  %10f in %d pairs" % (1.0*total_same_name/total_pair_num, 
            1.0*total_same_suffix/total_pair_num, total_pair_num)

def get_popular_types(trace):
    # {suffix : [num, size]}
    suffixset = {}
    while True:
        line = list(itertools.islice(trace, 1))
        if len(line) == 0:
            break;
        fnum = int(line[0].split()[1])
        iflines = list(itertools.islice(trace, fnum))

        files = [ifl.split() for ifl in iflines]
        for file in files: 
            if file[-1] in suffixset:
                suffixset[file[-1]][0] += 1 
                suffixset[file[-1]][1] += int(file[2])
            else:
                suffixset[file[-1]] = [1, int(file[2])]

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

# find all identical files
# -n check names
# -s check file size distribution 
# -p output popular suffix
if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "npsr")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-n"]:
            check_name_and_type(trace)
        elif o in ["-p"]:
            get_popular_types(trace)
        elif o in ["-s"]:
            get_file_size(trace)
        elif o in ["-r"]:
            get_file_refs_distribution(trace)

    trace.close()
