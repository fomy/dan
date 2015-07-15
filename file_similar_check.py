#!/usr/bin/python
import sys
import getopt
import itertools
import math

#def get_file_size_coefficient(trace):
    #co = 0
    #co_count = 0
    #while True:
        #line = list(itertools.islice(trace, 1))
        #if len(line) == 0:
            #break;
        #fnum = int(line[0].split()[1])
        #assert(fnum > 1)
        #sflines = list(itertools.islice(trace, fnum))

        #m = 0
        #m_2 = 0
        #c = 0
        #for line in sflines:
            #size = int(line.split()[2])
            #m += size
            #m_2 += size * size
            #c += 1

        #m = 1.0*m/c
        #m_2 = 1.0*m_2/c
        #d_2 = m_2 - m*m
        #d = math.sqrt(d_2)
        #print "%8.5f" % (d/m)
        #co += d/m
        #co_count+=1

    #print >>sys.stderr, "mean = %8.5f" % (1.0*co/co_count)

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

def check_each_similar_pair(sufs, names, hashes, sizes):

    same_name = 0
    same_suffix = 0
    pair_num = 0
    coefficient = 0

    diff_pairs = []
    # This is also correct
    # But less efficient than check_each_pair in file_distinct_check.py
    for i in range(len(sufs)):
        for j in range(i+1, len(sufs)):
            if hashes[i] == hashes[j]:
                # skip identical files
                continue
            pair_num += 1
            if sufs[i] == sufs[j]:
                same_suffix += 1
            elif sufs[i] < sufs[j]:
                diff_pairs.append((sufs[i], sufs[j]))
            else:
                diff_pairs.append((sufs[j], sufs[i]))
            if(names[i] == names[j]):
                same_name += 1

            f1size = int(sizes[i])
            f2size = int(sizes[j])
            mean = (f1size + f2size)/2.0
            dev = ((f1size - mean)*(f1size - mean) + (f2size - mean)*(f2size - mean))/2.0
            coefficient += math.sqrt(dev)/mean

    return (same_name, same_suffix, pair_num, diff_pairs, coefficient)


# Are similar files with identical suffixes
def check_name_and_type(trace):
    bin_num = 0
    file_num = 0
    suffix_num = 0
    name_num = 0

    total_same_name = 0
    total_same_suffix = 0
    total_pair_num = 0
    coefficient = 0

    total_diff_pairs = {}
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
        hashes = [ifl.split()[-1] for ifl in iflines]
        sizes = [ifl.split()[2] for ifl in iflines]

        # check each pair of similar (not identical) files
        (same_name, same_suffix, pair_num, diff_pairs, ce) = check_each_similar_pair(sufs, names, hashes, sizes)
        total_same_name += same_name
        total_same_suffix += same_suffix
        total_pair_num += pair_num
        coefficient += ce

        for pair in diff_pairs:
            if pair in total_diff_pairs:
                total_diff_pairs[pair] += 1
            else:
                total_diff_pairs[pair] = 1

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
        #if len(suffixset) > 1:
            #print suffixset

    top_diff = []
    for diff in total_diff_pairs:
        top_diff.append((total_diff_pairs[diff], diff))
        top_diff = sorted(top_diff, reverse=True)
        if(len(top_diff) > 10):
            top_diff.pop()

    print top_diff
    coefficient /= total_pair_num

    print >>sys.stderr, "%10s %10s %10s %10s" % ("Bins", "Files", "Name", "Suffix")
    print >>sys.stderr, "%10d %10d %10d %10d" % (bin_num, file_num, name_num, suffix_num)
    print >>sys.stderr, "Probability of same name and same type: %10f and  %10f in %d pairs" % (1.0*total_same_name/total_pair_num, 
            1.0*total_same_suffix/total_pair_num, total_pair_num)
    print >>sys.stderr, "File size coefficient is %10f" % coefficient

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

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "dpt")

    trace = open(args[0], "r")
    for o, a in opts:
        if o in ["-d"]:
            get_file_size_distribution(trace)
        elif o in ["-p"]:
            get_popular_types(trace)
        elif o in ["-t"]:
            check_name_and_type(trace)

    trace.close()
