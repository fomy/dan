import sys
import getopt
import itertools

def check_one_file(one, others):
    for other in others:
        if one == other:
            return True

    return False

def check_identical_file_names(trace):
    logical_files = 0
    physical_files = 0
    total_names = 0
    total_suffix = 0
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

    print >>sys.stderr, logical_files, physical_files, total_names, total_suffix

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

    print top_suffix

    top_suffix = []
    for suf in suffixset:
        top_suffix.append((suffixset[suf][1], suf))
        top_suffix = sorted(top_suffix, reverse=True)
        if(len(top_suffix) > 10):
            top_suffix.pop()

    print top_suffix

# find all identical files
# -n check names
# -s check suffix
# -o output all suffix
if __name__ == "__main__":

    (opts, args) = getopt.getopt(sys.argv[1:], "no")

    task = "check names"
    for o, a in opts:
        if o in ["-n"]:
            task = "check names"
        elif o in ["-o"]:
            task = "popular suffix"

    print task
    trace = open(args[0], "r")

    if task == "check names":
        check_identical_file_names(trace)
    else:
        get_popular_types(trace)

    trace.close()
