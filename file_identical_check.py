import sys
import getopt
import itertools

def check_one_file(one, others):
    for other in others:
        if one == other:
            return True

    return False


def check_identical_files(files):
    same_files = 0
    total_files = 0
    diff = []
    while True:
        if check_one_file(files[0], files[1:]):
            same_files += 1
        else:
            diff.append(files[0])

        total_files += 1
        files.remove(files[0])
        if len(files) == 1:
            break
    
    if len(diff) > 0:
        diff.append(files[0])

    return total_files, same_files, diff


# find all identical files
# -n check names
# -s check suffix
# -o output all suffix
if __name__ == "__main__":

    col = -1
    file = open(sys.argv[1], "r")

    total_names = 0;
    same_names = 0;
    while True:
        line = list(itertools.islice(file, 1))
        if len(line) == 0:
            break;
        fnum = line[0].split()[1]
        iflines = list(itertools.islice(file, int(fnum)))

        (t,s,d) = check_identical_files([ifl.split()[col] for ifl in iflines])

        if t != s:
            #print [ifl.split()[col] for ifl in iflines]
            print d
        else:
            assert(len(d) == 0)

        total_names += t
        same_names += s

    print >>sys.stderr, total_names, same_names
