import sys
import getopt
import itertools

if __name__ == "__main__":

    (opts, args) = getopt.getopt(sys.argv[1:], "l:r:s:")

    lb = 5
    rb = sys.maxint
    segment_size = 512
    for o, a in opts:
        if o in ["-l"]:
            lb = int(a)
        elif o in ["-r"]:
            rb = int(a)
        elif o in ["-s"]:
            segment_size = int(a)

    file = open(args[0], "r")

    while True:
        segment = list(itertools.islice(file, segment_size))
        if len(segment) == 0:
            break
        count = 0
        for item in segment:
            ref = int(item)
            if ref >= lb and ref <= rb:
                count += 1
        print count

    file.close()


