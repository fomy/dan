import sys
import getopt

# This script is to calculate the distance between two chunks of similar reference number

if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "l:r:")

    lb = 5
    rb = sys.maxint
    for o, a in opts:
        if o in ["-l"]:
            lb = int(a)
        elif o in ["-r"]:
            rb = int(a)

    file = open(args[0], "r")

    cur = -1
    prev = -1

    while True:
        line = file.readline()
        if line == '':
            break
        ref = int(line)
        cur += 1
        if ref >= lb and ref <= rb:
            prev = cur
            break

    print >> sys.stderr, "first = %d" %cur
    sum = 0
    count = 0
    while True:
        line = file.readline()
        if line == '':
            break
        ref = int(line)
        cur += 1
        if ref >= lb and ref <= rb:
            dist = cur - prev
            print dist
            prev = cur

            count += 1
            sum += dist

    file.close()
    print >> sys.stderr, "last = %d, count = %d" % (prev, count)
    print >> sys.stderr, "avg. = %10.2f" % (1.0*sum/count)
    

