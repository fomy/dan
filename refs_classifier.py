#!/usr/bin/python
import sys
import getopt

def cluster3(filename):
    # Low, Mid, High
    # (1,2], (2,4], (4,infi)
    count = [0, 0, 0]

    file_object = open(filename, "r")
    try:
        for line in file_object:
            ref = float(line)
            if ref > 4:
                count[2] += 1
            elif ref > 2:
                count[1] += 1
            elif ref > 1:
                count[0] += 1

    finally:
        file_object.close()

    return count

def cluster5(filename):
    # 1, 2, [3,4], [5,6,7], [8,infi)
    count = [0, 0, 0, 0, 0]

    max = 1
    file_object = open(filename, "r")
    try:
        for line in file_object:
            ref = float(line)
            if ref > 7:
                count[4] += 1
            elif ref > 4:
                count[3] += 1
            elif ref > 2:
                count[2] += 1
            elif ref == 2:
                count[1] += 1
            elif ref == 1:
                count[0] += 1
            
            if ref > max:
                max = ref

        print max
    finally:
        file_object.close()

    return count

def cluster10(filename):
    # (1,2], (2,3], (3,4], (4,5], (5,6], (6,7], (7,8], (8,9], (9,10], (10, infi)
    count = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    file_object = open(filename, "r")
    try:
        for line in file_object:
            ref = float(line)
            if ref > 10:
                count[10] += 1
            elif ref > 9:
                count[9] += 1
            elif ref > 8:
                count[8] += 1
            elif ref > 7:
                count[7] += 1
            elif ref > 6:
                count[6] += 1
            elif ref > 5:
                count[5] += 1
            elif ref > 4:
                count[4] += 1
            elif ref > 3:
                count[3] += 1
            elif ref > 2:
                count[2] += 1
            elif ref > 1:
                count[1] += 1
            elif ref > 0:
                count[0] += 1

    finally:
        file_object.close()

    return count

if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "c:", ["cluster"])

    c = 5
    for o, a in opts:
        if o in ["-c"]:
            c = int(a)

    if c == 10:
        count = cluster10(args[0])
    elif c == 5:
        count = cluster5(args[0])
    else:
        count = cluster3(args[0])

    s = sum(count)
    for i in range(len(count)):
        print 1.0 * count[i]/s
