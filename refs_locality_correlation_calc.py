import sys
import getopt
from mpmath import *

def calcMeanAndDev(refs):
    sum = 0.
    sum_2 = 0.
    count = 0
    mean = 0.
    dev_2 = 0.
    for ref in refs:
        sum += ref 
        sum_2 += ref*ref

    mean = sum/len(refs)
    dev_2 = sum_2/len(refs) - mean*mean 

    print >> sys.stderr,  "mean =", mean, "dev^2 =", dev_2

    return mean, dev_2

def calcCorrelation(refs, mean, dev_2, lag):

    p = lag 
    correlation = 0.
    while True:
        correlation += (refs[p-lag] - mean)*(refs[p] - mean) 
        p += 1
        if p == len(refs):
            break

    correlation /= len(refs) 
    correlation /= dev_2

    return correlation

if __name__ == "__main__":

    (opts, args) = getopt.gnu_getopt(sys.argv[1:], "bl:", ["binary", "lag"])

    binary = False
    lag = 1
    for o, a in opts:
        if o in ["-b"]:
            binary = True
        elif o in ["-l"]:
            lag = int (a)

    file = open(args[0], "r")

    refs = [int(line) for line in file]

    file.close()
    
    if binary == True:
        print >> sys.stderr, "binary is True"
        i = 0
        while i<len(refs):
            if refs[i] > 1:
                refs[i] = 1
            else:
                refs[i] = 0
            i+=1


    (mean, dev_2) = calcMeanAndDev(refs)
    for i in range(lag+1)[1:]:
        print calcCorrelation(refs, mean, dev_2, i)
