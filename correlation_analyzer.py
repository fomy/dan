import sys
import getopt
from mpmath import *

def calcMeanAndDev(lines):
    sum = 0.
    sum_2 = 0.
    count = 0
    mean = 0.
    dev_2 = 0.
    for line in lines:
        sum += line
        sum_2 += line*line 

    mean = sum/len(lines)
    dev_2 = sum_2/len(lines) - mean*mean 

    print >> sys.stderr,  "mean =", mean, "dev^2 =", dev_2

    return mean, dev_2

def calcCorrelation(lines, mean, dev_2, lag):

    p = lag 
    correlation = 0.
    while True:
        #print (lines[p-lag] - mean)*(lines[p] - mean)
        correlation += (lines[p-lag] - mean)*(lines[p] - mean) 
        p += 1
        if p == len(lines):
            break

    correlation /= len(lines) 
    correlation /= dev_2

    return correlation

if __name__ == "__main__":

    (opts, args) = getopt.getopt(sys.argv[1:], "l:", ["lag"])

    # lag
    lag = 1
    for o, a in opts:
        if o in ["-l"]:
            lag = int(a)
    #print >> sys.stderr, "lag =", lag

    file = open(args[0], "r")

    lines = [int(line) for line in file]

    file.close()
    
    (mean, dev_2) = calcMeanAndDev(lines)
    for i in range(1000):
        print calcCorrelation(lines, mean, dev_2, i)
