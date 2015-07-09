# dan
Deduplication Analyzer

collector: go through the trace and build the chunk, file, region, and container databases

refs\_distrbution + refs\_classifier.py: go through the chunk/region/container database and print all reference numbers

distance\_analyzer: go through the chunk database and calc the distance between the first two references of a duplicate chunk

chunksize\_analyzer: go through the chunk database and calc the chunk size distribution, with different reference numbers

refs\_analyzer: go through the chunk database and analyze the relationship between reference and file

filesize\_analyzer: go through the chunk and file database to get file size distribution

locality: go through the trace and output all reference numbers in logical or physical sequence

correlation\_analyzer.py: With the output of locality\_sequence, calc the correlation coefficient of reference number
