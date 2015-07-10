# dan
Deduplication Analyzer

Data Collector
--------------
*collector*: go through the trace and build the chunk, file, region, and container databases

Reference Analyzer
------------------
1. *refs\_distrbution\_exporter*: go through the chunk/region/container database and print all reference numbers
2. *refs\_classifier.py*: classify chunk/region/container according to reference numbers
3. *refs_locality_exporter*: go through the trace and output all reference numbers in logical or physical locality
4. *refs_locality_analyzer.py*: with the output of locality\_sequence, calc the correlation coefficient of reference number

Chunk Analyzer
--------------
1. *chunk_refs_distance_analyzer*: go through the chunk database and calc the distance between chunk references 
2. *chunk_size_analyzer*: go through the chunk database and calc the chunk size distribution, with different reference numbers

File Analyzer
-------------
1. *file_size\_analyzer*: go through the chunk and file database to get file size distribution
2. *file_type\_analyzer*: go through the chunk and file database to get the fractions of popular file types
3. *file_refs_source_analyzer*: go through the chunk database and analyze the relationship between reference and file
