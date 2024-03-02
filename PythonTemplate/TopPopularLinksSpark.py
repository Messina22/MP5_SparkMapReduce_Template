#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parse_links(line):
    parts = line.split(':')
    linked_ids = parts[1].split()
    links = []
    for linked_id in linked_ids:
        links.append((linked_id, 1))
    return links

# Flatten the list of links
links = lines.flatMap(parse_links)

# Reduce by key to sum the counts
reduced_counts = links.reduceByKey(lambda a, b: a + b)

topLinks = reduced_counts.takeOrdered(10, key=lambda x: -x[1])
topLinks = sorted(topLinks, key=lambda x: x[0])

output = open(sys.argv[2], "w")
 
#write results to output file. Foramt for each line: (line + "\n")
for (link, count) in topLinks:
    output.write(f'{link}\t{count}\n')

sc.stop()

