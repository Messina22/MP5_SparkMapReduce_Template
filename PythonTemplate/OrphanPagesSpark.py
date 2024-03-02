#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parse_links(line):
    parts = line.split(':')
    page_id = parts[0]
    linked_ids = parts[1].split()
    links = [(page_id, 0)]
    for linked_id in linked_ids:
        links.append((linked_id, 1))
    return links

# Flatten the list of links
links = lines.flatMap(parse_links)

# Reduce by key to sum the counts
reduced_counts = links.reduceByKey(lambda a, b: a + b)

# Filter out all pages that have at least one incoming link
orphan_pages = reduced_counts.filter(lambda x: x[1] == 0).sortByKey()
output = open(sys.argv[2], "w")
 
#write results to output file. Foramt for each line: (line + "\n")
for orphan in orphan_pages.collect():
    output.write(orphan[0] + "\n")

sc.stop()

