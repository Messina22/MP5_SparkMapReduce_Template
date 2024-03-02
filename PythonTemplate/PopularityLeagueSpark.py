#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parse_links(line):
    parts = line.split(':')
    linked_ids = parts[1].split()
    return [(linked_id, 1) for linked_id in linked_ids]

link_counts = lines.flatMap(parse_links).reduceByKey(lambda a, b: a + b)

leagueIds = sc.textFile(sys.argv[2], 1)
ids = leagueIds.collect()
league_counts = link_counts.filter(lambda x: x[0] in ids)
counts = league_counts.values().collect()

ranked_links = league_counts.map(lambda x: (x[0], sum([1 for count in counts if count < x[1]]))).sortByKey()

output = open(sys.argv[3], "w")

for (link, rank) in ranked_links.collect():
    output.write(f'{link}\t{rank}\n')

sc.stop()

