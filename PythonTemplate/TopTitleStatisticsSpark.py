#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)
wordCounts = lines.map(lambda line: line.split('\t'))
counts = wordCounts.map(lambda x: int(x[1]))
mean = counts.mean()
sum = counts.sum()
min = counts.min()
max = counts.max()
var = counts.variance()

outputFile = open(sys.argv[2], "w")

outputFile.write('Mean\t%s\n' % int(mean))
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % min)
outputFile.write('Max\t%s\n' % max)
outputFile.write('Var\t%s\n' % int(var))

sc.stop()

