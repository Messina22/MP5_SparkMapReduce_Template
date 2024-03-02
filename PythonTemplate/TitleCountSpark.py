#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stop_words = set(f.read().splitlines())

with open(delimitersPath) as f:
    delimiters = f.read().strip()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

def tokenize(title, delimiters):
    title = title.strip().lower()
    
    for delimiter in delimiters:
        title = title.replace(delimiter, ' ')
    
    tokens = title.split()
    return [token for token in tokens if token]

tokens = lines.flatMap(lambda line: tokenize(line, delimiters)).filter(lambda token: token not in stop_words)

wordCounts = tokens.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

topWords = wordCounts.takeOrdered(10, key=lambda x: -x[1])
topWords = sorted(topWords, key=lambda x: x[0])
            
outputFile = open(sys.argv[4],"w")

for (word, count) in topWords:
    outputFile.write(f"{word}\t{count}\n")

sc.stop()
