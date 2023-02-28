#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
def create_pairs(line):
    src, tgts = line.strip().split(': ')
    tgts = tgts[:-1].split(' ') if tgts[-1] == '\n' else tgts.split(' ')
    ret = []
    for tgt in tgts:
        if tgt != src:
            ret.append((tgt, 1))
    return ret

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
counts = lines.flatMap(create_pairs).reduceByKey(add).top(10, key=lambda x: x[1])
for k, v in sorted(counts):
    # k, v = pair
    output.write('%s\t%s\n' % (k, v))
output.close()
sc.stop()

