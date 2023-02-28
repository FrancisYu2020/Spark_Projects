#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100) 

#TODO
def produce_pairs(line):
    src, tgts = line.strip().split(': ')
    tgts = tgts[:-1].split(' ') if tgts[-1] == '\n' else tgts.split(' ')
    ret = []
    for tgt in tgts:
        ret.append((tgt, (1, 0)))
    ret.append((src, (0, 1)))
    return ret

tgts = lines.flatMap(produce_pairs).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[1][0] == 0).sortByKey(lambda x: int(x))
# tgts = tgts.reduceByKey(lambda a, b: a + b)

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
for pair in tgts.collect():
    output.write(pair[0] + '\n')
output.close()
sc.stop()

