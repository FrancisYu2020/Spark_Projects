#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
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

counts = lines.flatMap(create_pairs).reduceByKey(add)

leagueIds = sc.textFile(sys.argv[2], 1)

#TODO
leagues = leagueIds.flatMap(lambda line: line.strip().split('\n')).collect()
counts = counts.filter(lambda x: x[0] in leagues).sortBy(lambda x: -x[1]).collect()
output = open(sys.argv[3], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
for i in range(len(counts)):
    # print(counts.collect())
    counts[i] = (counts[i][0], len(counts) - i - 1)
for k, v in sorted(counts):
    output.write('%s\t%s\n' % (k, v))
output.close()
sc.stop()

