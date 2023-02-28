#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

#TODO
data = lines.map(lambda line: line.strip().split('\t')).map(lambda line: int(line[1]))
print(data.collect())
print(type(data))
mean = data.mean()
min_count = data.min()
max_count = data.max()
total = data.sum()
var = data.variance()

outputFile = open(sys.argv[2], "w", encoding='utf-8')
'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''
outputFile.write('Mean\t%d\n' % mean)
outputFile.write('Sum\t%d\n' % total)
outputFile.write('Min\t%d\n' % min_count)
outputFile.write('Max\t%d\n' % max_count)
outputFile.write('Var\t%d\n' % var)
outputFile.close()
sc.stop()

