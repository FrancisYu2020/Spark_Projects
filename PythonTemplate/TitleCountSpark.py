#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
	#TODO
    stopWords = set(f.read().split('\n'))

with open(delimitersPath) as f:
    #TODO
    temp_delimiters = f.read()
    # delimiters = f.read()
delimiters = '['
paren = set(['(',')', '[', ']', '{', '}', '.'])
for char in temp_delimiters:
    delimiters += f'\{char}'
    # delimiters += f'\{char}' if char in paren else char
delimiters += '\n]'
conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO
outputFile = open(sys.argv[4],"w",encoding='utf-8')
counts = lines.flatMap(lambda line: re.sub(delimiters, ' ', line.strip().lower()).split(' ')).filter(lambda x: x != '' and x not in stopWords).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0])).top(10)
# for delimiter in delimiters:
#     counts = counts.map(lambda line: line.replace(delimiter, ' '))
# for l in (counts.collect()[:10]):
#     outputFile.write(l + '\n')
# counts = counts.flatMap(lambda line: line.split(' ')).filter(lambda x: x != '' and x not in stopWords)
# counts = counts.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0])).top(5)
# counts = lines.flatMap(lambda line: re.sub(delimiters, ' ', line.strip().lower()).split(' ')).filter(lambda x: x != '' and x not in stopWords).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0])).top(5)


# #TODO
# #write results to output file. Foramt for each line: (line +"\n")
# print((counts.collect()[0]))
# outputFile.write(delimiters)
for pair in sorted(counts, key=lambda x: x[1]):
    outputFile.write(pair[1] + '\t' + str(pair[0]) + '\n')
outputFile.close()
sc.stop()
