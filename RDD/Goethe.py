#What's the most frequently used word in Geothe's book Faust?

from pyspark import SparkContext, SparkConf
import initspark

conf = SparkConf().setAppName("GoetheFaust").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.textFile("data/goethe/faust.txt").cache()

rdd.take(10)

rdd\
    .flatMap(lambda x: x.split(" "))\
    .take(5)

rdd\
    .flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x, 1))\
    .take(5)

rdd\
    .flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y)\
    .take(5)

rdd\
    .flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y)\
    .map(lambda x: (x[1], x[0]))\
    .take(5)

rdd\
    .flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y)\
    .map(lambda x: (x[1], x[0]))\
    .sortByKey(ascending = False)\
    .filter(lambda x: x[1] != "")\
    .take(5)