from pyspark import SparkContext, SparkConf
import initspark

conf = SparkConf().setAppName("Airports").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.textFile("data/airports/airports-semicolon.csv")
rdd\
    .map(lambda x: x.split(";"))\
    .take(2)

#Wie viele Airports gibt es insgesamt in Deutschland?
rdd\
    .map(lambda x: x.split(";"))\
    .filter(lambda x: x[8] == 'DE')\
    .count()

#Wie viele geschlossene Airports gibt es in Deutschland?
rdd\
    .map(lambda x: x.split(";"))\
    .filter(lambda x: x[8] == 'DE')\
    .filter(lambda x: x[2] == 'closed')\
    .count()

conf = SparkConf().setAppName("Airports2").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc\
    .textFile("data/airports/airports-semicolon.csv")\
    .map(lambda x: x.split(";"))\
    .cache()

#In welchem Land gibt es am meisten Airports?
rdd\
    .map(lambda x: (x[8],1))\
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x: (x[1],x[0]))\
    .sortByKey(ascending=False)\
    .take(1)