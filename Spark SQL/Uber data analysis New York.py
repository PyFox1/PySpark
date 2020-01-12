#Data from: https://www.kaggle.com/fivethirtyeight/uber-pickups-in-new-york-city
#Trip data for over 20 million Uber (and other for-hire vehicle) trips in NYC

import pyspark
import initspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import pyspark.sql.functions as F

%matplotlib inline
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("TaxiData").getOrCreate()

schema = StructType([
    StructField("date", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("base", StringType())
])

csv = spark.read.option("header", "true").csv("./data/uber/*.csv", schema = schema).cache()

f = udf(lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M:%S"), TimestampType())
data = csv.withColumn("date", f(csv.date))
data.show()

#In which hour do most Uber rides take place?
data.groupBy(F.date_format(data.date, "H")).count()

dataPerHour = data\
    .groupBy(F.date_format(data.date, "H"))\
    .count()\
    .orderBy("date_format(date, H)")\
    .cache()

dataPerHour\
    .withColumn("hour", dataPerHour["date_format(date, H)"].cast(IntegerType()))\
    .orderBy("hour")\
    .show()

dataPerHourFetched = dataPerHour\
    .withColumn("hour", dataPerHour["date_format(date, H)"].cast(IntegerType()))\
    .orderBy("hour")\
    .collect()

x = []
y = []
for row in dataPerHourFetched:
    x.append(row["date_format(date, H)"])
    y.append(row["count"])

plt.plot(x, y, 'bo')
plt.show()

#At which day of the week do most Uber rides take place?
dataPerDayOfWeek = data\
    .groupBy(F.date_format(data.date, "u"))\
    .count()\
    .orderBy("date_format(date, u)")\
    .cache()

dataPerDayOfWeek\
    .withColumn("weekday", dataPerDayOfWeek["date_format(date, u)"].cast(IntegerType()))\
    .orderBy("weekday")\
    .show()

dataPerDayOfWeekFetched = dataPerDayOfWeek\
    .withColumn("weekday", dataPerDayOfWeek["date_format(date, u)"].cast(IntegerType()))\
    .orderBy("weekday")\
    .collect()

x = []
y = []
for row in dataPerDayOfWeekFetched:
    x.append(row.weekday)
    y.append(row["count"])

plt.plot(x, y, 'bo')
plt.show()