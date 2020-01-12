#Data from: https://www.kaggle.com/kaggle/us-baby-names
#Explore naming trends from babies born in the US

from pyspark.sql import SparkSession
import initspark

#Kommt der Name Lucia in Kalifornien häufiger vor, als im Gesamtdurchschnitt der USA?

spark = SparkSession \
    .builder \
    .appName("PythonSpark") \
    .getOrCreate()

from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("state", StringType(), True),
    StructField("count", IntegerType(), True),
])

data = spark.read\
    .option("header", True)\
    .csv("data/names/StateNames.csv", schema = schema)\
    .cache()

data.show()

#Wie oft kommt der Name Lucia in Kalifornien vor?
CA_Lucia = data\
    .where(data.state=="CA")\
    .where(data.name=="Lucia")\
    .agg({"count": "sum"})\
    .collect()

CA_Lucia_num =CA_Lucia[0]["sum(count)"]
print(CA_Lucia_num)

#Anzahl Babys, die in Kalifornien geboren sind
CA_ALL = data\
    .where(data.state=="CA")\
    .agg({"count": "sum"})\
    .collect()

CA_ALL_num = CA_ALL[0]["sum(count)"]
print(CA_ALL_num)

#Wie oft kommt der Name Lucia in den gesamten USA vor?
USA_Lucia = data\
    .where(data.name=="Lucia")\
    .agg({"count": "sum"})\
    .collect()

USA_Lucia_num = USA_Lucia[0][0]
print(USA_Lucia_num)

#Anzahl Babys, die in den USA geboren sind?
USA_ALL = data\
    .agg({"count": "sum"})\
    .collect()

USA_ALL_num = USA_ALL[0][0]
print(USA_ALL_num)

Quote_CA = CA_Lucia_num/CA_ALL_num
Quote_USA = USA_Lucia_num/USA_ALL_num
print(Quote_CA)
print(Quote_USA)