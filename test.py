 #spark-submit --master spark://10.10.10.11:7077 --total-executor-cores 9 --executor-memory 3G --packages ch.cern.sparkmeasure:spark-measure_2.11:0.15 ./testNovi.py


hdfsPath = "hdfs://10.10.10.11:9000//data//Grancarevo2019New.parquet"
inputVector='{"FDB-12": 3.597,"D-24": 0.004518,"D-22": 3.433e-07,"FDB-2": 3.597,"B-VI": 3.433e-07,"B-IV": 0.004518,"FDB-13": 3.597,"P1": 474.0,"B-II": 0.004518,"BR": 2.448e-13,"P6": 436.6,"DBA-1": 3.433e-07,"FDB-9": 3.597,"P4": 401.0,"ST-1": 2.139e-08,"P5": 194.4,"DA-18": 3.433e-07,"DA-19": 3.433e-07,"DBA-2": 3.433e-07,"DA-10": 3.433e-07,"DA-11": 3.433e-07,"DA-12": 3.433e-07,"DA-13": 3.433e-07,"DA-14": 3.433e-07,"DA-15": 3.433e-07,"DA-16": 3.433e-07,"B-III": 0.004518,"P2": 495.7,"D-13": 0.004518,"PB": 2.448e-13,"IZ": 1.896e-13,"FDB-10": 3.597,"D-17": 0.004518,"D-14": 3.433e-07,"D-15": 3.433e-07,"ST-2": 1.68e-07,"ST-3": 1.593e-06,"B-I": 3.433e-07,"DB-3": 0.004518,"FDB-1": 3.597,"FDB-14": 3.597,"DB-2": 0.004518,"B-V": 0.004518,"FDB-11": 3.597,"D-6": 3.433e-07,"D-7": 3.433e-07,"D-9": 3.433e-07,"P3": 356.2,"DB-1": 0.004518,"DA-8": 3.433e-07,"DA-9": 3.433e-07,"DB-5": 0.004518,"DB-4": 0.004518,"DB-6": 0.004518,"DA-3": 3.433e-07,"DA-1": 3.433e-07,"DA-7": 3.433e-07,"DA-4": 3.433e-07,"DA-5": 3.433e-07,"DA-21": 3.433e-07,"FDB-3": 3.597,"DA-22": 3.433e-07,"FDB-6": 3.597,"FDB-7": 3.597,"FDB-4": 3.597,"FDB-5": 3.597,"D-4": 0.004518,"FDB-8": 3.597,"DA-17": 3.433e-07}'
n=10
threshold=0.5
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("appName").getOrCreate()
from pyspark.sql import SQLContext
sc = spark.sparkContext
sqlContext = SQLContext(sc)
from sparkmeasure import StageMetrics
import pandas
stagemetrics = StageMetrics(spark)
#ucitavanje inputa
stagemetrics.begin()
df=spark.read.parquet(hdfsPath)
df.registerTempTable('grancarevo')
resultSet = sqlContext.sql("select GUID, inputs from grancarevo")
import json
inputDictionary = json.loads(inputVector)


def fieldFilter(row):
 return all(elem in row.keys()  for elem in inputDictionary.keys())
fieldNames=list(inputDictionary.keys())
minDict = dict()
maxDict = dict()



kolone=df.select("Inputs").take(1)[0][0].asDict().keys()
tabela = df.select("Inputs").take(df.count())
rdd1 = sc.parallelize(tabela)
row_rdd = rdd1.map(lambda x: x["Inputs"])

#odbacivanje onih koji nemaju odgovarajuca polja
df = sqlContext.createDataFrame(row_rdd, kolone).toPandas()
df=df[df.apply(lambda x: fieldFilter(x), axis=1)]

#minmax

minDict = spark.createDataFrame(df).toPandas().min().T
maxDict = spark.createDataFrame(df).toPandas().max().T

guidInputDict = resultSet.toPandas().set_index('GUID').T.to_dict('records')[0]
import sys
import math
distanceDict = dict()
guidInputDict.update( (k,json.dumps(v.asDict())) for k,v in guidInputDict.items())
guidInputDf = sc.parallelize(guidInputDict.items()).toDF(['GUID', 'input'])
#kalkulacija

def kalkulacija(input):	
	d = json.loads(input)
	distance = 0
	valid = 1
	for fieldName in d.keys():
		if not(fieldName in minDict):
			continue	 
		min = minDict[fieldName]
		max = maxDict[fieldName]
		range = max - min
		inputValue = inputDictionary[fieldName]
		databaseValue = d[fieldName]
		value=0
		if abs(range) > sys.float_info.min:
			value=(databaseValue - min)/(range) - (inputValue- min)/(range)
		if value > threshold:
			distance = sys.float_info.max
			valid = 0
			break
		distance += (value * value)	
	if valid==1:
		distance = math.sqrt(distance)
	return distance

from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
kalkulacija_udf = udf(kalkulacija, FloatType())
guidInputDf = guidInputDf.withColumn("distance", kalkulacija_udf(col("input")))



guidInputDf.drop("input").sort('distance').show()
stagemetrics.end()
f = open('/home/milos/analiza_cluster.txt', 'a+')
f.write(stagemetrics.report()+"\nREPORTEND\n")
