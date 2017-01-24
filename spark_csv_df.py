from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import OrderedDict

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "SparkCsvDf")
ssc = StreamingContext(sc, 1)
schemaPath = "./schema.csv"
dataPath = "./data/"

spark = SparkSession \
    .builder \
    .appName("SparkCsvDf") \
    .getOrCreate()

schemaFile = sc.textFile(schemaPath)
header = schemaFile.first()
schemaString = header.replace('"','')

fields = OrderedDict()
for field_name in schemaString.split(','):
    fields[field_name] = StructField(field_name, StringType(), True)

fields["Year"].dataType = IntegerType()
fields["Quarter"].dataType = IntegerType()
fields["Month"].dataType = IntegerType()
fields["DayofMonth"].dataType = IntegerType()
fields["DepDelay"].dataType = FloatType()
fields["DepDelayMinutes"].dataType = FloatType()

schema = StructType(map(lambda x: x[1], fields.items()))

csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .option("header", "True") \
    .schema(schema) \
    .csv(dataPath)  # Equivalent to format("csv").load("/path/to/directory")

csvDF.createOrReplaceTempView("flights")

sqlDF = spark.sql("SELECT avg(DepDelay) FROM flights")
query = sqlDF.writeStream.outputMode("complete").format('console').start()
query.awaitTermination()
