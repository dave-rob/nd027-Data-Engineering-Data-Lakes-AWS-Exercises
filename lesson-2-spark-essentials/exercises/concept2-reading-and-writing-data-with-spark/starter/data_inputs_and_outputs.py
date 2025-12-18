import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

print(spark.sparkContext.getConf().getAll())

#import and read the json file into a df
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log_df = spark.read.json(path)

#print the schema of the dataframe
user_log_df.printSchema()

#print out columns of the df and the type in each column
print(user_log_df.describe())

#show top row
user_log_df.show(n=1)

#print the top 5  entries in a list
print(user_log_df.take(5))

#create the path to where the df will be exported into a csv file
out_path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.csv"

#write file to out_path
user_log_df.write.mode("overwrite").save(out_path, format="csv", header=True)

#all info should be the same as when imported from json file
user_log_df2 = spark.read.csv(out_path, header=True)

user_log_df2.printSchema()

print(user_log_df2.take(2))

user_log_df2.select("userID").show()

print(user_log_df2.take(1))