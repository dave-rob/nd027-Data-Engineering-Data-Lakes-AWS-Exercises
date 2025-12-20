# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession.builder.appName("Wrangling Quiz").getOrCreate()

path = "../../data/sparkify_log_small.json"
df = spark.read.json(path)

df.printSchema()
# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# TODO: write your code to answer question 1
print("---------------------------------------------")
print("Question 1")
print("---------------------------------------------")
empty_user_df = df.select("page").where(df["userId"] == "").dropDuplicates()
all_pages_df = df.select("page").dropDuplicates()
print("The pages that the empty userId accessed:")
empty_user_df.show()
print("All the pages that can be accessed:")
all_pages_df.show()

print("-"*25)
print("The pages the empty user did not access:")
for row in set(all_pages_df.collect()) - set(empty_user_df.collect()):
    print(row.page)


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 


# TODO: use this space to explore the behavior of the user with an empty string
print("\n---------------------------------------------")
print("Question 2")
print("---------------------------------------------")
print("The empty user seems to be a user who isn't logged in or doesn't have an account. \nThis would be a user" \
    " who is accessing the home page and accessing pages generally accessible from the home page before being given access to the " \
    "other pages.")

# # Question 3
# 
# How many female users do we have in the data set?


# TODO: write your code to answer question 3
print("\n---------------------------------------------")
print("Question 3")
print("----------------------------------------------")

female_df = df.select(["gender", "userId"]).where(df["gender"] == 'F').dropDuplicates()
print(f"\nThere are {female_df.count()} female users.")

# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

print("\n---------------------------------------------")
print("Question 4")
print("----------------------------------------------")

print("\nThe artist with the most amount of different songs played:")
most_played_df = df.select("artist", "song").where(df["artist"] != "NULL").groupBy("Artist").agg({'Artist':'count'}).sort(desc('count(Artist)')).limit(1)
most_played_df.show()


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: write your code to answer question 5

print("\n---------------------------------------------")
print("Question 5")
print("----------------------------------------------")

windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)

is_home = udf(lambda h: int(h=="Home"), IntegerType())

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .where(df["userId"] != "") \
    .withColumn('homevisit', is_home(col('page'))) \
    .withColumn('period', Fsum('homevisit') \
    .over(windowval)) 

cusum.show(300)

cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}) \
    .show()
