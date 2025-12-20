# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession.builder.appName("Wrangling Quiz").getOrCreate()

path = "../../data/sparkify_log_small.json"
df = spark.read.json(path)

# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# TODO: write your code to answer question 1
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


# # Question 3
# 
# How many female users do we have in the data set?


# TODO: write your code to answer question 3


# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: write your code to answer question 5

