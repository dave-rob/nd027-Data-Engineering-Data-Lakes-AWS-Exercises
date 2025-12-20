#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with Spark SQL Quiz
# 
# This code uses the same dataset and most of the same questions from the earlier code using dataframes. For this scropt, however, use Spark SQL instead of Spark Data Frames.


from pyspark.sql import SparkSession

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) create a view to use with your SQL queries
# 5) write code to answer the quiz questions 

spark = SparkSession.builder.appName("Wrangling Quiz with Spark SQL").getOrCreate()

path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small_2.json"
df = spark.read.json(path)

df.printSchema()

df.createOrReplaceTempView("user_log_table")
# # Question 1
# 
# Which page did user id ""(empty string) NOT visit?

# TODO: write your code to answer question 1
print("---------------------------------------------")
print("Question 1")
print("---------------------------------------------")

empty_user_query = '''
          SELECT DISTINCT page AS empty_pages
          FROM user_log_table
          WHERE userId == ''
          '''

all_pages_query = '''
                         SELECT DISTINCT page
                         FROM user_log_table
                         '''

empty_user_df = spark.sql(empty_user_query).show()

all_pages_df = spark.sql(all_pages_query).show()

not_visited_df = spark.sql(f'''
                           SELECT all_pages.page
                            FROM ({all_pages_query}) all_pages
                            LEFT JOIN ({empty_user_query}) empty_pages
                            ON all_pages.page = empty_pages.empty_pages
                            WHERE empty_pages.empty_pages IS NULL
                           ''').show()

# # Question 2 - Reflect
# 
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?

# # Question 3
# 
# How many female users do we have in the data set?

# TODO: write your code to answer question 3
print("\n---------------------------------------------")
print("Question 3")
print("----------------------------------------------")

female_user_query = '''
                                SELECT COUNT(DISTINCT userId) AS Female_Count
                                FROM user_log_table
                                WHERE gender == 'F'
                                  '''

female_query_count = spark.sql(female_user_query).show()


# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4
print("\n---------------------------------------------")
print("Question 4")
print("----------------------------------------------")

most_played_query = '''
                                SELECT artist, COUNT(artist) as songs_played
                                FROM user_log_table
                                WHERE artist != "NULL"
                                GROUP BY artist
                                ORDER BY songs_played DESC
                                LIMIT 1                           
'''

spark.sql(most_played_query).show()

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.

# TODO: write your code to answer question 5

print("\n---------------------------------------------")
print("Question 5")
print("----------------------------------------------")

query = """
    WITH cusum AS (
        SELECT userID, page, ts,
            CASE WHEN page = 'Home' THEN 1 ELSE 0 END AS homevisit,
            SUM(CASE WHEN page = 'Home' THEN 1 ELSE 0 END) OVER (
                PARTITION BY userID
                ORDER BY ts DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS period
        FROM user_log_table
        WHERE page IN ('NextSong', 'Home') AND userID != ""
    )
    SELECT AVG(song_count) AS average_songs
    FROM (
        SELECT userID, period, COUNT(period) AS song_count
        FROM cusum
        WHERE page = 'NextSong'
        GROUP BY userID, period
    )
"""

spark.sql(query).show()
