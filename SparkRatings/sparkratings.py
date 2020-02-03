from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("SparkRatings")\
        .getOrCreate()
sc = spark.sparkContext

# ratings = spark.read.csv("hdfs:///bigdata/books/BX-Book-Ratings_sample.csv", header=True)
# users = spark.read.csv("hdfs:///bigdata/books/BX-Users_sample.csv", header=True)

ratings = spark.read.csv("hdfs:///bigdata/books/BX-Book-Ratings.csv", header=True)
users = spark.read.csv("hdfs:///bigdata/books/BX-Users.csv", header=True)

ratings.show()
users.show()