from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import *
import os


def Spark_Session():
    spark = SparkSession.builder.appName('assignment_1').getOrCreate()
    return spark


def read_user(spark):
    return spark.read.format("csv").options(header=True, inferSchema=True, sep=",") \
        .load(r"data\user.csv")


def read_transaction(spark):
    return spark.read.format("csv").options(header=True, inferSchema=True, sep=",") \
        .load(r"data\transaction.csv")


def new_column(transaction_df):
    return transaction_df.withColumnRenamed('userid', 'user_id')


def join_column(spark, user_df, new_transaction):
    return user_df.join(new_transaction, user_df.user_id == new_transaction.user_id, "inner") \
        .orderBy(user_df.user_id)
