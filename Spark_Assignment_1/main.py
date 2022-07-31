from pyspark import *
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession
from utility import *

spark = Spark_Session()

user_df = read_user(spark)
user_df.show()

transaction_df = read_transaction(spark)
transaction_df.show()

new_transaction = new_column(transaction_df)
new_transaction.show()

# combine_data = new_transaction_column.join(df_user, on=['user_id'], how='inner')

Join_data = join_column(spark, user_df, new_transaction)
Join_data.show()

# Count of unique locations where each product is sold:-

Unique_location = Join_data.select("location ").distinct().show(40, False)
print("Count of Unique Locations where each Product is sold: " + str(Join_data.select("location ",).distinct().count()))

# Find out products bought by each user:-
products_bought = Join_data.select("product_description").show(40, False)

# Total spending done by each user on each product:-

Total_spending = Join_data.groupBy("product_description").sum("price").show(40, False)

