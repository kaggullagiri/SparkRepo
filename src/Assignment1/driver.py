import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Assignment1").getOrCreate()

# Read users and transactions data
users_df = spark.read.csv("C:/Users/KOLA PRABHU&PARVATHI/PycharmProjects/SparkRepo/resource/user.csv", header=True, inferSchema=True)
transactions_df = spark.read.csv("C:/Users/KOLA PRABHU&PARVATHI/PycharmProjects/SparkRepo/resource/transaction.csv", header=True, inferSchema=True)

total_df=users_df.join(transactions_df,users_df.user_id==transactions_df.userid,"inner")
# here merging two tables

#Count of unique locations where each product is sold
Uniquelocations_df=count_unique_locations(total_df)
#Find out products bought by each user
Productsbought_df=find_products_bought(total_df)
#Total spending done by each user on each product
Totalspending_df=calculate_total_spending(total_df)

# displaying the particular results
Uniquelocations_df.show()
Productsbought_df.show()
Totalspending_df.show()
