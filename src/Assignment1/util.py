import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, collect_list, sum

# Creating a SparkSession
def merge_dataframes(users_df, transactions_df):
    #Merge the user and transaction dataframes
    total_df = users_df.join(transactions_df, users_df.user_id == transactions_df.userid)
    return total_df
#Count of unique locations where each product is sold
def count_unique_locations(total_df):
    #Count of unique locations where each product is sold
    product_locations_df = total_df.groupBy("product_id").agg(countDistinct("location ").alias("UniqueLocations"))
    return product_locations_df
#Find out products bought by each user
def find_products_bought(total_df):
    user_products_df = total_df.groupBy("userid").agg(collect_list("product_id").alias("ProductsBought"))
    return user_products_df
 #Total spending done by each user on each product
def calculate_total_spending(total_df):
    user_product_spending_df = total_df.groupBy("userid", "product_id").agg(sum("price").alias("TotalSpending"))
    return user_product_spending_df
    
    


