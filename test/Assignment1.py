import unittest
from SparkRepo.src.Assignment1.util import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *



class MyTestCase(unittest.TestCase):
    spark = SparkSession.builder.appName('Assignment1').getOrCreate()
    def test_Assignment1(self):
        # actual dataframe schema for users
        user_schema = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location ', StringType(), True)
        ])
        # actual data for users
        user_data = [(101, "abc.123@gmail.com", "hindi", "mumbai"),
                     (102, "jhon@gmail.com", "english", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88@outlook.com", "tamil", "chennai"),
                     (105, "sahil.55@gmail.com", "english", "usa")
                     ]
        # creating to dataframes
        user_df = self.spark.createDataFrame(data=user_data, schema=user_schema)
        # actual dataframe schema for transaction
        transaction_schema = StructType([
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])
        ##actual dataframe data for transaction
        transaction_data = [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge"),
                            (3300105, 1000005, 105, 55000, "sofa")
                            ]
        transact_df = self.spark.createDataFrame(data=transaction_data, schema=transaction_schema)
        # Expected dataframe schema
        expected_schema = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True),
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])
        # Expected data
        expected_data = [(101, "abc.123@gmail.com", "hindi", "mumbai", 3300104, 1000004, 101, 35000, "fridge"),
                         (101, "abc.123@gmail.com", "hindi", "mumbai", 3300101, 1000001, 101, 700, "mouse"),
                         (102, "jhon@gmail.com", "english", "usa", 3300102, 1000002, 102, 900, "keyboard"),
                         (103, "madan.44@gmail.com", "marathi", "nagpur", 3300103, 1000003, 103, 34000, "tv"),
                         (105, "sahil.55@gmail.com", "english", "usa", 3300105, 1000005, 105, 55000, "sofa")
                         ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        transformdata_df = merge(user_df, transact_df)
        self.assertEqual(sorted(transformdata_df.collect()), sorted(expected_df.collect()))


        # unique location testcase
        exp_location_schema = StructType([
            StructField('location ', StringType(), True),
            StructField('product_Count', IntegerType(), True),

        ])
        exp_location_data = [( "mumbai",2),
                             ( "usa",2),
                             ("nagpur",1)
                             ]
        exp_location_df = self.spark.createDataFrame(data=exp_location_data, schema=exp_location_schema)
        act_location_df = UniqueLocation(transformdata_df)
        self.assertEqual(sorted(act_location_df.collect()), sorted(exp_location_df.collect()))

        # Products bought by user
        exp_product_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('collect_list(product_description)', ArrayType(StringType()), True)
        ])
        exp_product_data = [(101, ['mouse','fridge']),
                            (102, ['keyboard']),
                            (103, ['tv']),
                            (105, ['sofa'])
                            ]
        exp_product_df = self.spark.createDataFrame(data=exp_product_data, schema=exp_product_schema)
        act_product_df = ProductBought(transformdata_df)
        self.assertEqual(sorted(act_product_df.collect()), sorted(exp_product_df.collect()))

        # Total_spend testcase
        exp_total_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('Total_Spending', IntegerType(), True)
        ])
        exp_total_data = [(101, 35700),
                          (102, 900),
                          (103, 34000),
                          (105, 55000),
                          ]
        exp_total_df = self.spark.createDataFrame(data=exp_total_data, schema=exp_total_schema)
        act_total_df = TotalSpending(transformdata_df)
        self.assertEqual(sorted(act_total_df.collect()), sorted(exp_total_df.collect()))  # add assertion here


if __name__ == '__main__':
    unittest.main()
