import unittest
from pyspark.sql import *
from SparkRepo1.src.Assignment2.util import *
from pyspark import SparkContext
from pyspark.sql.types import StringType,StructType,StructField,IntegerType
from pyspark.sql.functions import *

class MyTestCase(unittest.TestCase):
    spark = SparkSession.builder.appName("Assignment2").getOrCreate()
    def test_Assignment(self):
        filepath = "C:/Users/KOLAPRABHU&PARVATHI/PycharmProjects/SparkRepo/resource/ghtorrent-logs.txt""
        # creating spark session
        spark = spark_session()

        # creating rdd

        rdd = creating_rdd(spark, filepath)
        actual_rdd = rdd.collect()
        expected_rdd = ['DEBUG,2017-03-24T13:56:38+00:00,ghtorrent-46 -- ghtorrent.rb: Repo agco/harvesterjs exists',
                        'WARN,2017-03-23T11:04:43+00:00,ghtorrent-32 -- ght_data_retrieval.rb: Error processing event. Type: IssueCommentEvent, ID: 5531718847, Time: 64 ms',
                        'DEBUG,2017-03-23T10:44:44+00:00,ghtorrent-14 -- api_client.rb: Sleeping for 931 seconds',
                        'DEBUG,2017-03-23T09:36:14+00:00,ghtorrent-42 -- api_client.rb: Sleeping for 1441 seconds']
        self.assertEqual(actual_rdd, expected_rdd)

        # test case for number of lines in rdd

        num_lines = num_of_lines(rdd)
        expected_output = 4
        self.assertEqual(num_lines, expected_output)

        # test case for creating a dataframe from rdd

        df = reading_file(spark, filepath)
        df_torrent = create_dataframe_from_rdd(df)
        df_torrent.show(truncate=False)

        data1 = [
            ("DEBUG", "2017-03-24T13:56:38+00:00", "ghtorrent-46 ", " ghtorrent.rb", " Repo agco/harvesterjs exists"),
            ("WARN", "2017-03-23T11:04:43+00:00", "ghtorrent-32 ", " ght_data_retrieval.rb",
             " Error processing event. Type"),
            ("DEBUG", "2017-03-23T10:44:44+00:00", "ghtorrent-14 ", " api_client.rb", " Sleeping for 931 seconds"),
            ("DEBUG", "2017-03-23T09:36:14+00:00", "ghtorrent-42 ", " api_client.rb", " Sleeping for 1441 seconds")
        ]

        df_expec = spark.createDataFrame(data1,
                                         ["log message", "timestamp", "downloader", "repository_clients",
                                          "commit_message"])

        df_expec.show(truncate=False)
        self.assertEqual(df_torrent.collect(), df_expec.collect())

        # getting count of warn messages

        df_warn = warn_messages(df_torrent)
        expected_warn_output = 1
        self.assertEqual(df_warn, expected_warn_output)

        # test case for getting failed clients

        df_api_clients_actual = api_clients(df_torrent)
        expected_api_clients = 2
        self.assertEqual(df_api_clients_actual, expected_api_clients)

        # test case for most http requests

        df_most_http_requests = http(df_torrent)
        df_most_http_requests.show()
        schema_most_requests = StructType([
            StructField("repository_clients", StringType())])
        data_most_requests = []
        expected_most_req_df = spark.createDataFrame(data=data_most_requests, schema=schema_most_requests)
        self.assertEqual(df_most_http_requests.collect(), expected_most_req_df.collect())

        # test case for most failed requests

        df_most_failed_requests = failed_http(df_torrent)
        schema_most_failed = StructType([
            StructField("repository_clients", StringType())])
        data_most_failed = []
        expected_most_failed_df = spark.createDataFrame(data=data_most_failed, schema=schema_most_failed)
        self.assertEqual(df_most_failed_requests.collect(), expected_most_failed_df.collect())

        # test case for most active hours

        df_most_active_hours = active_hours(df_torrent)
        expected_active_data = [(16,)]
        expected_active_schema = StructType([
            StructField('hour', IntegerType(), True)
        ])
        expected_df_more_active_hours = spark.createDataFrame(data=expected_active_data, schema=expected_active_schema)
        self.assertEqual(df_most_active_hours.collect(), expected_df_more_active_hours.collect())

        # test case for most active repositories

        df_most_active_repositories = active_repo(df_torrent)
        expected_activerepo_schema = StructType([
            StructField('log message', StringType(), True),
            StructField('timestamp', StringType(), True),
            StructField('downloader', StringType(), True),
            StructField('repository_clients', StringType(), True),
            StructField('commit_message', StringType(), True)
        ])
        expected_activerepo_data = [
            ("DEBUG", "2017-03-24T13:56:38+00:00", "ghtorrent-46 ", " ghtorrent.rb", " Repo agco/harvesterjs exists")
        ]
        expected_activerepo_df = spark.createDataFrame(data=expected_activerepo_data, schema=expected_activerepo_schema)
        self.assertEqual(df_most_active_repositories.collect(), expected_activerepo_df.collect())



if __name__ == '__main__':
    unittest.main()
