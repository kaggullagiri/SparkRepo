from SparkRepo.src.Assignment2.util import *

spark=spark_session()

filepath= "C:/Users/KOLA PRABHU&PARVATHI/PycharmProjects/SparkRepo/resource/ghtorrent-logs.txt"
rdd=creating_rdd(spark,filepath)


num_lines=num_of_lines(rdd)


df=reading_file(spark,filepath)

df_torrent = create_dataframe_from_rdd(df)

df_warn=warn_messages(df_torrent)

df_api_clients=api_clients(df_torrent)


df_http=http(df_torrent)
df_http.show()
#
df_failed_http=failed_http(df_torrent)
df_failed_http.show()
#
df_active_hours=active_hours(df_torrent)
df_active_hours.show()
#
df_active_repo= active_repo(df_torrent)
df_active_repo.show()