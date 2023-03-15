import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
from pyspark.sql import functions as F
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ["AWS_ACCESS_KEY_ID"]=config["aws"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config["aws"]["AWS_SECRET_ACCESS_KEY"]

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Process song data from given input_path and store it at output_data as parquet files.
    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None
    """
    # get filepath to song data file. 
    song_data = input_data + "song-data/A/A/A/*.json"
    
    #define schema for song data file
    song_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
        ])
    
    # read song data file
    df_song = spark.read.json(song_data, schema=song_schema)
    # extract columns to create songs table
    songs_table = df_song.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates(subset=["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songs'), partitionBy=['year',  'artist_id'])
    
    # extract columns to create artists table
    artists_table = df_song.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as lattitude', 'artist_longitude as longitude').dropDuplicates(subset=["artist_id"])
    
    # write artists table to parquet files
    artists_table = artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))

def process_log_data(spark, input_data, output_data):
    """Process log data from given input_path and store it at output_data as parquet files.
    Args:
        spark(pyspark.sql.SparkSession): SparkSession object
        input_data(string): Path to input data directory. End with '/' e.g. 'data/log/'
        output_data(string): Path to output data directory. End with '/' e.g. 'data/log/'
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/*/*.json"
    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')
    
    # extract columns for users table    
    users_table =  df_log.selectExpr('userID as user_id', 'firstName as first_name', ' lastName as last_name', 'gender', 'level').dropDuplicates(subset=["user_id"])
    
    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users'))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df_log = df_log.withColumn("datetime", get_datetime(df_log.ts))
    
    # extract columns to create time table
    time_table = df_log.selectExpr(
        "timestamp as start_time",
        "hour(timestamp) as hour",
        "dayofmonth(timestamp) as day",
        "weekofyear(timestamp) as week",
        "month(timestamp) as month",
        "year(timestamp) as year",
        "dayofweek(timestamp) as weekday"
    ).dropDuplicates(subset=["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+ "time_table")
    
    # read in song data to use for songplays table
    song_data = input_data + "song-data/A/A/A/*.json"
    df_song = spark.read.json(song_data)

    df_log = df_log.withColumn('songplay_id', F.monotonically_increasing_id())
    df_log.createOrReplaceTempView("df_log")
    df_song.createOrReplaceTempView("df_song")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
SELECT
    l.songplay_id,
    l.timestamp as start_time,
    year(l.timestamp)as year,
    month(l.timestamp) as month,
    l.userID as user_id,
    l.level as level,
    s.song_id as song_id,
    s.artist_id as artist_id,
    l.sessionId as session_id,
    l.location as location,
    l.userAgent as user_agent
FROM 
    df_log as l 
JOIN
    df_song as s ON s.song_id = l.song AND s.artist_name = l.artist AND s.duration = l.timestamp
""")    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+ "songplay")
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://soniadatalake/"  
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
if __name__ == "__main__":
    main()