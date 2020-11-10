import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, udf, col, row_number, date_format
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, DecimalType, LongType, IntegerType

# The problems/errors encoutered while developing this script were resolved with the help of google, stackoverflow and udacity mentor/knowledge base.
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def specify_log_schema():    
    schema = StructType(        
     [            
      StructField('artist', StringType(), True),            
      StructField('auth', StringType(), True),            
      StructField('firstName', StringType(), True),            
      StructField('gender', StringType(), True),            
      StructField('itemInSession', IntegerType(), True),            
      StructField('lastName', StringType(), True),            
      StructField('length', DecimalType(), True),            
      StructField('level', StringType(), True),            
      StructField('location', StringType(), True),            
      StructField('method', StringType(), True),            
      StructField('page', StringType(), True),            
      StructField('registration', LongType(), True),            
      StructField('sessionId', IntegerType(), True),            
      StructField('song', StringType(), True),            
      StructField('status', IntegerType(), True),            
      StructField('ts', LongType(), True),            
      StructField('userAgent', StringType(), True),            
      StructField('userId', StringType(), True)        
     ]    
    )   
    return schema

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
     song_data = input_data + "song_data"
    # When testing, work with smaller dataset
    # song_data = input_data + "song-data/A/B/C/TRABCEI128F424C983.json" 

    print("processing songs table ......................")    
    # read song data file
    df = spark.read.json(song_data)    
    
    #  Create view to query songs data.
    df.createOrReplaceTempView("base_songs_tbl")
    
    # Generate songs table.
    songs_table = spark.sql("""
                            SELECT 
                            DISTINCT 
                                song_id, title, 
                                artist_id, year, duration
                            FROM 
                                base_songs_tbl
                            WHERE song_id IS NOT NULL
                            """) 

    # write songs table to parquet files partitioned by year and artist 
    # set output file path        
    songs_table_path = output_data + "songs_table.parquet"
    
    print(".............................................")
    print("generate songs table in parquet format.......")    
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(songs_table_path)
    print(".............................................")
    
    print("processing artists table ....................")    
    #  Create view to query artists data.
    df.createOrReplaceTempView("base_artists_tbl")

    # Generate artists table.
    artists_table = spark.sql( """
                                SELECT 
                                DISTINCT
                                    artist_id,
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                                FROM 
                                    base_artists_tbl
                                   """)
    
    # write artists table to parquet files
    # set output file path        
    artists_table_path = output_data + "artists_table.parquet"
    
    print("generate artists table in parquet format.....")        
    artists_table.write.mode("overwrite").parquet(artists_table_path)
    print(".............................................")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data"    
    
    # Populate schema
    log_schema = specify_log_schema()   

    # read log data file
    df = spark.read.json(log_data, schema=log_schema)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    print("processing user table .......................")   
    #  Create view to query songs data.
    df.createOrReplaceTempView("base_user_table")

    # Generate user table.
    user_table = spark.sql( """
                            SELECT
                             e.userId,
                             e.firstName,
                             e.lastName,
                             e.gender,
                             coalesce(eo.level, e.level) as level
                            FROM
                            (
                              SELECT
                              DISTINCT
                               eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
                              FROM
                               base_user_table eo
                              WHERE userId IS NOT NULL
                              AND eo.level = 'free') e
                              LEFT JOIN
                              (SELECT
                               DISTINCT
                                eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
                               FROM
                                base_user_table eo
                               WHERE userId IS NOT NULL
                               AND eo.level <> 'free') eo ON(e.userId = eo.userId)
                          """)
   
    # write users table to parquet files
    # set output file path        
    user_table_path = output_data + "user_table.parquet"    
    
    print("generate user table in parquet format........")           
    user_table.write.mode("overwrite").parquet(user_table_path)
    print(".............................................")
    
    print("processing time table .......................")
    # Generate time table.
    # UDF to convert column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    # Calculate time and create a new dataframe
    df = df.withColumn('start_time', get_timestamp(df.ts))
    time_table = df.withColumn('start_time', get_timestamp(df.ts))

    # Calculate all aspects of time and add the columns to dataframe.
    time_table = time_table.withColumn("year", year("start_time"))\
                            .withColumn("hour", hour("start_time"))\
                             .withColumn("month", month("start_time"))\
                             .withColumn("day", dayofmonth("start_time"))\
                             .withColumn("week", weekofyear("start_time"))\
                             .withColumn("weekday", date_format(col("start_time"), "EEEE"))
    
    # write time table to parquet files
    # set output file path  
    time_table_path = output_data + "time_table.parquet"   
    print("generate time table in parquet format........") 
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(time_table_path)
    print(".............................................")
   
    #  Generate songsplay table.

    # read song data file    
    # When testing, work with smaller dataset
    song_data = input_data + "song_data/A/B/C/TRABCEI128F424C983.json" 

    print("processing songplay table....................")
    # song_data = input_data + "song_data"
    songs_df = spark.read.json(song_data)    
    
    #  Create view to query songs data.
    songs_df.createOrReplaceTempView("base_songs_tbl")
    
    #  Use the original dataframe with log data to query logs
    df.createOrReplaceTempView("base_songs_log_tbl")   
    #  Join song and event data to build songplays table.
    songplays_table = spark.sql("""
                                SELECT
                                DISTINCT
                                 e.start_time,
                                 extract(month from e.start_time) as month,
                                 extract(year from e.start_time) as year,
                                 e.userId as user_id,
                                 e.level,
                                 s.song_id,
                                 s.artist_id,
                                 e.sessionId as session_id,
                                 e.location,
                                 e.userAgent as user_agent
                                FROM
                                 base_songs_log_tbl e, 
                                 base_songs_tbl s
                                WHERE                                 
                                 e.userId IS NOT NULL
                                 AND e.song = s.title and e.artist = s.artist_name
                               """)         
    # write songplays table to parquet files partitioned by year and month    
    # set output file path  
    songplays_table_path = output_data + "songplays_table.parquet" 
    print("generate songplays table in parquet format...")
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(songplays_table_path)
    print(".............................................")
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-156719884113-us-west-2/output/"
    

    print("..............Populating tables..............")        
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    print("..............Population complete............")
    spark.stop();
    
if __name__ == "__main__":
    main()
