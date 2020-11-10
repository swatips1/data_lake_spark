Sparkify, a startup stramping app, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
With growing user base and song database they want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  This ETL pipeline extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


The details of contents of this project are as below:
1) All the data used by this application can be found in S3 bucket, specifically:
    's3a://udacity-dend/'    
2) dl.cfg: This is the configuration file that holds the user credentials.    
3) etl.py: This is where the data is read from the S3 bucket into the staging tables and then from staging tables into the main tables, all using spark. 

6) test.ipynb: Provides the developer an easy way to Calls create_tables.py and  etl.py and observe the results of each operation.

The DB Schema consists of the following:
1) users: List of all users of Sparkify.
2) songs: List of all songs available for play in Sparkify.
3) artists: List of all artists whose songs are available for play in Sparkify.
4) times: A table that stores all unique times across all song played in Sparkify. It provides all aspects of time individually e.g. hour of play etc.
5) songplays: The fact table supplies the metrics used for the song plays analytics.
    
To run:
The ETL process is quite simple to execute:
To run on EMR:
1) Launch a EMR in the same region as the S3 bucket. Make sure the IAM user and security group is propery configured and linked to the cluster.
2) Make sure the EMR is correctly configured for SSH and Firefox connectivity.
3) Make sure all parameters in dl.cfg are configured with valid information.
4) Make sure you are reading and writing from the correct S3 buckets and that they reside in the same region.
5) Upload relevant files into the cluster and run the process.

To run locally:
1) Make sure the EMR is correctly configured for SSH and Firefox connectivity.
2) Make sure you are reading and writing from the correct S3 buckets and that they reside in the same region.
3) Run test.ipynb
    NOTE: It is best to make sure the Kernel is in proper state. One way would be to re-start the Kernel prior to repeate runs. 

To see the output:
1) Login to your Amazon account. 
2) Navigate to the S3 bucket the etl was writing to. 
3) Make sure you see expected output file. e.g. artists_table.parquet/
