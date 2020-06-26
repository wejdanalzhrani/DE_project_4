# Data Warehouse with AWS

---



### Introduction


- A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app




As data engineer, I building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

This provides an environment that is designed for:

* Reduce the cost of managing and maintaining the IT systems 
* Scale up or scale down depends on our need.
* Flexibility of work practices 
and more .. 



### Database schema design and ETL process:


* I modeled the database using the Star Schema Model by used python and Redshift data base.We have got tow stages table "staging_events_copy" and "staging_songs_copy" then transformat it into one Fact table, "songplays" along with four more Dimension tables named users", "songs", "artists" and "time".


* First, I used IaC to perform ETL on the tow dataset, `song_data` and `log_data` , to load data from S3 to staging tables on Redshift "staging_events_copy" and "staging_songs_copy".


* Then, load data from staging tables to analytics tables on Redshift the `artists`,`time` and `users` dimensional tables, as well as the `songplays` fact table.





### Files in repository

Description of files and how to use them in your own application are mentioned below.

| File | Description |
| ------ | ------ |
| dwh.cfg |Contain of CLUSTER, IAM, S3, AWS, and DWH info |
|IaC.py|Used infrastructure as code to got informations and save it to dwh.cfg|
| create_tables.py | is where you'll create your fact and dimension tables for the star schema in Redshift.|
| etl.ipynb | reads and processes a single file from song_data and log_data and loads the data into tables. This notebook contains detailed instructions on the ETL process for each of the tables.|
| etl.py | is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.|
| sql_queries.py |is where you'll define you SQL statements, which will be imported into the two other files above.|
| Project3_guidline.ipynb |Instructions for how to implement ETL pipeline.|





