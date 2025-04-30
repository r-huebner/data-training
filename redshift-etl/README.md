#### Purpose of database:
-------------------------

###### Goal

- Provide a dimensional database for efficient and easy access to company data for the analytics team of Sparkify.
- The data to be analysed is event data from the users listening to different songs on the platform, which are described in the song data.


#### ETL Pipeline
-----------------------

###### Extract: 
Event and song data in raw JSON format is extracted from Amazon S3.

###### Load (Staging):
The raw data is loaded into two staging tables.

###### Transform & Load:
From there, data is transformed and inserted into dimensional tables in a Redshift data warehouse.

Additional:
- Data quality checks are executed to validate the integrity of the loaded data.
- Sample queries are run to showcase capabilities.


#### Database Schema
-----------------------

- The star schema of the data warehouse is optimised for querying data for analysis purposes.
- There are 1 fact table (songplay) and 4 dimension tables (songs, users, artists, times).


##### Data distribution:

###### Distribution key

If a distribution key for a table is set, the distribution changes from the default even distribution to a key dirstribution. 
In that case we want a high cardinality to ensure all slices are used for parallelisation.
Furthermore we want to use a column that is most often used in the joins in the queries.

- Distribution key on song_id allows for an even distribution on the slices and as we want to have data on songs we anticipate that it will be used with almost all of the queries. 

Why was user_id not used?
- The information on frequency and trends can be analysed without the username by just using the user_id of the fact table. (some data like gender and level can still be relevant)
- The anonymisation is also good for protecting sensitive user data.

###### Sort key

- The songplay table and time table are sorted by start_time, enabling faster time-based filtering.
- The user, songs, artists dimensions tables are not sorted as we do not anticipate many lookups on the song or artist names and the user table is quite small.


##### Transformations:

- Only data from the events is taken where a song is played (on the NextSong page).
- Users that both have listened to paid and free songs are inserted as paid users.


#### File structure:
--------------------

root 
|_ README.txt
|_ dwh.cfg
|_ sql_queries.py 
|_ create_tables.py 
|_ etl.py 


#### How to Run:
-------------------

1. Fill out dwh.cfg file with Redshift cluster, database and AWS credentials.
2. Run the create_tables.py to connect to Redshift and create the tables. (Any existing duplicate tables will be dropped.)
3. Run etl.py to extract, transform and load the data from the S3 into Redshift and to run data checks and sample queries.


#### Sample queries:
-----------------------------

1. Get the 3 most popular songs by gender.
2. Get the ten most played artists in 2018.
3. Get the 3 top users with the most played songs.
4. Get song count by location and level.


#### Optimatization points:
---------------------------

- Most of the songs/artists from the event table dont match the songs/aritists from the songs table
- There needs to be a better way to check if the user is a paid user or a free user (not from the songs they listened to)
- Specify VARCHAR size (need more data exploration)
- Evaluate the distribution and sorting keys by checking what queries are used most often and their performance
- Add error handling
- Refractor WHERE-constrains for AND and OR, JOIN-conditions for inner, outer, left, right join