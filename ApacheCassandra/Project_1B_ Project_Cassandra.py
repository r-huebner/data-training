#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# In[5]:


df=pd.read_csv('event_datafile_new.csv')
df.head()


# In[6]:


df.info()


# # Part II: Data Modeling for Apache Cassandra 

# #### Creating a Cluster

# In[7]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[8]:


# Create a Keyspace
try:
    session.execute("""CREATE KEYSPACE IF NOT EXISTS project_nosql 
                    WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}""")
except Exception as e:
    print(e)


# #### Set Keyspace

# In[9]:


# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace("project_nosql")
except Exception as e:
    print(e)


# ## Create tables for the three following queries:
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[10]:


# List with all tables
tables =[]

# CSV file with data
file = 'event_datafile_new.csv'


# In[11]:


# Function for verifying table content
def verify_table(table_name):
    rows = list(session.execute("SELECT * FROM "+table_name))
    
    print("Verifying table content")
    print("-----------------------")
    print("Table length: ",len(rows))
    print("First row:")
    print(pd.DataFrame([rows[0]]))


# In[12]:


# Function for executing query
def execute_query(query,query_name,query_text):
    rows = session.execute(query)
    print("Verifying "+query_name)
    print("-----------------------")
    print(query_text)
    print("\n")
    print("Query:")
    print(query)
    print("\n")
    print("Result:")
    data = [row for row in rows]
    print(pd.DataFrame(data))
    print("\n")


# ### Table for Query 1

# In[13]:


# Save table name for easy renaming
table_name = "songs_by_session"
tables.append(table_name)

# Query 1:  
query_text1= "Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4."
query_1= "SELECT artist, song, length from "+table_name+" WHERE sessionId = 338 and itemInSession = 4"

try:
    # Create table for query1 with composite session id and session item count key
    session.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY ((sessionId,itemInSession)))")
    
    # Insert data
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO "+table_name+" (sessionId, itemInSession, artist, song, length)"
            query = query + "VALUES (%s,%s,%s,%s,%s)" 
            session.execute(query, (int(line[8]), int(line[3]),line[0], line[9], float(line[5])))
        
    # Verify the data was entered into the table
    verify_table(table_name)
    
except Exception as e:
    print(e)


# ### Table for Query 2

# In[14]:


# Save table name for easy renaming
table_name = "song_by_session_and_user"
tables.append(table_name)

# Query 2: 
query_text2= "Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182"
query_2= "SELECT artist, song, firstName, lastName from "+table_name+" WHERE userid = 10 and sessionid = 182"

try:
    # Create table for query2 (userId is not unique, so we need session id and session item count as well) 
    session.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (userId int, sessionId int, itemInSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY ((userId),sessionId,itemInSession))")

    # Insert data
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO "+table_name+" (userId, sessionId, itemInSession, artist, song, firstName, lastName)"
            query = query + "VALUES (%s,%s,%s,%s,%s,%s,%s)" 
            session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

    # Verify the data was entered into the table
    verify_table(table_name)

except Exception as e:
    print(e)


# ### Table for Query 3

# In[15]:


# Save table name for easy renaming
table_name = "user_by_song"
tables.append(table_name)

# Query 3: 
query_text3= "Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'"
query_3= "SELECT firstName, lastName from "+table_name+" WHERE song = 'All Hands Against His Own'"

try:
    # Create table for query3 (song is not unique, so we need session id and session item count as well) 
    session.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (song text, sessionId int, itemInSession int, firstName text, lastName text, PRIMARY KEY ((song),sessionId,itemInSession))")

    # Insert data
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO "+table_name+" (song, sessionId, itemInSession, firstName, lastName)"
            query = query + "VALUES (%s,%s,%s,%s,%s)" 
            session.execute(query, (line[9], int(line[8]), int(line[3]), line[1], line[4]))

    # Verify the data was entered into the table
    verify_table(table_name) 
    
except Exception as e:
    print(e)
              


# In[16]:


# Execute all 3 queries
try:
    # Query1
    execute_query(query_1,"Query1",query_text1)

    # Query2
    execute_query(query_2,"Query2",query_text2)

    # Query3
    execute_query(query_3,"Query3",query_text3)
    
except Exception as e:
    print(e)


# ### Drop the tables before closing out the sessions

# In[17]:


try:
    for table in tables:    
        session.execute("DROP TABLE IF EXISTS "+ table)
        print("Dropped table "+ table)
        
except Exception as e:
    print(e)


# ### Close the session and cluster connection

# In[18]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




