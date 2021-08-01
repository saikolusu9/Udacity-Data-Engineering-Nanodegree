<p align="center">
 <a href="https://github.com/saikolusu9/Data-Engineering-Nanodegree">
  <img src="./images/cassandralogo.png" alt="Logo" width="200" height="200">
 </a>
 <h2 align="center">Data Modeling ETL with Apache Cassandra</h2>
 <h3 <p align="center">
 Project 2 </h2>
  <br />
  
<br />
<br />
 


<!-- ABOUT THE PROJECT -->

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming application. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the application, as well as a directory with JSON meta-data on the songs in their application.

They'd like a data engineer to create a Apache Cassandra database which can create queries on song play data to answer the questions and make meaningful insights. The role of this project is to create a database schema and ETL pipeline for this analysis. 

</br>
</br>

## Project Description

In this project, we will model the data with Apache Cassandra and build an ETL pipeline using Python. The ETL pipeline transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables. We will create separate denormalized tables for answering specific queries, properly using partition keys and clustering columns.

### Built With

* python
* Apache Cassandra
* iPython notebooks

### Dataset

#### Event Dataset

Event dataset is a collection of CSV files containing the information of user activity across a period of time.  Each file in the dataset contains the information regarding the song played, user information and other attributes . 

List of available data columns :

```
artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userId
```


</br>
</br>

## Keyspace Schema Design

### Data Model ERD

The keyspace design is shown in the image below. Each table is modeled to answer a specific known query. This model enables to query the database schema containing huge amounts of data. Relational databases are not suitable in this scenario due to the magnitude of data. 

![database](./images/keyspace.png)

## Project structure

Files in this repository:

|     File / Folder      |                         Description                          |
| :--------------------: | :----------------------------------------------------------: |
|       event_data       | Folder where all user activity CSVs reside |
|         images         |  Folder where images used in this project are stored  |
| event_datafile_new.csv | File that contains the data after merging the CSV files at `event_data` |
|    Project 2.ipynb     | File containing the ETL pipeline including data extraction, modeling and loading into the keyspace tables. |
|         README         |                         Readme file                          |



</br>
</br>

## How to run


1. Navigate to `Project 2 Data Modeling with Apache Cassandra` folder
2. Run `Project 2.ipynb` iPython notebook
3. Run Part 1 to create `event_datafile_new.csv` 
4. Run Part 2 to initiate the ETL process and load data into tables 

5. Check whether the data has been loaded into database by executing `SELECT` queries
