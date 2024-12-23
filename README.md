# Building Azure Data Lake for Bike Share Data using Azure Databricks, Azure Synapses and Spark SQL

## Project purpose
This project focuses on designing data models, building data warehouses, implementing data lakes and lakehouse architectures, creating data pipelines, and working with large datasets using Azure Synapse Analytics. It is part of the "Data Engineering with Microsoft Azure" program offered by Udacity.

## Project Overview
In this project, you'll build a data lake solution for Divvy bikeshare. Divvy is a bike sharing program in Chicago, Illinois USA that allows riders to purchase a pass at a kiosk or use a mobile application to unlock a bike at stations around the city and use the bike for a specified amount of time. The bikes can be returned to the same station or to another station. The City of Chicago makes the anonymized bike trip data publicly available for projects like this where we can analyze the data. Since the data from Divvy are anonymous, we have generated fake rider and account profiles along with fake payment data to go along with the data from Divvy. The dataset looks like this:
 
## Objective
The goal of this project is to develop a data lake solution using Azure Databricks using a lake house architecture. The project involve the following steps:
•	Design a star schema based on the business outcomes listed below;
•	Import the data into Azure Databricks using Delta Lake to create a Bronze data store;
•	Create a gold data store in Delta Lake tables;
•	Transform the data into the star schema for a Gold data store.

## Business outcomes
1.	Analyze how much time is spent per ride
•	Based on date and time factors such as day of week and time of day
•	Based on which station is the starting and / or ending station
•	Based on age of the rider at time of the ride
•	Based on whether the rider is a member or a casual rider
2.	Analyze how much money is spent
•	Per month, quarter, year
•	Per member, based on the age of the rider at account start
3.	EXTRA CREDIT - Analyze how much money is spent per member
•	Based on how many rides the rider averages per month
•	Based on how many minutes the rider spends on a bike per month

## TASK NEED TO COMPLETE
## Bike Dataset

![Original Bike Dataset](https://github.com/user-attachments/assets/5d476c92-a71c-4695-a7a2-105b4af8f598)


## Task 1: Design Bikeshare Star Schema. Below is our star schema:

[Bike_Star Scheme.pdf](https://github.com/user-attachments/files/18228233/Bike_Star.Scheme.pdf)

 ## Task 2: Import the data into Azure Databricks using Delta Lake to create a Bronze data store
1.	Create Azure Databricks Workspace (use the resource group already created
  
![1_Azure_DataBrick_Workspace](https://github.com/user-attachments/assets/b3a6145b-2239-48cb-aa49-ebcf0d1e448d)


2.	Create Databricks Cluster
 ![2_DataBrick_Clusters](https://github.com/user-attachments/assets/02dd45d0-a73e-43bb-8336-1b4efb02c965)


## 3.	Import data to DBFS path /FileStore/tables/
 
3.1 Extract: Load data into Databricks as dataframes and Write data into Bronze data lake
![3 1_Enable_DBFSFiles](https://github.com/user-attachments/assets/d1b06d75-bfa2-4d87-a025-dd87883c50e7)

Load CSV files

Payments
payments_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .option("sep", ",") \
    .load("dbfs:/FileStore/tables/payments.csv") \
    .toDF("payment_id","payment_date","amount","rider_id")	

Riders
riders_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .option("sep", ",") \
    .load("dbfs:/FileStore/tables/riders.csv") \
    .toDF("rider_id", "first_name", "last_name", "address", "birthday", "account_start_date", "account_end_date", "is_member")

Stations
stations_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .option("sep", ",") \
    .load("dbfs:/FileStore/tables/stations.csv") \
    .toDF("station_id", "name", "latitude", "longitude")

Trips 
trips_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .option("sep", ",") \
    .load("dbfs:/FileStore/tables/trips.csv") \
    .toDF("trip_id", "rideable_type", "start_at", "ended_at", "start_station_id", "end_station_id", "rider_id")
![3 2_Import_data_to_DBFS](https://github.com/user-attachments/assets/b2f955c6-2d68-462e-9a4b-21bbdcdbda6b)


3.2 Write into Dataframes Delta Lakes tables 
Payments
payments_df.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/payments")

Riders
riders_df.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/riders")

Satations
stations_df.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/stations")

Trips
trips_df.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/trips")

3.3 Write Datagrams into bronze data lake
delta_path = "/bronze"
df_payments.write.format("delta").mode("overwrite").save(f"{delta_path}/payments")
df_riders.write.format("delta").mode("overwrite").save(f"{delta_path}/riders")
df_stations.write.format("delta").mode("overwrite").save(f"{delta_path}/stations")
df_trips.write.format("delta").mode("overwrite").save(f"{delta_path}/trips")

![3 3_Tables_into_Datalake](https://github.com/user-attachments/assets/dfbce5fb-d151-4f93-8c39-2842f0293e2d)

 ## 4.	Load Spark SQL from Delta tables

![4_Staging_tables](https://github.com/user-attachments/assets/dacc1a4a-2108-461d-9b06-c31b220bbf03)

 
## 5.	Transform Delta source tables into designed star schema (dim/fact tables)

![5_Transform_ StartShema_GoldDataStorepng](https://github.com/user-attachments/assets/d1c8b678-df2f-4421-8b7e-4fd54f287c20)




