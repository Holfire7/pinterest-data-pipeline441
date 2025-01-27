# TITLE: PINTEREST DATA PIPELINE

## TABLE OF CONTENT
- Project Description.
- Setting up and getting Started.
- Configuring the kafka Client and  Configuring the API Gateway.
- Reading the data on Databricks and spark transformation.
- Creating a DAG.
- AWS Kinesis.
- License Information.
- File structure of the project.

## PROJECT DESCRIPTION

The Pinterest data pipeline project is a coding project that seeks to imitate the pinterest system in getting data and using that data to create more value for it's users all around the globe. This system of getting and analysing data for better customer experience is what I created using a set of codes to imitate how pinterest does theirs.
In this project, I made use of AWS services provided by the AICORE platform and a set of codes to ingest the data, stored in an s3 bucket and stored the transformed data in a delta table using databricks. 
The aim of the project was to better get a hands-on knowledge on the use of AWS services in handling data, the use of kafka clients, DAGS, APIs, Databricks and Spark, and the use of kinesis in getting, transforming and storing streaming data. 
Through this project, I have been able to better understand how to handle batch data, transform and store the data for future use. I have also learned how to use kinesis in the getting, transformation and storage of streaming data. I also better understand the importance of data security by encrypting database details where the data is gotten from. These can be better explained in the process of how I conducted the project below:

1. SETTING UP AND GETTING STARTED: An AWS account was set up and granted acess to me which I used in line with my previously created GitHub account. Then with a template python file containing the database acess details, I edited the file which enabled me download the pinterest data in 3 tables (pin_result, geo_result and user_result). An AWS account was also granted to me with which I was able to acess all AWS services needed for my project work.

2. CONFIGURING THE KAFKA CLIENT AND CONFIGURING THE API GATEWAY: with an already created .pem key, I was able to connect to the EC2 on AWS and created 3 topics based on the 3 results as stated in the set up above (<my_UserId>.pin, <my_UserId>.geo, <my_UserId>.user). Here, I also created a kafka REST proxy API where I sent data to my s3 bucket based on the result partition(i.e: topics/<my_UserId>.pin/partition=0/).

3. READING THE DATA ON DATABRICKS AND SPARK TRANSFORMATION: with access to databricks, I read the data from the three buckets and loaded onto data frames based on their topics(i.e: df_pin, df_geo, df_user). With the use of spark on databricks, I was able to transform the data, removing unwanted and erronous data from the raw data, allowing for easy querying of the data using SQL on databricks.

4. CREATING A DAG: on AWS MWAA, access to the dags bucket was provided, where I uploaded my dag python file and scheduled it to run daily. The dag was triggered and found to run successfully. 

5. AWS KINESIS: The concept of streaming data and it's transformation was well understood in this section. After finding the kinesis data stream on AWS kinesis, I configured the previously created API  to a kinesis proxy integration, which was used to send data to the kinesis stream. Then on databricks, the streams were read, transformed and stored in delta tables based on their stream results(<my_UserId>_pin_table, <my_UserId>_geo_table and <my_UserId>_user_table).

LICENSE INFORMATION

This project is licensed under the MIT License. See the LICENSE file in the root of the repository for more details.

FILE STRUCTURE OF THE PROJECT:

All files can be found individually as there was no need to put them in directories.

project-root/
|├── .gitignore

|├── 0eaf46a0829f_dag.py

|├── AWS Kinesis work.ipynb

|├── db_creds.yaml

|├── Pinterest_data_project (1).ipynb

|├── README.md

|├── user_posting_emulation.py

|├── user_posting_emulation_streaming.py

|├── LICENSE.txt

