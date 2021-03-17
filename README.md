# Pokemon ETL Pipeline

The goal of this project is to develop an ETL pipeline using AWS Infrastructure, 
we will pull the Pokemon data as a request to an API, then we will clean and 
transform the data and store it in AWS Glue Data Catalog, finally, we will load 
it into an table in Snowflake. All of these will be done using Python and Spark shell in AWS.

Url for Pokemon data: [Pokemon API](https://raw.githubusercontent.com/ClaviHaze/CDMX009-Data-Lovers/master/src/data/pokemon/pokemon.json)

![ETL-Pipeline](https://i.ibb.co/dWDr0G7/Screen-Shot-2021-03-17-at-10-46-56.png)


To orchestrate this process, we will use AWS Step Functions where we will have 3 steps 
(each of them a glue job) to manage de data:

* Data ingestion
* Data Transform
* Data Load

![Step Function](https://i.ibb.co/q1Qsdzr/Screen-Shot-2021-03-17-at-10-50-10.png)


## Data ingestion

This is the first glue job, in this one you will extract the data from the API and you will 
store it in a bucket in s3 (s3://<your-bucket>/raw/)
  
  
## Data transformations

The second glue job is for the data transformation, you will take the raw data and then apply 
the transformations below, then you will write to a table in AWS Glue Data Catalog where you 
will specify the schema of your data already transformed which under the hood will be uploaded 
to s3 (s3:// <your-bucket>/prepared/)
  
  * 1. Add a column for the calculation of the BMI (Body Mass Index) of the Pokemon
  
    ![Formula](https://i.ibb.co/qFJ8Wss/Screen-Shot-2021-03-17-at-10-54-39.png)
  
  * 2. Add a column that displays the obesity label for each Pokemon taking into account the column 
       BMI and the following chart:
       
     ![Table](https://i.ibb.co/3mBgZ90/Screen-Shot-2021-03-17-at-10-59-17.png)
     
   * 3. Filter the Pokemons to only keep the ones with an average spawn between 1 and 220.
   
   * 4. Filter the Pokemons to show everyone but the one’s which eggs have “Not in Eggs”
   
   * 5. Update the column name to display the name of the Pokemon + your last name (i.e. Bulbasaur Patron)
   
   * 6. Remove img, num, and candy_count columns
   
## Data Loading:

In this job, you will take the data already transformed and you will load it into a table in Snowflake.
     
