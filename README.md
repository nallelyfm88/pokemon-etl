# Pokemon ETL Pipeline


The goal of this project is to develop an ETL pipeline using AWS Infrastructure, 
we will pull the Pokemon data as a request to an API, then we will clean and 
transform the data and store it in AWS Glue Data Catalog, finally, we will load 
it into an table in Snowflake. All of these will be done using Python and Spark shell in AWS.

Url for Pokemon data: [Pokemon API](https://raw.githubusercontent.com/ClaviHaze/CDMX009-Data-Lovers/master/src/data/pokemon/pokemon.json)

To orchestrate this process, we will use AWS Step Functions where we will have 3 steps (each of them a glue job) to manage de data:

· Data ingestion
· Data Transform
· Data Load
