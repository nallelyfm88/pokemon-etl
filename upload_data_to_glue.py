import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import requests


def main():
    spark = SparkSession \
        .builder \
        .appName("EMR Spark Glue Example") \
        .config("hive.metastore.connect.retries", 5) \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()
    write_db(spark)


def read_df(spark):
    df = spark.read.json(
        's3://aws-glue-pokemon-data-nallely/data/part-00079-fd9b37c5-0981-4c37-a1b0-1d4f561547d1-c000.json')
    return df


def transform_data(spark):
    df = read_df(spark).withColumn('all_pokemon', f.explode('pokemon')).select('all_pokemon.*')
    df = df \
        .withColumn('multipliers', f.array_join('multipliers', ' ,')) \
        .withColumn('type', f.array_join('type', ' | ')) \
        .withColumn('weaknesses', f.array_join('weaknesses', ' | ')) \
        .withColumn('next_evolution', f.array_join('next_evolution.name', ' ,')) \
        .withColumn('prev_evolution', f.array_join('prev_evolution.name', ' ,')) \
        .withColumn('height', f.regexp_replace('height', 'm', '')) \
        .withColumn('weight', f.regexp_replace('weight', 'kg', ''))
    # Add a column for the calculation of the BMI (Body Mass Index) of the Pokemon
    df = df.withColumn('BMI', f.round(f.column('weight') / f.column("height"), 2))
    # Add a column that displays the obesity label for each Pokemon taking into account the column BM
    df = df.withColumn('Obesity', f.when(f.column('BMI') < 18.5, "Underweight")
                       .when((f.column('BMI') >= 18.5) & (f.column('BMI') <= 24.9), "Normal weight")
                       .when((f.column('BMI') >= 25) & (f.column('BMI') <= 29.9), "Overweight")
                       .when(f.column('BMI') > 30, "Obesity"))
    # Filter the Pokemons to only keep the ones with an average spawn between 1 and 220.
    df = df.filter((df["avg_spawns"] >= 1) & (df["avg_spawns"] <= 220)).select('*')
    # Filter the Pokemons to show everyone but the one’s which eggs have “Not in Eggs”
    df = df.filter(df["egg"] != "Not in Eggs")
    # Update the column name to display the name of the Pokemon + your last name
    df = df.withColumn('name', f.concat('name', f.lit(' Flores')))
    # Remove img, num, and candy_count columns
    df = df.drop('img', 'num', 'candy_count')
    return df


def write_db(spark):
    data = transform_data(spark)
    table_exist = spark.sql('show tables in pokemon_nallely').where(f.col('tableName') == 'pokemon_data2').count() == 1
    if table_exist == True:
        data.write.insertInto('pokemon_nallely.pokemon_data2', overwrite=True)
    else:
        data.write.saveAsTable('pokemon_nallely.pokemon_data2', path='s3://aws-glue-pokemon-data-nallely/prepared')


if __name__ == "__main__":
    main()
