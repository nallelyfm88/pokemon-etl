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
    create_df()


def get_data():
    req = requests.get(
        'https://raw.githubusercontent.com/ClaviHaze/CDMX009-Data-Lovers/master/src/data/pokemon/pokemon.json')
    data = req.text
    return data


def create_df():
    spark = SparkSession \
        .builder \
        .appName("Pokemon") \
        .getOrCreate()
    sc = spark.sparkContext
    df = sc.parallelize([get_data()])
    df = spark.read.json(df)
    df.write.format('json').mode('overwrite').save('s3://aws-glue-pokemon-data-nallely/data/')


if __name__ == "__main__":
    main()