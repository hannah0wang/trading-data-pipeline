import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, explode_outer, concat
from pyspark.sql.types import StructType, StructField, StringType, Row, MapType
import requests
import pandas as pd
import config

spark = SparkSession.builder \
    .appName("Trading Data Pipeline") \
    .config("spark.jars", "./lib/postgresql-42.7.4.jar") \
    .getOrCreate()

SCHEMA_URL = StructType([StructField('url', StringType())])
def rest_api_get(url):
    res = requests.get(url)
    if res is not None and res.status_code == 200:
        return res.json()
    return None

def extract():
    schema = StructType([
        StructField('General', StructType([
           StructField('Code', StringType(), True)
        ])),  
        StructField('Components', MapType(  
            StringType(),  
            StructType([  
                StructField('Code', StringType(), True),  
                StructField('Exchange', StringType(), True),  
                StructField('Name', StringType(), True),  
                StructField('Sector', StringType(), True),  
                StructField('Industry', StringType(), True),  
            ]),  
        ))  
    ])
    df = pd.read_csv(config.INDICES_CSV_DRIVE_LINK)
    indices_symbols = df['symbol'].tolist() 
    urls = []
    for index_symbol in indices_symbols:
        urls.append(
    Row(f'https://eodhd.com/api/fundamentals/{index_symbol}?api_token={config.EOD_API_TOKEN}&fmt=json'))
    api_df = spark.createDataFrame(urls, SCHEMA_URL)
    udf_api_call = udf(rest_api_get, schema)
    df_data = api_df.withColumn('data', udf_api_call(col('url')))
    df_data = df_data.select(col('data.General.Code').alias('index_symbol'), explode_outer('data.Components')).drop('data', 'key')
    df_data = df_data.select('value.*', '*').drop('value')
    return df_data

def transform(ex_comp_df):
    def snake_case(text): 
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    def rename_cols(df, mapping_dict):
        for key in mapping_dict.keys():
            df = df.withColumnRenamed(key, mapping_dict.get(key))
        return df
    cols_dict = {
        "code": "symbol",
        "exchange": "exchange_symbol",
    }
    df = ex_comp_df.toDF(*[snake_case(c) for c in ex_comp_df.columns])
    df = rename_cols(df, cols_dict)
    df = df.withColumn('type', lit('STOCK'))
    df = df.withColumn('symbol', concat(col('symbol'), lit('.'), col('exchange_symbol')))
    df = df.withColumn('index_symbol', concat(col('index_symbol'), lit('.INDX')))
    df = df.drop(col('exchange_symbol'))
    return df

def load(df):
    JDBC_PROPERTIES = {
        "user": config.DB_USER,
        "password": config.DB_PASSWORD,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified" # Added so we can use database type enums
    }

    indexes_components__db_df = spark.read.jdbc(url=config.get_jdbc_url(), table='indexes_components', properties=JDBC_PROPERTIES)

    new_df = df.withColumnRenamed('symbol', 'component_symbol').join(indexes_components__db_df, on=['index_symbol', 'component_symbol'], how='left_anti')
   
    instruments_df = new_df.select('component_symbol', 'name', 'type').withColumnRenamed('component_symbol', 'symbol')
    indexes_components_df = new_df.select('index_symbol', 'component_symbol')
    instruments_df.write.jdbc(config.get_jdbc_url(), 'instruments', properties=JDBC_PROPERTIES, mode="ignore")
    indexes_components_df.write.jdbc(config.get_jdbc_url(), 'indexes_components', properties=JDBC_PROPERTIES, mode="ignore")


# EXTRACT TESTING
df_extract = extract()
# df_data.printSchema()
df_extract.show(10, truncate=False)

# TRANSFORM TESTING
df_transform = transform(df_extract)
df_transform.show(10, truncate=False)

load(transform(extract()))

spark.stop()