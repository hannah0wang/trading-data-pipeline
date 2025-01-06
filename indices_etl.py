import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, explode_outer, concat, when
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
            StructField('Code', StringType(), True),
            StructField('Name', StringType(), True),
            StructField('Type', StringType(), True),
            StructField('ISIN', StringType(), True),
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
    urls = [
        Row(f'https://eodhd.com/api/fundamentals/{index_symbol}?api_token={config.EOD_API_TOKEN}&fmt=json')
        for index_symbol in indices_symbols
    ]
    api_df = spark.createDataFrame(urls, SCHEMA_URL)
    udf_api_call = udf(rest_api_get, schema)
    df_data = api_df.withColumn('data', udf_api_call(col('url')))

    # Extract index data
    index_data = df_data.select(
        concat(col('data.General.Code'), lit('.INDX')).alias('symbol'),
        col('data.General.Name').alias('name'),
        col('data.General.ISIN').alias('isin'),
        lit(None).alias('sector'),  # Add missing columns for alignment
        lit(None).alias('industry'),
        lit(None).alias('exchange'),
        lit('INDEX').alias('type'),
    )

    # Explode stock components
    component_data = df_data.select(
        concat(col('data.General.Code'), lit('.INDX')).alias('index_symbol'),
        explode_outer('data.Components').alias('key', 'value')
    ).select(
        col('index_symbol'),  # Retain index_symbol in output
        col('value.Code').alias('symbol'),
        col('value.Name').alias('name'),
        col('value.Sector').alias('sector'),
        col('value.Industry').alias('industry'),
        col('value.Exchange').alias('exchange'),
        lit('STOCK').alias('type'),
        lit(None).alias('isin')
    )

    # Align component_data with index_data by adding missing columns
    component_data = component_data.withColumn('index_symbol', col('index_symbol'))
    combined_data = index_data.unionByName(component_data, allowMissingColumns=True) # Combine
    return combined_data



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

    # Distinguish types
    df = df.withColumn(
        'type',
        when(col('type') == 'INDEX', lit('INDEX')).otherwise(lit('STOCK'))
    )

    # Format symbol for stocks and indexes
    df = df.withColumn(
        'symbol',
        when(col('type') == 'STOCK', concat(col('symbol'), lit('.'), col('exchange_symbol')))
        .otherwise(col('symbol'))
    )

    # Keep index_symbol as is for stocks
    df = df.withColumn(
        'index_symbol',
        when(col('type') == 'STOCK', col('index_symbol'))
        .otherwise(lit(None))
    )

    df = df.drop('exchange_symbol')

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

    indexes_df = spark.createDataFrame(pd.read_csv(config.INDICES_CSV_DRIVE_LINK)[['symbol']].drop_duplicates())
    instruments_df = new_df.select('component_symbol', 'name', 'type', 'isin').withColumnRenamed('component_symbol', 'symbol')
    indexes_components_df = new_df.select('index_symbol', 'component_symbol')
    indexes_components_df = indexes_components_df.filter(
        (indexes_components_df.index_symbol.isNotNull()) & (indexes_components_df.component_symbol.isNotNull())
    )
    
    # debugging
    # instruments_df.show()
    # indexes_df.show()
    # indexes_components_df.show()

    instruments_df.write.jdbc(config.get_jdbc_url(), 'instruments', mode='append', properties=JDBC_PROPERTIES)
    indexes_df.write.jdbc(config.get_jdbc_url(), 'indexes', mode='append', properties=JDBC_PROPERTIES)
    indexes_components_df.write.jdbc(config.get_jdbc_url(), 'indexes_components', mode='append', properties=JDBC_PROPERTIES)

load(transform(extract()))

spark.stop()