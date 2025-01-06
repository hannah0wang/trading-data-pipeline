from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, concat, to_date, when
from pyspark.sql.types import StructType, StructField, StringType, Row, IntegerType, DoubleType, DateType
import requests
import re
import psycopg2
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

def get_postgres_connection():
    return psycopg2.connect(
        host=config.DB_HOST,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )

def extract():
    schema = StructType([
        StructField('General', StructType([
            StructField('Code', StringType(), True),
            StructField('Exchange', StringType(), True),
            StructField('IPODate', StringType(), True), 
            StructField('CountryISO', StringType(), True),
            StructField('Type', StringType(), True),
            StructField('Sector', StringType(), True),
            StructField('Industry', StringType(), True),
            StructField('GicSector', StringType(), True),
            StructField('GicGroup', StringType(), True), 
            StructField('GicIndustry', StringType(), True),
            StructField('GicSubIndustry', StringType(), True),
            StructField('Description', StringType(), True),
            StructField('Address', StringType(), True),
            StructField('WebURL', StringType(), True), 
            StructField('LogoURL', StringType(), True),
            StructField('FullTimeEmployees', IntegerType(), True)])),
        StructField('SharesStats', StructType([
            StructField('SharesOutstanding', IntegerType(), True),
            StructField('SharesFloat', IntegerType(), True),
            StructField('PercentInsiders', DoubleType(), True),
            StructField('PercentInstitutions', DoubleType(), True),
        ]))
    ])

    # Get data to extract
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT i.symbol FROM instruments i WHERE i.type='STOCK' AND i.symbol NOT IN (SELECT symbol FROM stocks)")
    symbols = [r[0] for r in cursor.fetchall()]
    urls = []
    for symbol in symbols:
        urls.append(
            Row(f"https://eodhd.com/api/fundamentals/{symbol}?filter=General,SharesStats&api_token={config.EOD_API_TOKEN}&fmt=json"))
    api_df = spark.createDataFrame(urls, SCHEMA_URL)
    udf_api_call = udf(rest_api_get, schema)
    df_data = api_df.withColumn('data', udf_api_call(col('url')))  # create a column data with response of rest api
    df_data = df_data.select('data.*', '*').drop('url', 'col', 'data')
    df_data = df_data.select('*', 'General.*', 'SharesStats.*').drop('General', 'SharesStats')
    return df_data


EOD_HD_API_US_SPECIFIC_EXCHANGES = ['NYSE', 'NASDAQ', 'BATS', 'AMEX']
EOD_HD_API_US_EXCHANGE_GROUP = 'US'

def transform(df_data):
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
    stocks_dict = {
        "code": "symbol",
        "exchange": "exchange_symbol",
        "full_time_employees": "employees",
    }
    df = df_data.toDF(*[snake_case(c) for c in df_data.columns])
    df = rename_cols(df, stocks_dict)
    df.show()
    df = df.filter(df.type == 'Common Stock')
    df = df.withColumn('ipo_date', to_date(df['ipo_date'], 'yyyy-MM-dd').cast(DateType()))
    df = df.withColumn('logo_url', concat(lit('https://eodhistoricaldata.com'), col('logo_url')))
    df = df.withColumn('exchange_symbol', when(col('exchange_symbol').isin(EOD_HD_API_US_SPECIFIC_EXCHANGES), EOD_HD_API_US_EXCHANGE_GROUP).otherwise(col('exchange_symbol')))
    df = df.withColumn('symbol', concat(col('symbol'), lit('.'), col('exchange_symbol')))
    df = df.drop('exchange_symbol', 'type')
    df = df.na.fill(value=0.0, subset=['percent_insiders', 'percent_institutions'])
    return df

def load(df):
    JDBC_PROPERTIES = {
        "user": config.DB_USER,
        "password": config.DB_PASSWORD,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified" # Added so we can use database type enums
    }
    
    stocks_db_df = spark.read.jdbc(url=config.get_jdbc_url(), table='stocks', properties=JDBC_PROPERTIES)


    if df.count() > 0:
        # mode append to truncate all and recreate
        df.write.jdbc(config.get_jdbc_url(), 'stocks', mode='append', properties=JDBC_PROPERTIES)

# # EXTRACT TESTING
# df_extract = extract()
# df_extract.show(10, truncate=15)

# # TRANSFORM TESTING
# df_transform = transform(df_extract)
# df_transform.show(10, truncate=15)
# df_transform.printSchema()

load(transform(extract()))

spark.stop()