EOD_API_TOKEN = "<eod_api_token>"
INDICES_CSV_DRIVE_LINK = "https://docs.google.com/spreadsheets/d/<file_id>/gviz/tq?tqx=out:csv"
DB_USER = "<username>"
DB_PASSWORD = "<password>"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "trading_data"

# generate JDBC URL
def get_jdbc_url():
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"