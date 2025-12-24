from google.cloud import bigquery
import pandas as pd

def load_data_to_bq():
    client = bigquery.Client()
    table_id = "ace-agility-457703-q3.smart_city.traffic_data"
    
    df = pd.read_csv("data/transformed_traffic_data.csv")
    
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  
    
    print("Data Loaded to BigQuery")

