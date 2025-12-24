from google.cloud import bigquery

def create_bigquery_table():
    client = bigquery.Client()

    project_id = "ace-agility-457703-q3"
    dataset_id = "smart_city"
    table_id = "traffic_data"

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Define schema based on your CSV columns
    schema = [
        bigquery.SchemaField("City", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Vehicle_Type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Weather", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Economic_Condition", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Day_Of_Week", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Hour_Of_Day", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Speed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("Is_Peak_Hour", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("Random_Event_Occurred", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("Energy_Consumption", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("Traffic_Density", "FLOAT", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_ref, schema=schema)

    # Create the table (skip creation if exists)
    table = client.create_table(table, exists_ok=True)
    print(f"✅ Table {table.project}.{table.dataset_id}.{table.table_id} created successfully.")

# Call the function
create_bigquery_table()



