from google.cloud import bigquery
from google.oauth2 import service_account

# Path to your downloaded key file (replace the name if needed)
key_path = "C:\Users\sharm\Downloads\ace-agility-457703-q3-01f538b73865.json"

# Create credentials
credentials = service_account.Credentials.from_service_account_file(key_path)

# Create a BigQuery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# (Optional) Test query to validate connection
query_job = client.query("SELECT CURRENT_DATE() as today;")
results = query_job.result()

for row in results:
    print(f"Today's date from BigQuery is: {row.today}")
