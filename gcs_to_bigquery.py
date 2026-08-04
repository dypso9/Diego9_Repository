import os
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# CONFIGURATION
PROJECT_ID = "technical-assessment-504501"  # Your verified GCP Project ID
BUCKET_NAME = "alchemialabs-tech-assessment"
DATASET_ID = "alchemia_dataset"

def automate_gcs_to_bigquery():
    # 1. Initialize Google Cloud clients
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 2. Automatically create the dataset if it does not exist.
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"✔ Dataset '{DATASET_ID}' successfully created.")
    except Conflict:
        print(f"ℹ Dataset '{DATASET_ID}' already exists. Proceeding...")

    # 3. List all files within the bucket
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    
    print(f"Processing files in gs://{BUCKET_NAME}...\n")

    # 4. Iterate over each file and create/append to its respective target table.
    for blob in blobs:
        # Ignore empty virtual directories, if any.
        if blob.name.endswith('/'):
            continue
            
        print(f"--- Processing file: {blob.name} ---")
        
        file_name = os.path.basename(blob.name)
        _, extension = os.path.splitext(file_name)
        clean_ext = extension.lower().replace(".", "")
        
        # Define the full URI path for the source file
        uri_file = f"gs://{BUCKET_NAME}/{blob.name}"
        
        # ==============================================================
        # CRITICAL UPDATE: FILE IDENTIFICATION & TARGET TABLE ROUTING
        # ==============================================================
        # Base schema fallback variables
        explicit_schema = None
        
        if "crm_accounts" in blob.name:
            # Route all variations of accounts into a single historical destination table
            table_name = "crm_accounts"
            # Explicitly enforce string schema to bypass mismatched column positions and data corruption
            explicit_schema = [
                bigquery.SchemaField("account_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("industry", "STRING"),
                bigquery.SchemaField("annual_revenue", "STRING"),
                bigquery.SchemaField("employee_count", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("expected_close_date", "STRING"),
                bigquery.SchemaField("deal_stage", "STRING"),
                bigquery.SchemaField("assigned_owner", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("updated_at", "STRING")
            ]
        elif "crm_contacts" in blob.name:
            # Route all variations of contacts into its own historical destination table
            table_name = "crm_contacts"
            # Fallback 8-column layout using safe data types for text data containing bad commas
            explicit_schema = [
                bigquery.SchemaField("col1", "STRING"), bigquery.SchemaField("col2", "STRING"),
                bigquery.SchemaField("col3", "STRING"), bigquery.SchemaField("col4", "STRING"),
                bigquery.SchemaField("col5", "STRING"), bigquery.SchemaField("col6", "STRING"),
                bigquery.SchemaField("col7", "STRING"), bigquery.SchemaField("col8", "STRING")
            ]
        else:
            # Standard treatment for clean files outside of the corrupted CRM subfolders
            clean_name = blob.name.replace("/", "_")
            table_name, _ = os.path.splitext(clean_name)
            table_name = table_name.replace("-", "_").replace(" ", "_").replace(".", "_")

        table_ref = dataset_ref.table(table_name)
        
        # ==============================================================
        # DYNAMIC CONFIGURATION BASED ON FILE EXTENSION & SCHEMA RULES
        # ==============================================================
        if clean_ext == "csv":
            source_format = bigquery.SourceFormat.CSV
            
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                max_bad_records=50000,       # High tolerance limit to bypass bad rows
                allow_quoted_newlines=True,  # Handles text blocks containing multi-line values
                ignore_unknown_values=True,  # Discards the 12th extra column in an 11-column table layout
                # Appends data since multiple daily crm dumps belong to the same entities
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND if explicit_schema else bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            if explicit_schema:
                job_config.schema = explicit_schema
                job_config.autodetect = False
            else:
                job_config.autodetect = True
                
        elif clean_ext == "parquet":
            source_format = bigquery.SourceFormat.PARQUET
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif clean_ext == "json":
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif clean_ext == "avro":
            source_format = bigquery.SourceFormat.AVRO
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        else:
            print(f"⚠ Format .{clean_ext} not automatically supported for '{blob.name}'. Skipping...\n")
            continue

        # Perform a direct load into BigQuery
        try:
            load_job = bq_client.load_table_from_uri(
                uri_file, 
                table_ref, 
                job_config=job_config
            )
            load_job.result()  # Wait for the loading job to complete.
            
            # Confirmation of the created table status
            created_table = bq_client.get_table(table_ref)
            print(f"✔ Target Table '{table_name}' processed. Current row count: {created_table.num_rows}\n")
        except Exception as e:
            # Loop continues safely if an unexpected underlying data breakdown happens
            print(f"❌ Skipped/Failed file {blob.name} due to format errors: {e}\n")

    print("====== The entire bucket has been processed. ======")

if __name__ == "__main__":
    automate_gcs_to_bigquery()
