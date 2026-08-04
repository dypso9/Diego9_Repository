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

    # 4. Iterate over each file and create its respective table.
    for blob in blobs:
        # Ignore empty virtual directories, if any.
        if blob.name.endswith('/'):
            continue
            
        print(f"--- Processing file: {blob.name} ---")
        
        # Generate a clean table name based on the filename (and preserve path context)
        # Replacing slashes avoids name collisions if files share names in different folders
        clean_name = blob.name.replace("/", "_")
        table_name, extension = os.path.splitext(clean_name)
        
        # Clean disallowed characters from BigQuery table names
        table_name = table_name.replace("-", "_").replace(" ", "_").replace(".", "_")
        
        # Define the full path for the file and the destination table.
        uri_file = f"gs://{BUCKET_NAME}/{blob.name}"
        table_ref = dataset_ref.table(table_name)
        
        # Detect the format based on the file extension.
        clean_ext = extension.lower().replace(".", "")
        
        # ==============================================================
        # DYNAMIC CONFIGURATION BASED ON FILE EXTENSION
        # ==============================================================
        if clean_ext == "csv":
            source_format = bigquery.SourceFormat.CSV
            # For CSV: Enable autodetect and allow ultra-high tolerance for bad rows
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                autodetect=True,
                max_bad_records=50000,  # <-- High tolerance to bypass massively corrupted crm files
                allow_quoted_newlines=True, # <-- Helps BigQuery parse text strings spanning multiple lines
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
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
            
            # Confirmation of the created table
            created_table = bq_client.get_table(table_ref)
            print(f"✔ Table '{table_name}' created/updated with {created_table.num_rows} rows.\n")
        except Exception as e:
            # By catching the error here, the loop does not break and will process the rest of the bucket
            print(f"❌ Skipped/Failed file {blob.name} due to format errors: {e}\n")

    print("====== The entire bucket has been processed. ======")

if __name__ == "__main__":
    automate_gcs_to_bigquery()
