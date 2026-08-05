import os
import time
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# ==============================================================
# GENERAL CONFIGURATION
# ==============================================================
PROJECT_ID = "technical-assessment-504501"  
BUCKET_NAME = "alchemialabs-tech-assessment"
DATASET_ID = "alchemia_dataset"

def automate_gcs_to_bigquery():
    # 1. Initialize Google Cloud clients
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 2. Automatically create the dataset if it does not exist
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"✔ Dataset '{DATASET_ID}' successfully created.")
    except Conflict:
        print(f"ℹ Dataset '{DATASET_ID}' already exists. Proceeding...")

    # 3. List all files within the GCS bucket
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs()) # Materialize list to ensure it's not empty
    
    if not blobs:
        print(f"❌ No files found in bucket gs://{BUCKET_NAME}. Exiting.")
        return

    # Dictionary structure to group URIs by target table
    files_by_table = defaultdict(list)
    
    print(f"\nClassifying files in gs://{BUCKET_NAME}...")

    # 4. Iterate over files and group them into their strategic target tables
    for blob in blobs:
        if blob.name.endswith('/'):
            continue
            
        file_name = os.path.basename(blob.name)
        _, extension = os.path.splitext(file_name)
        clean_ext = extension.lower().replace(".", "")
        uri_file = f"gs://{BUCKET_NAME}/{blob.name}"
        
        # Intelligent routing to unified consolidated tables
        if "crm_accounts" in blob.name:
            table_name = "crm_accounts"
        elif "crm_contacts" in blob.name:
            table_name = "crm_contacts"
        elif "crm_opportunities" in blob.name or "opportunity" in blob.name.lower():
            table_name = "crm_opportunities"
        else:
            # Standalone tables outside of the primary CRM structures
            clean_name = blob.name.replace("/", "_")
            table_name, _ = os.path.splitext(clean_name)
            table_name = table_name.replace("-", "_").replace(" ", "_").replace(".", "_")
            
        files_by_table[table_name].append((uri_file, clean_ext))

    # ==============================================================
    # 5. BATCH PROCESSING (One Load Job per Destination Table)
    # ==============================================================
    for table_name, file_list in files_by_table.items():
        print(f"\n--- Starting batch for destination table: {table_name} ---")
        table_ref = dataset_ref.table(table_name)
        
        # FIX: Extract individual strings out of the tuple dictionary
        uris = [item[0] for item in file_list]
        actual_ext = file_list[0][1] # Safely grabs the string extension (e.g., 'csv')
        
        explicit_schema = None
        
        # ==============================================================
        # REAL EXPECTED SCHEMA FOR CRM_ACCOUNTS
        # ==============================================================
        if table_name == "crm_accounts":
            explicit_schema = [
                bigquery.SchemaField("account_name", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("store_number", "STRING"),
                bigquery.SchemaField("segment", "STRING"),
                bigquery.SchemaField("account_owner", "STRING"),
                bigquery.SchemaField("contract_start_date", "STRING"),
                bigquery.SchemaField("annual_target_gbp", "STRING"),
                bigquery.SchemaField("last_activity_date", "STRING"),
                bigquery.SchemaField("created_date", "STRING"),
                bigquery.SchemaField("status", "STRING")
            ]
        # ==============================================================
        # REAL EXPECTED SCHEMA FOR CRM_CONTACTS
        # ==============================================================
        elif table_name == "crm_contacts":
            explicit_schema = [
                bigquery.SchemaField("contact_id", "STRING"),
                bigquery.SchemaField("account_id", "STRING"),
                bigquery.SchemaField("first_name", "STRING"),
                bigquery.SchemaField("last_name", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("linkedin_url", "STRING")
            ]
        # ==============================================================
        # REAL EXPECTED SCHEMA FOR CRM_OPPORTUNITIES
        # ==============================================================
        elif table_name == "crm_opportunities":
            explicit_schema = [
                bigquery.SchemaField("opportunity_id", "STRING"),
                bigquery.SchemaField("account_id", "STRING"),
                bigquery.SchemaField("product_sku", "STRING"),
                bigquery.SchemaField("stage", "STRING"),
                bigquery.SchemaField("value_gbp", "STRING"),
                bigquery.SchemaField("owner", "STRING"),
                bigquery.SchemaField("expected_close_date", "STRING"),
                bigquery.SchemaField("last_activity_date", "STRING"),
                bigquery.SchemaField("created_date", "STRING"),
                bigquery.SchemaField("close_date", "STRING"),
                bigquery.SchemaField("type", "STRING")
            ]

        # Dynamic mapping of the ingestion engine configurations
        if actual_ext == "csv":
            is_combined_table = table_name in ["crm_accounts", "crm_contacts", "crm_opportunities"]
            write_mode = bigquery.WriteDisposition.WRITE_APPEND if is_combined_table else bigquery.WriteDisposition.WRITE_TRUNCATE
            
            # Only skip the header row if processing crm_contacts
            rows_to_skip = 1 if table_name == "crm_contacts" else 0
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=rows_to_skip, 
                max_bad_records=50000,       
                allow_quoted_newlines=True,  
                ignore_unknown_values=True,  
                write_disposition=write_mode
            )
            
            if explicit_schema:
                job_config.schema = explicit_schema
                job_config.autodetect = False
            else:
                job_config.autodetect = True
                
        elif actual_ext == "parquet":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif actual_ext == "json":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif actual_ext == "avro":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.AVRO,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        else:
            print(f"⚠ Format .{actual_ext} is not natively supported. Skipping batch {table_name}...\n")
            continue

        # Send the bundled load jobs to the BigQuery API
        try:
            print(f"Submitting {len(uris)} files grouped together into a single Job for '{table_name}'...")
            load_job = bq_client.load_table_from_uri(
                uris,  
                table_ref, 
                job_config=job_config
            )
            load_job.result()  
            
            created_table = bq_client.get_table(table_ref)
            print(f"✔ Target Table '{table_name}' processed successfully. Current row count: {created_table.num_rows}\n")
            
            time.sleep(1.5)
            
        except Exception as e:
            print(f"❌ Critical error loading file batch for table {table_name}: {e}\n")

    print("====== All bucket files have been successfully batched and processed. ======")

if __name__ == "__main__":
    automate_gcs_to_bigquery()

if __name__ == "__main__":
    automate_gcs_to_bigquery()
