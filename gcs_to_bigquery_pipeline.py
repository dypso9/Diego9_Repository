import os
import time
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# ==============================================================
# GENERAL CONFIGURATION
# ==============================================================
#========
# These three constants define *where* everything happens:
# - PROJECT_ID: the GCP project that owns both the bucket and the BigQuery dataset
# - BUCKET_NAME: the Cloud Storage bucket that contains the raw source files
# - DATASET_ID: the BigQuery dataset that will receive the loaded tables
PROJECT_ID = "technical-assessment-504501"  
BUCKET_NAME = "alchemialabs-tech-assessment"
DATASET_ID = "alchemia_dataset"

#End-to-end pipeline that:
      #1. Connects to GCS and BigQuery.
      #2. Ensures the target BigQuery dataset exists.
      #3. Scans every file in the bucket and decides which BigQuery tableit belongs to (grouping related files together).
def automate_gcs_to_bigquery():
    # 1. Initialize Google Cloud clients
    # storage_client talks to Cloud Storage (to list files in the bucket):
    storage_client = storage.Client(project=PROJECT_ID)
     # bq_client talks to BigQuery (to create datasets/tables and run load jobs):
    bq_client = bigquery.Client(project=PROJECT_ID)

    # ----------------------------------------------------------------
    # 2. Automatically create the dataset if it does not exist
    # ----------------------------------------------------------------
    # dataset_ref is just a lightweight pointer/reference to the dataset:
    # (project + dataset name) — it doesn't fetch or create anything by itself.
    dataset_ref = bq_client.dataset(DATASET_ID)
    # Try to actually create the dataset in BigQuery.
    # BigQuery raises a Conflict (HTTP 409) if the dataset already exists.
    # That's fine here — we just want to make sure it exists, so we
    # catch the error and continue instead of crashing.
    try:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"✔ Dataset '{DATASET_ID}' successfully created.")
    except Conflict:
        print(f"ℹ Dataset '{DATASET_ID}' already exists. Proceeding...")

    # ----------------------------------------------------------------
    # 3. List all files within the GCS bucket
    # ----------------------------------------------------------------
    bucket = storage_client.bucket(BUCKET_NAME)
     # list_blobs() returns a lazy iterator, so we convert it to a list
    # ("materialize" it) up front. This lets us check right away whether
    # the bucket is empty, and lets us iterate over it multiple times if needed.
    blobs = list(bucket.list_blobs()) # Materialize list to ensure it's not empty
    
    if not blobs:
          # Nothing to do if the bucket has no files — exit early.
        print(f"❌ No files found in bucket gs://{BUCKET_NAME}. Exiting.")
        return

    # Dictionary structure to group URIs by target table
    # files_by_table will map: table_name -> list of (file_uri, extension) tuples.
    # defaultdict(list) means we can do files_by_table[key].append(...) without
    # having to first check whether "key" already exists.
    files_by_table = defaultdict(list)
    
    print(f"\nClassifying files in gs://{BUCKET_NAME}...")

   # ----------------------------------------------------------------
   # 4. Iterate over files and group them into their target tables
   # ----------------------------------------------------------------
    for blob in blobs:
        # Objects whose name ends in "/" are "folder placeholders" in GCS,
        # not real files — skip them.
        if blob.name.endswith('/'):
            continue

        # Extract just the filename (no folder path), then split it into
        # name + extension, e.g. "crm_accounts_2024.csv" -> (".csv")
        file_name = os.path.basename(blob.name)
        _, extension = os.path.splitext(file_name)
        # Normalize the extension: lowercase, and strip the leading dot
        # so "CSV"/"csv"/".CSV" all become "csv":
        clean_ext = extension.lower().replace(".", "")
        # Build the full gs:// URI that BigQuery's load job will use to read this file.
        uri_file = f"gs://{BUCKET_NAME}/{blob.name}"
        
        # Intelligent routing to unified consolidated tables
         # --- Routing logic: decide which BigQuery table this file feeds into ---
        # Files whose path/name contains these keywords are consolidated into
        # one of three "known" CRM tables, regardless of which specific file
        # or folder they came from (e.g. multiple CSVs from different dates
        # all land in the same "crm_accounts" table).
        if "crm_accounts" in blob.name:
            table_name = "crm_accounts"
        elif "crm_contacts" in blob.name:
            table_name = "crm_contacts"
        elif "crm_opportunities" in blob.name or "opportunity" in blob.name.lower():
            table_name = "crm_opportunities"
        else:
            # Standalone tables outside of the primary CRM structures
            # Anything that doesn't match a known CRM pattern gets its own
            # standalone table, named after its file path.
            # Example: "misc/product_catalog.csv" -> "misc_product_catalog"
            clean_name = blob.name.replace("/", "_")
            table_name, _ = os.path.splitext(clean_name)
            # BigQuery table names can't contain dashes, spaces, or dots,
            # so replace those with underscores to get a valid identifier.
            table_name = table_name.replace("-", "_").replace(" ", "_").replace(".", "_")
            
        files_by_table[table_name].append((uri_file, clean_ext))

    # ==============================================================
    # 5. BATCH PROCESSING (One Load Job per Destination Table)
    # ==============================================================
    for table_name, file_list in files_by_table.items():
        print(f"\n--- Starting batch for destination table: {table_name} ---")
        # Reference to the specific destination table (doesn't create it yet;
        # the load job itself will create/append to it).
        table_ref = dataset_ref.table(table_name)
        
        # FIX: Extract individual strings out of the tuple dictionary
        # file_list is a list of (uri, extension) tuples. Pull out just the
        # URIs for the load job..
        uris = [item[0] for item in file_list]
        # ...and grab the extension from the first file. This assumes every
        # file grouped under the same table shares the same format/extension.
        actual_ext = file_list[0][1] # Safely grabs the string extension (e.g., 'csv')
        
        explicit_schema = None
        
        # ==============================================================
        # REAL EXPECTED SCHEMA FOR CRM_ACCOUNTS
        # ==============================================================
        # Now that files are grouped by destination table, loop over each group
        # and issue a single BigQuery load job per table (loading all its files
        # together is more efficient than one job per file).
        if table_name == "crm_accounts":
            explicit_schema = [
                bigquery.SchemaField("account_id", "STRING"),
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

        # (If table_name doesn't match any of the three above, explicit_schema
        # stays None, and BigQuery will auto-detect the schema instead — see
        # the format-specific branches below.)
 
        # ----------------------------------------------------------------
        # Dynamic mapping of the ingestion engine configuration, based on
        # file extension. Each branch builds a LoadJobConfig telling
        # BigQuery how to interpret and load the files.
        # ----------------------------------------------------------------

        # Dynamic mapping of the ingestion engine configurations
        if actual_ext == "csv":
         # The three "known" CRM tables are meant to accumulate rows from
            # multiple files/loads over time, so they use WRITE_APPEND
            # (add new rows without deleting old ones). Any other/standalone
            # table uses WRITE_TRUNCATE (wipe and replace) since it's assumed
            # to be a full, self-contained snapshot each time.
            is_combined_table = table_name in ["crm_accounts", "crm_contacts", "crm_opportunities"]
            write_mode = bigquery.WriteDisposition.WRITE_APPEND if is_combined_table else bigquery.WriteDisposition.WRITE_TRUNCATE
            # Only the crm_contacts files are assumed to have a header row
            # that needs to be skipped; other CSVs are assumed headerless
            # (or their header handling is managed differently)
            
            # Only skip the header row if processing crm_contacts
            rows_to_skip = 1 if table_name == "crm_contacts" else 0
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=rows_to_skip, 
                # Tolerate up to 50,000 malformed rows per load instead of failing outright:
                max_bad_records=50000,       
                # Allow newline characters inside quoted CSV fields (e.g. multi-line addresses).:
                # Silently drop any extra columns not present in the schema, rather than erroring.:
                allow_quoted_newlines=True,  
                ignore_unknown_values=True,  
                write_disposition=write_mode
            )
            
            if explicit_schema:
                # Use the hand-defined schema and turn off autodetection:
                job_config.schema = explicit_schema
                job_config.autodetect = False
            else:
                # No explicit schema defined (standalone table) — let
                # BigQuery infer column names/types from the file contents.
                job_config.autodetect = True
                
        elif actual_ext == "parquet":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif actual_ext == "json":
             # Expects newline-delimited JSON (one JSON object per line).
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif actual_ext == "avro":
             # Avro files, like Parquet, carry their own embedded schema.
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.AVRO,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        else:
            # Any other file extension (e.g. .txt, .xlsx) isn't handled by
            # this pipeline — skip this table's batch entirely and move on
            # to the next one.
            print(f"⚠ Format .{actual_ext} is not natively supported. Skipping batch {table_name}...\n")
            continue

        # ----------------------------------------------------------------
        # Send the bundled load job(s) to the BigQuery API
        # ----------------------------------------------------------------
        try:
            print(f"Submitting {len(uris)} files grouped together into a single Job for '{table_name}'...")
            # load_table_from_uri() can accept a list of URIs and load them
            # all as part of one job, which is why files were grouped earlier.
            load_job = bq_client.load_table_from_uri(
                uris,  
                table_ref, 
                job_config=job_config
            )
            # .result() blocks execution until the load job finishes
            # (success or failure) — this makes the loop synchronous,
            # processing one table fully before moving to the next.
            load_job.result()  
            # After a successful load, fetch the table's current metadata
            # to report how many rows it now contains (useful for sanity-
            # checking that data actually landed).
            
            created_table = bq_client.get_table(table_ref)
            print(f"✔ Target Table '{table_name}' processed successfully. Current row count: {created_table.num_rows}\n")

            # Small pause between jobs — likely to avoid hitting BigQuery
            # API rate limits when processing many tables back-to-back.
            time.sleep(1.5)
            
        except Exception as e:
            # Catch-all: if anything goes wrong with this table's load job
            # (bad data, quota errors, schema mismatch, etc.), report it but
            # keep going so one bad table doesn't stop the whole pipeline.
            print(f"❌ Critical error loading file batch for table {table_name}: {e}\n")

    print("====== All bucket files have been successfully batched and processed. ======")

if __name__ == "__main__":
    # Only run the pipeline when this script is executed directly
    # (not when it's imported as a module elsewhere).
    automate_gcs_to_bigquery()
