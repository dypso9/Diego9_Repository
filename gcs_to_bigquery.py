import os
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# CONFIGURATION
PROJECT_ID = "technical-assessment-504501"  # Replace with your GCP project ID.
BUCKET_NAME = "alchemialabs-tech-assessment"
DATASET_ID = "alchemia_dataset"

def automatizar_gcs_a_bigquery():
    # 1. Initialize Google Cloud clients
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 2. Automatically create the dataset if it does not exist.
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"✔ Dataset '{DATASET_ID}' creado exitosamente.")
    except Conflict:
        print(f"ℹ El dataset '{DATASET_ID}' ya existe. Continuando...")

    # 3. List all files within the bucket
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    
    print(f"Processing files in gs://{BUCKET_NAME}...")

    # 4. Iterate over each file and create its respective table.
    for blob in blobs:
        # Ignore empty virtual directories, if any.
        if blob.name.endswith('/'):
            continue
            
        print(f"\n--- Procesando archivo: {blob.name} ---")
        
       # Generate a clean table name based on the filename
        # Example: "user_data.csv" -> "user_data"
        file_name = os.path.basename(blob.name)
        file_name, extension = os.path.splitext(nombre_archivo)
        
        # Clean disallowed characters from BigQuery table names
        table_name = table_name.replace("-", "_").replace(" ", "_")
        
        # Define the full path for the file and the destination table.
        uri_archivo = f"gs://{BUCKET_NAME}/{blob.name}"
        table_ref = dataset_ref.table(table_name)
        
        # Detect the format based on the file extension.
        ext_limpia = extension.lower().replace(".", "")
        if ext_limpia == "csv":
            formato = bigquery.SourceFormat.CSV
        elif ext_limpia == "json":
            formato = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        elif ext_limpia in ["parquet", "avro"]:
            formato = bigquery.SourceFormat.PARQUET if ext_limpia == "parquet" else bigquery.SourceFormat.AVRO
        else:
            print(f"⚠ Formato .{ext_limpia} no soportado automáticamente para '{blob.name}'. Saltando...")
            continue

        # Configure the automatic creation of the table and its schema.
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Creates columns and automatically detects data types (Int, String, etc.).
            source_format=formato,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Replaces data if the table already existed.
        )
        
        # Perform a direct load into BigQuery
        try:
            load_job = bq_client.load_table_from_uri(
                uri_archivo, 
                table_ref, 
                job_config=job_config
            )
            load_job.result() # Wait for the loading to finish.
            
            # Confirmation of the created table
            tabla_creada = bq_client.get_table(table_ref)
            print(f"✔ Table '{}' created/updated with {tabla_creada.num_rows} filas.")
        except Exception as e:
            print(f"❌ Error {blob.name}: {e}")

    print("\n====== The entire bucket has been processed. ======")

if __name__ == "__main__":
    # Remember to run 'gcloud auth application-default login' before running it.
    automatizar_gcs_a_bigquery()
