import os
from google.cloud import bigquery

def run_iowa_liquor_etl():
    # 1. Initialize the BigQuery client
    # Note: Ensure you have your GOOGLE_APPLICATION_CREDENTIALS environment variable configured
    client = bigquery.Client()

    # 2. Configure clean and correct target references
    PROJECT_ID = "technical-assessment-504501"
    DATASET_ID = "alchemia_dataset"
    TABLE_ID = "iowa_liquor_sales_extracted"
    
    destination_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 3. Extraction query string with filters completely removed
    query_string = """
        SELECT 
            invoice_and_item_number,
            date,
            store_number,
            store_name,
            address,
            city,
            zip_code,
            store_location,
            county_number,
            county,
            category,
            category_name,
            vendor_number,
            vendor_name,
            item_number,
            item_description,
            pack,
            bottle_volume_ml,
            state_bottle_cost,
            state_bottle_retail,
            bottles_sold,
            sale_dollars,
            volume_sold_liters,
            volume_sold_gallons
        FROM `bigquery-public-data.iowa_liquor_sales.sales`
    """

    # 4. Set up the BigQuery QueryJobConfig to write directly into your custom destination table
    job_config = bigquery.QueryJobConfig(
        destination=destination_table_ref,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Overwrites the table if it already exists
    )

    # Open log record channel
    with open("registro_etl.txt", "w") as f:
        f.write("Starting script execution...\n")
        print("Starting public table data extraction...")
        
        try:
            query_job = client.query(query_string, job_config=job_config)
            f.write(f"Job submitted to BigQuery with ID: {query_job.job_id}\n")
            print(f"Job submitted to BigQuery with ID: {query_job.job_id}")
            
            # Wait for Google Cloud to process the massive table query completely
            query_job.result()
            
            # Retrieve table size data to confirm loaded dataset metrics
            created_table = client.get_table(destination_table_ref)
            
            f.write(f"Google Cloud processed query successfully! Total rows loaded: {created_table.num_rows}\n")
            print(f"✔ Success! Table '{TABLE_ID}' generated with {created_table.num_rows} rows.")
            
        except Exception as e:
            f.write(f"An internal exception occurred: {str(e)}\n")
            print(f"❌ Error during execution: {str(e)}")

if __name__ == "__main__":
    run_iowa_liquor_etl()
