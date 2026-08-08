import os
from google.cloud import bigquery

#A simple ETL (Extract-Transform-Load) job that runs entirely inside
#BigQuery: it queries a public dataset (Iowa liquor sales) and writes
#the results into your own project's table, using BigQuery itself as
#both the query engine and the destination — no data ever leaves
#Google's infrastructure or gets processed locally.
#Progress and errors are logged both to the console and to a local
#text file ("registro_etl.txt") for a persistent record of the run.
    
def run_iowa_liquor_etl():
    # ----------------------------------------------------------------
    # 1. Initialize the BigQuery client
    # ----------------------------------------------------------------
    # Note: Ensure you have your GOOGLE_APPLICATION_CREDENTIALS environment variable configured
    
    # bigquery.Client() authenticates using Application Default
    # Credentials — it looks for the GOOGLE_APPLICATION_CREDENTIALS
    # environment variable (pointing to a service account key file) or,
    # if running on GCP infrastructure, uses the attached service account
    # automatically. No project is passed explicitly here, so it falls
    # back to whatever project your credentials are associated with by default.
    client = bigquery.Client()

    # ----------------------------------------------------------------
    # 2. Configure clean and correct target references
    # ----------------------------------------------------------------
    # These three values together identify exactly where the query
    # results will be written: <project>.<dataset>.<table>
    PROJECT_ID = "technical-assessment-504501"
    DATASET_ID = "alchemia_dataset"
    TABLE_ID = "iowa_liquor_sales_extracted"

    # BigQuery accepts a fully-qualified table string in this format
    # when specifying a query destination.
    destination_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # ----------------------------------------------------------------
    # 3. Extraction query string with filters completely removed
    # ----------------------------------------------------------------
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

    # ----------------------------------------------------------------
    # 4. Set up the BigQuery QueryJobConfig to write directly into your
    #    custom destination table
    # ----------------------------------------------------------------
     # QueryJobConfig lets you attach extra behavior to a query job beyond
    # just "run this SQL and hand back the results" — here, instead of
    # returning results to the client, BigQuery will materialize them
    # directly into destination_table_ref.
    job_config = bigquery.QueryJobConfig(
        destination=destination_table_ref,
        # WRITE_TRUNCATE means: if the destination table already exists,
        # wipe its contents first and replace them with this query's
        # results. This makes the job idempotent — re-running it always
        # leaves the table matching the current query output, rather
        # than duplicating rows on every run.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Overwrites the table if it already exists
    )

    # ----------------------------------------------------------------
    # Open log record channel
    # ----------------------------------------------------------------
    # "with open(...)" opens (or creates/overwrites, due to mode "w")
    # registro_etl.txt and automatically closes it once the indented
    # block below finishes, even if an error occurs. Every major step
    # is written both to this file (f.write) and to the console (print),
    # so you get a persistent log alongside real-time feedback.
    with open("registro_etl.txt", "w") as f:
        f.write("Starting script execution...\n")
        print("Starting public table data extraction...")
        
        try:
            # client.query() submits the SQL + job_config to BigQuery and
            # immediately returns a QueryJob object representing the
            # in-progress (asynchronous) job — it does not block here.
            query_job = client.query(query_string, job_config=job_config)
            f.write(f"Job submitted to BigQuery with ID: {query_job.job_id}\n")
            print(f"Job submitted to BigQuery with ID: {query_job.job_id}")
             # .result() blocks execution until the job completes. Since
            # this query scans a large public table, this call may take
            # a while. If the query fails (bad SQL, permissions, quota
            # exceeded, etc.), calling .result() is what raises the
            # exception, which is caught by the except block below.
            
            # Wait for Google Cloud to process the massive table query completely
            query_job.result()
            # Once the job succeeds, fetch metadata about the newly
            # written destination table so we can report exactly how
            # many rows ended up there — a quick sanity check that the
            # load actually happened as expected.
            
            # Retrieve table size data to confirm loaded dataset metrics
            created_table = client.get_table(destination_table_ref)
            
            f.write(f"Google Cloud processed query successfully! Total rows loaded: {created_table.num_rows}\n")
            print(f"✔ Success! Table '{TABLE_ID}' generated with {created_table.num_rows} rows.")
            
        except Exception as e:
            # Catch-all for any failure during job submission or execution
            # (invalid SQL, insufficient permissions on the public dataset
            # or destination project, quota/billing issues, etc.). The
            # error is recorded in both the log file and the console
            # instead of letting the script crash with a raw traceback.
            f.write(f"An internal exception occurred: {str(e)}\n")
            print(f"❌ Error during execution: {str(e)}")

if __name__ == "__main__":
    # Only run the ETL job when this file is executed directly
    # (e.g. `python script.py`), not when imported as a module.
    run_iowa_liquor_etl()
