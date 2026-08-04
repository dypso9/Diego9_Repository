import os
from google.cloud import bigquery

def run_iowa_liquor_etl():
    # 1. Inicializar el cliente de BigQuery
    # Nota: Asegúrate de tener la variable de entorno GOOGLE_APPLICATION_CREDENTIALS configurada
    client = bigquery.Client()

    # 2. Configurar el destino (Reemplaza con tus propios IDs)
    PROJECT_ID = "technical-assessment-504501"
    DATASET_ID = "iowa_liquor_sales_data"
    TABLE_ID = "iowa_liquor_sales_extracted"
    
    destination_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 3. Escribir la consulta de extracción 
    # (Se recomienda filtrar por año o usar TABLESAMPLE si solo deseas una muestra para pruebas)
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
        WHERE date >= '2025-11-04' and date <= '2026-05-02'
        LIMIT 100
        -- Filtro opcional para limitar el tamaño de extracción
    """

    # 4. Configurar el Job de BigQuery para guardar el resultado directamente en tu tabla
    job_config = bigquery.QueryJobConfig(
        destination=destination_table_ref,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Reemplaza la tabla si ya existe
    )

    print(f"Iniciando la extracción y carga hacia: {destination_table_ref}...")
    
    # Executar la consulta de extracción y carga masiva
    query_job = client.query(query_string, job_config=job_config)
    
    # Esperar a que el Job finalice por completo
    query_job.result()

    print(f"¡Éxito! Datos cargados correctamente en {destination_table_ref}.")

if __name__ == "__main__":
    run_iowa_liquor_etl()
