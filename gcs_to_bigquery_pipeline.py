import os
import time
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# ==============================================================
# CONFIGURACIÓN GENERAL
# ==============================================================
PROJECT_ID = "technical-assessment-504501"  
BUCKET_NAME = "alchemialabs-tech-assessment"
DATASET_ID = "alchemia_dataset"

def automate_gcs_to_bigquery():
    # 1. Inicializar clientes de Google Cloud
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 2. Crear el dataset automáticamente si no existe
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"✔ Dataset '{DATASET_ID}' creado exitosamente.")
    except Conflict:
        print(f"ℹ El Dataset '{DATASET_ID}' ya existe. Procediendo...")

    # 3. Listar archivos dentro del bucket de GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    
    # Estructura para agrupar URIs por tabla destino: {"nombre_tabla": [("gs://...", "csv")]}
    files_by_table = defaultdict(list)
    
    print(f"\nClasificando archivos en gs://{BUCKET_NAME}...")

    # 4. Clasificar y agrupar archivos por tabla de destino estratégica
    for blob in blobs:
        if blob.name.endswith('/'):
            continue
            
        file_name = os.path.basename(blob.name)
        _, extension = os.path.splitext(file_name)
        clean_ext = extension.lower().replace(".", "")
        uri_file = f"gs://{BUCKET_NAME}/{blob.name}"
        
        # Enrutamiento inteligente a tablas unificadas
        if "crm_accounts" in blob.name:
            table_name = "crm_accounts"
        elif "crm_contacts" in blob.name:
            table_name = "crm_contacts"
        elif "crm_opportunities" in blob.name or "opportunity" in blob.name.lower():
            table_name = "crm_opportunities"
        else:
            # Archivos externos o tablas independientes fuera de las carpetas CRM principales
            clean_name = blob.name.replace("/", "_")
            table_name, _ = os.path.splitext(clean_name)
            table_name = table_name.replace("-", "_").replace(" ", "_").replace(".", "_")
            
        # Añadir a la cola de procesamiento por lotes
        files_by_table[table_name].append((uri_file, clean_ext))

    # ==============================================================
    # 5. PROCESAMIENTO AGRUPADO (Un único Job de carga por Tabla)
    # ==============================================================
    for table_name, file_list in files_by_table.items():
        print(f"\n--- Iniciando lote para la tabla destino: {table_name} ---")
        table_ref = dataset_ref.table(table_name)
        
        # Extraer solo las rutas de los archivos (URIs) del grupo
        uris = [item[0] for item in file_list]
        first_ext = file_list[0][1]  # Formato del formato compartido en el grupo
        
        # Definición explícita de esquemas protectores
        explicit_schema = None
        
        if table_name == "crm_accounts":
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
        elif table_name == "crm_contacts":
            explicit_schema = [
                bigquery.SchemaField("col1", "STRING"), bigquery.SchemaField("col2", "STRING"),
                bigquery.SchemaField("col3", "STRING"), bigquery.SchemaField("col4", "STRING"),
                bigquery.SchemaField("col5", "STRING"), bigquery.SchemaField("col6", "STRING"),
                bigquery.SchemaField("col7", "STRING"), bigquery.SchemaField("col8", "STRING")
            ]
        elif table_name == "crm_opportunities":
            explicit_schema = [
                bigquery.SchemaField("opportunity_id", "STRING"),
                bigquery.SchemaField("account_id", "STRING"),
                bigquery.SchemaField("opportunity_name", "STRING"),
                bigquery.SchemaField("amount", "STRING"),
                bigquery.SchemaField("stage", "STRING"),
                bigquery.SchemaField("close_date", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("updated_at", "STRING")
            ]

        # Configuración dinâmica de Jobs de carga según el formato
        if first_ext == "csv":
            is_combined_table = table_name in ["crm_accounts", "crm_contacts", "crm_opportunities"]
            
            # WRITE_APPEND consolida el historial; WRITE_TRUNCATE limpia si es un archivo aislado único
            write_mode = bigquery.WriteDisposition.WRITE_APPEND if is_combined_table else bigquery.WriteDisposition.WRITE_TRUNCATE
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
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
                
        elif first_ext == "parquet":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif first_ext == "json":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif first_ext == "avro":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.AVRO,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        else:
            print(f"⚠ Formato .{first_ext} no compatible nativamente. Saltando grupo {table_name}...\n")
            continue

        # Envío del Job por lotes a la API de BigQuery
        try:
            print(f"Enviando {len(uris)} archivos juntos en un único Job para '{table_name}'...")
            load_job = bq_client.load_table_from_uri(
                uris,  # Pasamos la lista completa de URIs gs:// juntas
                table_ref, 
                job_config=job_config
            )
            load_job.result()  # Esperar a que el Job de BigQuery finalice.
            
            # Comprobar estado final del volumen de filas cargadas
            created_table = bq_client.get_table(table_ref)
            print(f"✔ Tabla destino '{table_name}' procesada con éxito. Total de filas actuales: {created_table.num_rows}\n")
            
            # Pausa de cortesía de seguridad técnica para proteger las cuotas de metadatos globales
            time.sleep(1.5)
            
        except Exception as e:
            print(f"❌ Error crítico al cargar el grupo de archivos para la tabla {table_name}: {e}\n")

    print("====== Todos los archivos agrupados del Bucket han sido procesados. ======")

if __name__ == "__main__":
    automate_gcs_to_bigquery()
