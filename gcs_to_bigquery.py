import os
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# CONFIGURACIÓN
PROJECT_ID = "SU_PROYECTO_GCP"  # Reemplace con su ID de proyecto de GCP
BUCKET_NAME = "alchemialabs-tech-assessment"
DATASET_ID = "alchemia_dataset"

def automatizar_gcs_a_bigquery():
    # 1. Inicializar clientes de Google Cloud
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 2. Crear el Dataset automáticamente si no existe
    dataset_ref = bq_client.dataset(DATASET_ID)
    try:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"✔ Dataset '{DATASET_ID}' creado exitosamente.")
    except Conflict:
        print(f"ℹ El dataset '{DATASET_ID}' ya existe. Continuando...")

    # 3. Listar todos los archivos dentro del bucket
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    
    print(f"Procesando archivos en gs://{BUCKET_NAME}...")

    # 4. Iterar sobre cada archivo y crear su respectiva tabla
    for blob in blobs:
        # Ignorar directorios virtuales vacíos si los hay
        if blob.name.endswith('/'):
            continue
            
        print(f"\n--- Procesando archivo: {blob.name} ---")
        
        # Generar un nombre de tabla limpio basado en el nombre del archivo
        # Ejemplo: "datos_usuarios.csv" -> "datos_usuarios"
        nombre_archivo = os.path.basename(blob.name)
        nombre_tabla, extension = os.path.splitext(nombre_archivo)
        
        # Limpiar caracteres no permitidos en nombres de tablas de BigQuery
        nombre_tabla = nombre_tabla.replace("-", "_").replace(" ", "_")
        
        # Definir la ruta completa del archivo y de la tabla destino
        uri_archivo = f"gs://{BUCKET_NAME}/{blob.name}"
        table_ref = dataset_ref.table(nombre_tabla)
        
        # Detectar el formato según la extensión del archivo
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

        # Configurar la creación automática de la tabla y su esquema
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Crea las columnas y detecta tipos de datos (Int, String, etc) automáticamente
            source_format=formato,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Reemplaza datos si la tabla ya existía
        )
        
        # Ejecutar la carga directa en BigQuery
        try:
            load_job = bq_client.load_table_from_uri(
                uri_archivo, 
                table_ref, 
                job_config=job_config
            )
            load_job.result() # Esperar que termine la carga
            
            # Confirmación de la tabla creada
            tabla_creada = bq_client.get_table(table_ref)
            print(f"✔ Tabla '{nombre_tabla}' creada/actualizada con {tabla_creada.num_rows} filas.")
        except Exception as e:
            print(f"❌ Error al cargar {blob.name}: {e}")

    print("\n====== TODO EL BUCKET HA SIDO PROCESADO ======")

if __name__ == "__main__":
    # Recuerde ejecutar 'gcloud auth application-default login' antes de correrlo
    automatizar_gcs_a_bigquery()
