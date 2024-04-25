from datetime import timedelta, datetime
import fastavro
import io
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSDeleteObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

class MyGCSHook(GCSHook):
  def get_metadata(
    self,
    bucket_name: str,
    object_name: str,
    ) -> dict:
    client = self.get_conn()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name=object_name)
    return blob.metadata

  def set_metadata(
    self,
    bucket_name: str,
    object_name: str,
    metadata: dict
    ):

    client = self.get_conn()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name=object_name)
    blob.metadata = metadata
    blob.patch()


seven_days_ago = datetime.combine(datetime.today() - timedelta(7), datetime.min.time())

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': seven_days_ago,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 0,
  'bucket' : "rawcloud",
  'bucket_name' : "rawcloud",
  "google_cloud_storage_conn_id" : "google_cloud_default",
  "gcp_conn_id" : "google_cloud_default",
  "dataset" : "Pokemon"
}

with DAG(
  'gcs-storage',
  schedule_interval=timedelta(days=1),
  render_template_as_native_obj=True,        
  default_args=default_args) as dag:

  # Listing all Tables Files
  GCS_Files = GCSListObjectsOperator(
    task_id="List-AVRO-Files",
    prefix="pokemon-avro",
  )

  # Delete all temp files
  clean_up_temporary = GCSDeleteObjectsOperator(
    task_id="Delete-Temporary",
    prefix="temporary",
  )

  # Filter files to process
  @task(task_id="Create-File-To-Process-List")
  def printOutput(**context):
    bucket = context["dag"].default_args["bucket"]
    to_process = []
    files = context["ti"].xcom_pull(task_ids="List-AVRO-Files")
    GCS_Hook = MyGCSHook()
    for blob in files:
      if ".avro" in blob:
        metadata = GCS_Hook.get_metadata(object_name=blob, bucket_name=bucket)
        if metadata is None or "uploaded" not in metadata.keys():
          to_process.append(blob)
    context["ti"].xcom_push(key="to_process", value=to_process)

  # Expand Avro and Load to BQ
  @task(task_id="Expand-Avro-And-Load-BQ")
  def processFiles(**context):
    to_process = context["ti"].xcom_pull(task_ids="Create-File-To-Process-List", key="to_process")
    bucket = context["dag"].default_args["bucket"]
    dataset = context["dag"].default_args["dataset"]
    GCS_Hook = MyGCSHook()
    BQ_Hook = BigQueryHook()

    tables_uris = {}
    for file in to_process:

      file_name = file.split("/")[-1]
      date = file.split("/")[-2]
      table = file_name.split(".")[1]

      if table not in tables_uris.keys():
        tables_uris[table] = []

      blob_bytes = GCS_Hook.download(bucket_name=bucket, object_name=file)
      avro_reader = fastavro.reader(io.BytesIO(blob_bytes))
      schema = avro_reader.writer_schema
      records = list(avro_reader)

      for record in records:
        record["ingestion_date"] = date
      
      with open(file_name, "wb") as tempFile:
        fastavro.writer(tempFile, schema, records)
      GCS_Hook.upload(bucket_name=bucket, object_name=f"temporary/{file}", filename=file_name)

      tables_uris[table].append(f"gs://{bucket}/temporary/{file}")

    for table,uris in tables_uris.items():
      BQ_Hook.run_load(destination_project_dataset_table=f"{dataset}.{table}", source_uris=uris, source_format="AVRO", autodetect=True)

    for file in to_process:
      GCS_Hook.set_metadata(bucket_name=bucket, object_name=file, metadata={"uploaded": date})

  GCS_Files>>printOutput()>>processFiles()>>clean_up_temporary

if __name__ == "__main__":
  dag.test()
