import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import logging
import json
from google.cloud import bigquery
from datetime import datetime

temporary_gcs_bucket = "bd6d6c58-5bf3-4af7-9eda-f4dcfc4650fa"

def load_to_bigquery(bigqueryClient, rows_to_insert, full_table_id):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )

    try:
        load_job = bigqueryClient.load_table_from_json(rows_to_insert, full_table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Successfully inserted document into table: {full_table_id}")
    except Exception as e:
        logging.error(f"Failed to insert into BigQuery, error: {e}")
        
def insert_into_source_users(element, dataset_id, bigqueryClient):
    table_id = "source__users"
    full_table_id = f"{bigqueryClient.project}.{dataset_id}.{table_id}"

    # Extract the fullDocument field from the incoming message
    full_document = element['fullDocument']

    full_document["__row_creation_date"] = datetime.now().isoformat()
    del full_document["password"]
    del full_document["tokens"]

    logging.info(f"Processing document for source__users: {full_document}")

    load_to_bigquery(bigqueryClient, [full_document], full_table_id)

def insert_into_rm_qna_activity(element, dataset_id, bigqueryClient):
    table_id = "rm__qna__activity"
    full_table_id = f"{bigqueryClient.project}.{dataset_id}.{table_id}"

    # Extract the fullDocument field and prepare the row to insert
    full_document = element['fullDocument']

    qna_activity = {
        "user_id": full_document["_id"],
        "questions": 0,
        "solvedQuestions": 0,
        "answers": 0,
        "bestAnswers": 0,
        "__row_creation_date": datetime.now().isoformat()
    }
    logging.info(f"Processing document for rm__qna__activity: {qna_activity}")
    
    load_to_bigquery(bigqueryClient, [qna_activity], full_table_id)

def insert_into_bigquery(element):
    dataset_id = "data_warehouse"
    bigqueryClient = bigquery.Client()
    operation_type = element.get("operationType", "")
    
    if operation_type == "insert":
        insert_into_source_users(element, dataset_id, bigqueryClient)
        insert_into_rm_qna_activity(element, dataset_id, bigqueryClient)

def run():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the pipeline")

    pipeline_options = PipelineOptions(save_main_session=True)

    # Set project and other pipeline options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "backend-test-aladia"
    google_cloud_options.region = "us-east1"
    google_cloud_options.job_name = 'user-processor'
    google_cloud_options.staging_location = f"gs://{temporary_gcs_bucket}/staging"
    google_cloud_options.temp_location = f"gs://{temporary_gcs_bucket}/temp"

    # Set the runner to DataflowRunner to run on Google Cloud Dataflow
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Use the specified Pub/Sub topic
    input_subscription = "projects/backend-test-aladia/subscriptions/mongodbCDC.users-test-sub"

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | "Log received message" >> beam.Map(lambda x: logging.info(f"Received message: {x}") or x)
            | "Decode JSON" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | "Log decoded message" >> beam.Map(lambda x: logging.info(f"Decoded message: {x}") or x)
            | "Insert to BigQuery" >> beam.Map(lambda x: insert_into_bigquery(x))
        )

    logging.info("Pipeline execution completed")

if __name__ == '__main__':
    run()
