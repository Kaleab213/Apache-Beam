import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import logging
import json
from google.cloud import bigquery
from datetime import datetime

temporary_gcs_bucket = "bd6d6c58-5bf3-4af7-9eda-f4dcfc4650fa"
dataset_id = "data_warehouse"
table_id = "source__courses"

def load_to_bigquery(client, rows_to_insert, full_table_id):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )

    try:
        load_job = client.load_table_from_json(rows_to_insert, full_table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Successfully inserted document into table: {full_table_id}")
    except Exception as e:
        logging.error(f"Failed to insert into BigQuery, error: {e}")
        
def insert_into_source_courses(element, dataset_id, table_id="source__courses"):
    client = bigquery.Client()
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    # Extract the fullDocument field from the incoming message
    try:
        full_document = element['fullDocument']
        full_document["__row_creation_date"] = datetime.now().isoformat()
        logging.info(f"Processing document for source__courses: {full_document}")
    except KeyError as e:
        logging.error(f"Key error: {e}. The element structure might be different than expected: {element}")
        return 

    # Prepare the row to insert
    rows_to_insert = [full_document]
    load_to_bigquery(client, rows_to_insert, full_table_id)

    return element

def insert_into_rm_qna_overview(element, dataset_id, table_id="rm__qna__overview"):
    client = bigquery.Client()
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    # Extract the fullDocument field and prepare the row to insert
    try:
        full_document = element['fullDocument']
        qna_overview = {
            "course_id": full_document["_id"],
            "course_owner": full_document["owner"]["id"],
            "not_answered": 0,
            "answered": 0,
            "answered_by_teacher": 0,
            "solved": 0,
            "__row_creation_date": datetime.now().isoformat()
        }
        logging.info(f"Processing document for rm__qna__overview: {qna_overview}")
    except KeyError as e:
        logging.error(f"Key error: {e}. The element structure might be different than expected: {element}")
        return  # Skip processing this element if the necessary keys are missing

    # Prepare the row to insert
    rows_to_insert = [qna_overview]
    load_to_bigquery(client, rows_to_insert, full_table_id)

def insert_into_bigquery(element, dataset_id):
    operation_type = element.get("operationType", "")
    
    if operation_type == "insert":
        insert_into_source_courses(element, dataset_id)
        insert_into_rm_qna_overview(element, dataset_id)
    else:
        logging.info(f"Ignoring non-insert operation: {operation_type}")

def run():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the pipeline")

    pipeline_options = PipelineOptions(save_main_session=True)

    # Set project and other pipeline options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "backend-test-aladia"
    google_cloud_options.region = "us-east1"
    google_cloud_options.job_name = 'course-processor'
    google_cloud_options.staging_location = f"gs://{temporary_gcs_bucket}/staging"
    google_cloud_options.temp_location = f"gs://{temporary_gcs_bucket}/temp"

    # Set the runner to DataflowRunner to run on Google Cloud Dataflow
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Use the specified Pub/Sub topic
    input_topic = "projects/backend-test-aladia/topics/mongodbCDC.courses-test"

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Log received message" >> beam.Map(lambda x: logging.info(f"Received message: {x}") or x)
            | "Decode JSON" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | "Log decoded message" >> beam.Map(lambda x: logging.info(f"Decoded message: {x}") or x)
            | "Insert to BigQuery" >> beam.Map(lambda x: insert_into_bigquery(x, dataset_id))
        )

    logging.info("Pipeline execution completed")

if __name__ == '__main__':
    run()
