import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import logging
import json
from google.cloud import bigquery
from datetime import datetime

temporary_gcs_bucket = "bd6d6c58-5bf3-4af7-9eda-f4dcfc4650fa"

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

def get_latest_row(bigqueryClient, full_table_id, filter_name, filter_value):
    query = f"""
        SELECT * FROM `{full_table_id}`
        WHERE {filter_name} = @filter_value
        ORDER BY __row_creation_date DESC
        LIMIT 1
    """
    query_params = [
        bigquery.ScalarQueryParameter("filter_value", "STRING", filter_value)
    ]

    try:
        query_job = bigqueryClient.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=query_params
        ))
        results = query_job.result()

        # Iterate over results and return the first row
        for row in results:
            latest_row = dict(row)
            return latest_row
        
        return None

    except Exception as e:
        logging.error(f"Failed to retrieve the latest activity: {e}")
        return None
    
def delete_from_rm_qna_activity(element, dataset_id, bigqueryClient):
    source_table_id = "source__comments"
    full_source_table_id = f"{bigqueryClient.project}.{dataset_id}.{source_table_id}"

    comment_id = element['documentKey']['_id']
    latest_comment = get_latest_row(bigqueryClient, full_source_table_id, '_id', comment_id)
    if not latest_comment:
        logging.error(f"Could not find the comment with id: {comment_id}")
        return
    
    user_id = latest_comment['owner']['id']
    logging.info(f"Processing document for {user_id} rm__qna__activity")

    qna_activity_table_id = "rm__qna__activity"
    full_qna_activity_table_id = f"{bigqueryClient.project}.{dataset_id}.{qna_activity_table_id}"
   
     # Retrieve the latest activity for the user
    latest_activity = get_latest_row(bigqueryClient, full_qna_activity_table_id, "user_id", user_id)
    if not latest_activity:
        logging.error(f"Could not find the activity for user: {user_id}")
        return

    # Update the 'questions' field by decrementing it by 1
    latest_activity['answers'] -= 1
    latest_activity["__row_creation_date"] = datetime.now().isoformat()
    
    logging.info(f"Processing updated document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [latest_activity], full_qna_activity_table_id)

def insert_into_rm_qna_activity(element, dataset_id, bigqueryClient):
    post_type = element['fullDocument']['post']['type']
    if post_type != 'question':
        logging.info(f"the comment is not an answer")
        return
    
    table_id = "rm__qna__activity"
    full_table_id = f"{bigqueryClient.project}.{dataset_id}.{table_id}"

    user_id = element['fullDocument']['owner']['id']
    logging.info(f"Processing document for {user_id} {table_id}")
   
     # Retrieve the latest activity for the user
    latest_activity = get_latest_row(bigqueryClient, full_table_id, 'user_id', user_id)

    # Update the 'answers' field by incrementing it by 1
    latest_activity['answers'] += 1
    latest_activity["__row_creation_date"] = datetime.now().isoformat()
    logging.info(f"Processing updated document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [latest_activity], full_table_id)

def insert_into_source_comments(element, dataset_id, bigqueryClient):
    post_type = element['fullDocument']['post']['type']
    if post_type != 'question':
        logging.info(f"the comment is not an answer")
        return
    
    table_id = "source__comments"
    full_table_id = f"{bigqueryClient.project}.{dataset_id}.{table_id}"

    full_document = element['fullDocument']
    full_document["__row_creation_date"] = datetime.now().isoformat()
    logging.info(f"Processing document for source__comments: {full_document}")

    load_to_bigquery(bigqueryClient, [full_document], full_table_id)
    return element
    
def insert_into_bigquery(element):
    dataset_id = "data_warehouse"
    bigqueryClient = bigquery.Client()
    operation_type = element.get("operationType", "")

    try:
        if operation_type == "insert":
            insert_into_source_comments(element, dataset_id, bigqueryClient)
            insert_into_rm_qna_activity(element, dataset_id, bigqueryClient)

        elif operation_type == 'delete':
            delete_from_rm_qna_activity(element, dataset_id, bigqueryClient)

    except Exception as e:
            logging.error(f"Error processing element: {element}, Error: {e}")

def run():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the pipeline")

    pipeline_options = PipelineOptions(save_main_session=True)

    # Set project and other pipeline options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "backend-test-aladia"
    google_cloud_options.region = "us-central1"
    google_cloud_options.job_name = 'comment-processor'
    google_cloud_options.staging_location = f"gs://{temporary_gcs_bucket}/staging"
    google_cloud_options.temp_location = f"gs://{temporary_gcs_bucket}/temp"

    # Set the runner to DataflowRunner to run on Google Cloud Dataflow
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Use the specified Pub/Sub topic
    input_topic = "projects/backend-test-aladia/topics/mongodbCDC.comments-test"

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Log received message" >> beam.Map(lambda x: logging.info(f"Received message: {x}") or x)
            | "Decode JSON" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | "Insert to BigQuery" >> beam.Map(lambda x: insert_into_bigquery(x))
        )

    logging.info("Pipeline execution completed")

if __name__ == '__main__':
    run()
