import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import logging
import json
from google.cloud import bigquery
from datetime import datetime

temporary_gcs_bucket = "bd6d6c58-5bf3-4af7-9eda-f4dcfc4650fa"

source_table_id = "source__posts"
qna_activity_table_id = 'rm__qna__activity'

bigqueryClient = bigquery.Client()
dataset_id = "data_warehouse"
full_dataset_id = f"{bigqueryClient.project}.{dataset_id}"

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
        
def insert_into_source_posts(element, dataset_id, table_id="source__posts"):
    client = bigquery.Client()
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    # Extract the fullDocument field from the incoming message
    try:
        full_document = element['fullDocument']
        full_document["__row_creation_date"] = datetime.now().isoformat()
        logging.info(f"Processing document for source__posts: {full_document}")
    except KeyError as e:
        logging.error(f"Key error: {e}. The element structure might be different than expected: {element}")
        return 

    # Prepare the row to insert
    load_to_bigquery(client, [full_document], full_table_id)
    return element

def update_rm_qna_activity(element):
    full_source_table_id = f"{full_dataset_id}.{source_table_id}"
    full_qna_activity_table_id = f"{full_dataset_id}.{qna_activity_table_id}"

    # Extract the fullDocument field from the incoming message
    full_document = element['fullDocument']

    post_id = element['fullDocument']['_id']
    latest_post = get_latest_row(full_source_table_id, '_id', post_id)

    user_id = latest_post['owner']['id']
    latest_activity = get_latest_row(full_qna_activity_table_id, 'user_id', user_id)

    if not latest_post['answerId'] and full_document['answerId']:
        latest_activity['solvedQuestions'] += 1
    elif latest_post['answerId'] and not full_document['answerId']:
        latest_activity['solvedQuestions'] -= 1
    else:
        return
    
    logging.info(f"Processing document for qna_activity: {full_document}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [full_document], full_qna_activity_table_id)
    return element

def get_latest_row(full_table_id, filter_name, filter_value):
    # Retrieve the latest activity for the given user.
    query = f"""
        SELECT * FROM `{full_table_id}`
        WHERE {filter_name} = @{filter_value}
        ORDER BY __row_creation_date DESC
        LIMIT 1
    """
    query_params = [
        bigquery.ScalarQueryParameter({filter_name}, "STRING", filter_value)
    ]

    try:
        query_job = bigqueryClient.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=query_params
        ))
        results = query_job.result()

        # Directly access the latest row
        latest_row = dict(results[0]) if results.total_rows > 0 else None
        if not latest_row:
            logging.info(f"No existing row found for: {filter_value}")
            return
        
        return latest_row

    except Exception as e:
        logging.error(f"Failed to retrieve the latest activity: {e}")
        return None
    
def delete_from_rm_qna_activity(element):
    post_id = element['documentKey']
    user_id = get_latest_row(bigqueryClient, full_source_table_id, '_id', post_id)
    logging.info(f"Processing document for {user_id} rm__qna__activity")
   
     # Retrieve the latest activity for the user
    latest_activity = get_latest_row(bigqueryClient, full_activity_table_id, "user_id", user_id)

    # Update the 'questions' field by incrementing it by 1
    latest_activity['questions'] -= 1
    logging.info(f"Processing updated document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [latest_activity], full_activity_table_id)

def insert_into_rm_qna_activity(element, dataset_id):
    client = bigquery.Client()
    full_table_id = f"{client.project}.{dataset_id}.rm__qna__activity"

    user_id = element['fullDocument']['owner']['id']
    logging.info(f"Processing document for {user_id} rm__qna__activity")
   
     # Retrieve the latest activity for the user
    latest_activity = get_latest_row(client, full_table_id, 'user_id', user_id)

    # Update the 'questions' field by incrementing it by 1
    latest_activity['questions'] += 1
    logging.info(f"Processing updated document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(client, [latest_activity], full_table_id)
    
def insert_into_bigquery(element, dataset_id):
    operation_type = element.get("operationType", "")
    
    if operation_type == "insert":
        insert_into_source_posts(element, dataset_id)
        insert_into_rm_qna_activity(element, dataset_id)

    elif operation_type == "update":
        insert_into_source_posts(element, dataset_id)
        update_rm_qna_activity(element, dataset_id)

    elif operation_type == 'delete':
        delete_from_rm_qna_activity(element, dataset_id)

def run():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the pipeline")

    pipeline_options = PipelineOptions(save_main_session=True)

    # Set project and other pipeline options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "backend-test-aladia"
    google_cloud_options.region = "us-east1"
    google_cloud_options.job_name = 'post-processor'
    google_cloud_options.staging_location = f"gs://{temporary_gcs_bucket}/staging"
    google_cloud_options.temp_location = f"gs://{temporary_gcs_bucket}/temp"

    # Set the runner to DataflowRunner to run on Google Cloud Dataflow
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Use the specified Pub/Sub topic
    input_topic = "projects/backend-test-aladia/topics/mongodbCDC.posts-test"

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
