import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import logging
import json
from google.cloud import bigquery
from datetime import datetime

temporary_gcs_bucket = "bd6d6c58-5bf3-4af7-9eda-f4dcfc4650fa"

qna_activity_schema = [
    bigquery.SchemaField('__row_creation_date', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('user_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('answers', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('questions', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('bestAnswers', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('solvedQuestions', 'INTEGER', mode='NULLABLE'),
]

post_schema = [
    bigquery.SchemaField('_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('createdAt', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('updatedAt', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('__row_creation_date', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('content', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('isPublic', 'BOOLEAN', mode='NULLABLE'),
    bigquery.SchemaField(
        'numberOf', 'RECORD', mode='NULLABLE', fields=[
            bigquery.SchemaField('shares', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('comments', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField(
                'reactions', 'RECORD', mode='NULLABLE', fields=[
                    bigquery.SchemaField('clap', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('love', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('angry', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('funny', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('sad', 'INTEGER', mode='NULLABLE'),
                ]
            ),
            bigquery.SchemaField('reactionsCount', 'INTEGER', mode='NULLABLE'),
        ]
    ),
    bigquery.SchemaField('rating', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField(
        'owner', 'RECORD', mode='NULLABLE', fields=[
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
        ]
    ),
    bigquery.SchemaField(
        'entity', 'RECORD', mode='NULLABLE', fields=[
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
        ]
    ),
    bigquery.SchemaField('files', 'STRING', mode='REPEATED'),
    bigquery.SchemaField(
        'mentions', 'RECORD', mode='REPEATED', fields=[
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
        ]
    ),
    bigquery.SchemaField('answerId', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('createdBy', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField(
        'participants', 'RECORD', mode='REPEATED', fields=[
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
        ]
    ),
    bigquery.SchemaField('parentId', 'JSON', mode='NULLABLE'),
]

def load_to_bigquery(client, rows_to_insert, full_table_id, schema):
    job_config = bigquery.LoadJobConfig(
        schema = schema,
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True
    )

    try:
        load_job = client.load_table_from_json(rows_to_insert, full_table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Successfully inserted document into table: {full_table_id}")
    except Exception as e:
        logging.error(f"Failed to insert into BigQuery, error: {e}")

def get_latest_row(bigqueryClient, full_table_id, filter_name, filter_value):
    # Retrieve the latest activity for the given user.
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
    source_table_id = "source__posts"
    full_source_table_id = f"{bigqueryClient.project}.{dataset_id}.{source_table_id}"

    post_id = element['documentKey']['_id']
    latest_post = get_latest_row(bigqueryClient, full_source_table_id, '_id', post_id)
    if not latest_post:
        logging.error(f"Could not find the post with id: {post_id}")
        return

    user_id = latest_post['owner']['id']
    logging.info(f"Processing document for {user_id} rm__qna__activity")

    qna_activity_table_id = "rm__qna__activity"
    full_qna_activity_table_id = f"{bigqueryClient.project}.{dataset_id}.{qna_activity_table_id}"

     # Retrieve the latest activity for the user
    latest_activity = get_latest_row(bigqueryClient, full_qna_activity_table_id, "user_id", user_id)
    if not latest_activity:
        logging.error(f"Could not find the activity for user: {user_id}")
        return
    
    # Update the 'questions' field by decrementing it by 1
    latest_activity['questions'] -= 1
    latest_activity["__row_creation_date"] = datetime.now().isoformat()

    logging.info(f"Processing updated document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [latest_activity], full_qna_activity_table_id, qna_activity_schema)

def update_rm_qna_activity(element, dataset_id, bigqueryClient):
    logging.info("updating the qna activity read model")

    post_type = element['fullDocument']['type']
    if post_type != 'question':
        logging.info(f"the post is not of type question")
        return
    
    source_table_id = "source__posts"
    qna_activity_table_id = "rm__qna__activity"
    full_source_table_id = f"{bigqueryClient.project}.{dataset_id}.{source_table_id}"
    full_qna_activity_table_id = f"{bigqueryClient.project}.{dataset_id}.{qna_activity_table_id}"

    post_id = element['fullDocument']['_id']
    latest_post = get_latest_row(bigqueryClient, full_source_table_id, '_id', post_id)

    logging.info(f"retrieved latest post: {latest_post}")

    user_id = latest_post['owner']['id']
    latest_activity = get_latest_row(bigqueryClient, full_qna_activity_table_id, 'user_id', user_id)

    logging.info(f"retrieved latest activity: {latest_activity}")

    if not latest_post.get('answerId') and element['fullDocument'].get('answerId'):
        latest_activity['solvedQuestions'] += 1

    elif latest_post.get('answerId') and not element['fullDocument'].get('answerId'):
        latest_activity['solvedQuestions'] -= 1

    else:
        return
    
    logging.info(f"Processing document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [latest_activity], full_qna_activity_table_id, qna_activity_schema)

def insert_into_rm_qna_activity(element, dataset_id, bigqueryClient):
    post_type = element['fullDocument']['type']
    if post_type != 'question':
        logging.info(f"the post is not of type question")
        return
    
    table_id = "rm__qna__activity"
    full_table_id = f"{bigqueryClient.project}.{dataset_id}.{table_id}"

    user_id = element['fullDocument']['owner']['id']
    logging.info(f"Processing document for {user_id} {table_id}")
   
     # Retrieve the latest activity for the user
    latest_activity = get_latest_row(bigqueryClient, full_table_id, 'user_id', user_id)

    # Update the 'questions' field by incrementing it by 1
    latest_activity['questions'] += 1
    latest_activity["__row_creation_date"] = datetime.now().isoformat()
    logging.info(f"Processing updated document for rm__qna__activity: {latest_activity}")

    # Prepare the row to insert
    load_to_bigquery(bigqueryClient, [latest_activity], full_table_id, qna_activity_schema)

def insert_into_source_posts(element, dataset_id, bigqueryClient):
    post_type = element['fullDocument']['type']
    if post_type != 'question':
        logging.info(f"the post is not of type question")
        return
    
    table_id = "source__posts"
    full_table_id = f"{bigqueryClient.project}.{dataset_id}.{table_id}"

    full_document = element['fullDocument']
    full_document["__row_creation_date"] = datetime.now().isoformat()
    logging.info(f"Processing document for source__posts: {full_document}")

    load_to_bigquery(bigqueryClient, [full_document], full_table_id, post_schema)
    
def insert_into_bigquery(element):
    dataset_id = "data_warehouse"
    bigqueryClient = bigquery.Client()
    operation_type = element.get("operationType", "")
    
    if operation_type == "insert":
        insert_into_source_posts(element, dataset_id, bigqueryClient)
        insert_into_rm_qna_activity(element, dataset_id, bigqueryClient)

    elif operation_type == "update":
        insert_into_source_posts(element, dataset_id, bigqueryClient)
        update_rm_qna_activity(element, dataset_id, bigqueryClient)

    elif operation_type == 'delete':
        delete_from_rm_qna_activity(element, dataset_id, bigqueryClient)

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
    input_subscription = "projects/backend-test-aladia/subscriptions/mongodbCDC.posts-test-sub"

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
