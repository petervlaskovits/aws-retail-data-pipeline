from modules.generate_data import *
from dotenv import dotenv_values

from prefect import flow, get_run_logger, task, tags
from prefect_aws import AwsCredentials, S3Bucket, LambdaFunction

import time

import pyarrow as pa
import pyarrow.parquet as pq

env = dotenv_values('../.env')
bucket_name = env['bucket_name']

generator_settings = {
    "customer_records": 1000,
    "discount_records": 20,
    "product_records": 50,
    "order_records": 5000,
}

generators = [
    {
        "generator_name": "customers",
        "generator": generate_customers(generator_settings["customer_records"])
    },
    {
        "generator_name": "discounts",
        "generator": generate_discounts(generator_settings["discount_records"])
    },
    {
        "generator_name": "products",
        "generator": generate_products(generator_settings["product_records"])
    },
    {
        "generator_name": "orders",
        "generator": generate_orders(generator_settings["order_records"], generator_settings["customer_records"], generator_settings["product_records"], generator_settings["discount_records"])
    },
]

@task(tags=['elt'])
def generate_and_upload_to_s3():
    logger = get_run_logger()
    bucket = S3Bucket.load('s3-bucket')

    for generator_info in generators:
        save_to_parquet(generator_info["generator"], generator_info["generator_name"], k=2)
        logger.info(f"Successfully generated: {generator_info['generator_name']}")

    bucket.upload_from_folder(from_folder="../data", to_folder="raw/")

@task (tags=['elt'])
def trigger_lambda_function():
    lambda_function = LambdaFunction.load("process-lambda")

    try:
        response = lambda_function.invoke(payload={})
    except:
        raise Exception(response)

    return response

@task(tags=['elt'])
def start_glue_job():
    creds = AwsCredentials.load("aws-creds")
    glue_client = creds.get_boto3_session().client('glue', region_name='us-west-1')
    logger = get_run_logger()
    response = glue_client.start_job_run(JobName='Drop Nulls')
    jobRunId = response['JobRunId']
    logger.info(f"Glue job started with JobRunId: {jobRunId}, waiting for response...")

    while True:
        job_info = glue_client.get_job_run(JobName='Drop Nulls', RunId=jobRunId)
        job_state = job_info['JobRun']['JobRunState']

        if job_state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            logger.info(f"Glue job completed with state: {job_state}")
            return job_info['JobRun']

        time.sleep(10)
    
@task(tags=['elt'])
def rename_s3_objects():
    creds = AwsCredentials.load("aws-creds")
    s3_client = creds.get_s3_client()

    bucket = S3Bucket.load('s3-bucket')

    objects = bucket.list_objects("cleaned")
    for obj in objects:
        new_key = ''
        if obj['Key'] == 'cleaned/':
            continue

        obj_content = bucket.read_path(obj['Key'])
        reader = pa.BufferReader(obj_content)
        parquet = pq.read_table(source=reader)
        first_col = parquet.column_names[0]

        match first_col:
            case "customer_id":
                new_key = "cleaned/customers/customers.parquet"
            case "discount_id":
                new_key = "cleaned/discounts/discounts.parquet"
            case "product_id":
                new_key = "cleaned/products/products.parquet"
            case "order_id":
                new_key = "cleaned/orders/orders.parquet"

        bucket.copy_object(from_path=obj['Key'], to_path=new_key)
        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

@task(tags=['elt'])
def run_glue_crawler():
    glue_client = AwsCredentials.load('aws-creds').get_boto3_session().client('glue', 'us-west-1')
    glue_client.start_crawler(Name='test_crawler')
        
@flow
def orchestrate():
    generate_and_upload_to_s3()
    lambda_response = trigger_lambda_function()
    if lambda_response['StatusCode'] == 200:
        job_status = start_glue_job()
        if job_status['JobRunState'] == 'SUCCEEDED':
            rename_s3_objects()
            run_glue_crawler()   
        else:
            raise Exception("Glue job failed: " + job_status) 
    else:
        raise Exception("Lambda function failed to invoke: " + lambda_response)
    
orchestrate()