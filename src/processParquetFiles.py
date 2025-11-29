# Lambda function

import json
import boto3
import botocore

s3 = boto3.client('s3')

bucket = 'peter-v-bucket-playground'
keys = ['raw/discounts.parquet', 'raw/customers.parquet', 'raw/products.parquet']

def lambda_handler(event, context):
    keys_exist = []

    for key in keys:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            keys_exist.append(key)
        except botocore.exceptions.ClientError as e:
            keys_exist.append('')
            print(e.response)

    print(keys_exist)

    if keys_exist == keys:
        return {
            'statusCode': 200,
            'body': json.dumps('Parquet files processed successfully')
        }
    else:
        return {
            'statusCode': 404,
            'body': json.dumps('Parquet files not found')
        }