import json
import pandas as pd
import boto3
from io import StringIO

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

sns_client = boto3.client('sns')
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:905418268362:doordash-notify"

def lambda_handler(event, context):
    processed_data, status, filename = process_records(event)
    
    if status == 500:
        publish_notification("failed", processed_data)
        return {
            'statusCode': 500,
            'body': processed_data
        }
    if processed_data is None or filename is None:
        message = "Data not found"
        publish_notification("failed", message)
        return {
            'statusCode': 400,
            'body': message
        }
    
    return store_in_target_s3(processed_data, filename)
    
def process_records(event):
    try:
        # parse through the S3 trigger information
        records = event['Records']
        ## Although this lambda gets triggered for S3 PUT events, just checking here again for better security purpose
        filter_valid_s3_triggers = list(filter(lambda item: item['eventSource'] == "aws:s3", records))
        
        delivered_records = None
        key = None
        for file in filter_valid_s3_triggers:
            print(file)
            bucket = file['s3']['bucket']['name']
            key = file['s3']['object']['key']
            print(bucket, key)
            s3_data = s3_client.get_object(Bucket=bucket, Key=key)
            s3_data = s3_data['Body'].read().decode()
            print("s3 data", s3_data)
    
            delivery_records = pd.DataFrame(eval(s3_data))
            print(delivery_records.head())
            delivered_records = delivery_records[delivery_records['status'] == "delivered"]
            print(delivered_records.head())
        
        return delivered_records, 200, key
    except Exception as e:
        print(str(e))
        return f"Exception occurred - {str(e)}", 500, None

def store_in_target_s3(processed_data, filename):
    try:
        target_bucket = "jagadish-doordash-target-zn"
        "2024-03-09-raw_input.json"
        filename = filename.split("raw_input")[0]
        filename = filename + "processed_output.json"
        json_buffer = StringIO()
        processed_data.to_json(json_buffer)
        s3_resource.Object(target_bucket, filename).put(Body=json_buffer.getvalue())
        
        message = f"Successfully stored the processed file in the target S3 bucket."
        publish_notification("success", message)
        return {
            'statusCode': 200,
            'body': message
        }
        
    except Exception as e:
        print(str(e))
        message = f"Error encountered while storing the processed json file to target S3 bucket."
        publish_notification("failed", message)
        return {
            'statusCode': 500,
            'body': message
        }
        
def publish_notification(result, message):
    response = sns_client.publish(
    TopicArn=SNS_TOPIC_ARN,
    Message=message
)
