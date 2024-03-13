import io
import logging
import boto3
import os
import tempfile
import json
import urllib.parse
import uuid
import hashlib
import time
from urllib.parse import urlparse
import re

s3_client = boto3.client('s3')
step_split_queue = os.environ['STEP_SPLIT_QUEUE']



def send_sqs(bucket_name, file_path, file_name, name, description, total):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    sqs_client = boto3.client('sqs')
    
    sqs_queue_url = f'https://sqs.us-east-1.amazonaws.com/461135439633/{step_split_queue}'
   
    
    user_id, project_id = file_path.split('/')[0:2]
    
    file_path = file_path.replace("%20", " ")
    
    # Send message to SQS
    message_body = {
        'filename': file_name,
        'filepath': file_path,
        'Bucket': bucket_name,
        'total': total,
        'userid':user_id,
        'projectid':project_id,
        'name': name,
        'description':description
    }
    logger.info(f"Log: {message_body}")
    sqs_client.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=json.dumps(message_body) 
        )
    return message_body

def handler(event, context):
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    print("Event triggered")
    
    try:
        for record in event['Records']:
            
          body = json.loads(record['body'])
          demand_folder = body['demand_folder']
          bucket_name = body['bucket']
  
          # print('body =>', body)
          files = body.get('files', [])
          total_files = len(files)
          print('files =? ',files)
          
          file_path = files[0]['filepath'].replace('/input/','/')
          
          user_id, project_id = file_path.split('/')[0:2]
         
          
          for file in files:
              messages = send_sqs(bucket_name, file['filepath'],  file['fileName'], file['categoryName'], file['categoryDescription'], total_files)
          # print(messages)
          return 
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": f"Error occurred while splitting PDF. {str(e)} ",
        }
