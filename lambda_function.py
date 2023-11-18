import json
import boto3
from datetime import datetime
from customEncoder import CustomEncoder
import os

s3_bucket_name = 'project-1-datalake'
s3_client = boto3.client('s3')

getMethod = "GET"

s3Path = "/s3"
allPath = s3Path + "/all"
itemPath = s3Path + "/item"
allHealthPath = allPath + "/health"

def lambda_handler(event, context):
    
    path = event['path']
    httpMethod = event['httpMethod']
    print(path)
    if path == allHealthPath and httpMethod == getMethod:
        print("health checking...")
        return buildResponse(200, "health OK")
    elif path == allPath and httpMethod == getMethod:
        try:
    
            response = s3_client.list_objects_v2(Bucket=s3_bucket_name)
    
            object_info_list = []
            for obj in response.get('Contents', []):
                tags = s3_client.get_object_tagging(Bucket=s3_bucket_name, Key=obj['Key']).get('TagSet', [])
                filename = os.path.basename(obj['Key'])
                extension = os.path.splitext(filename)[1]
                object_info = {
                    'Key': obj.get('Key'),
                    'Type': extension,
                    'LastModified': obj.get('LastModified').strftime("%Y-%m-%d %H:%M:%S"),
                    'Tags': tags,  
                    'HasTagKey': any(tag['Key'] == 'tag_key' for tag in tags)  
                }
    
                object_info_list.append(object_info)
    
            object_info_list.sort(key=lambda x: datetime.strptime(x['LastModified'], "%Y-%m-%d %H:%M:%S"), reverse=True)
    
            top_20_objects = object_info_list[:20]
            
            return buildResponse(200, json.dumps(top_20_objects, default=str))
        except Exception as e:
            raise e
    elif path == itemPath and httpMethod == getMethod:
        # add single item get
        singleItem = event['item']
        try:
            s3_item = s3_client.get_object(Bucket=s3_bucket_name, Key=singleItem)
            
            content = s3_item['Body'].read()
            
            return buildResponse(200, content)
        except Exception as e:
            raise e

def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response
