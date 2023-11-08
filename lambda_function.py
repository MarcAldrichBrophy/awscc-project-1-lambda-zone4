import logging
import json
import boto3
from customEncoder import CustomEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamoName = "extracta-data"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamoName)

getMethod = "GET"
postMethod = "POST"

dataPath = "/data"
rekogPath = dataPath + "/rekognition"
comprehendPath = dataPath + "/comprehend"
transcribePath = dataPath + "/transcribe"
healthPath = dataPath + "/health"

#main handler
def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == rekogPath:
        response = rekogResponse(200)
    elif httpMethod == getMethod and path == comprehendPath:
        response = comprehendResponse(200)
    elif httpMethod == getMethod and path == transcribePath:
        response = transcribeResponse(200) 
    else:
        response = buildResponse(404, 'Not Found')
    
    return response

# Response builder.
def buildResponse(statusCode, body = None):
    id = 'health'
    increment_value = 1

    # Update the numericAttribute using an update expression.
    response = table.update_item(
        Key={'id': id},
        UpdateExpression='SET numericAttribute = numericAttribute + :val',
        ExpressionAttributeValues={':val': increment_value}
    )
    
    if body is not None:
        response['body'] = json.dumps(body, cls = CustomEncoder)
    return response

def rekogResponse(statusCode, body = None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls = CustomEncoder)
    return response

def comprehendResponse(statusCode, body = None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls = CustomEncoder)
    return response

def transcribeResponse(statusCode, body = None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls = CustomEncoder)
    return response