import logging
import json
import boto3
import traceback
from customEncoder import CustomEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamoName = "extracta-data"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamoName)

getMethod = "GET"
postMethod = "POST"

dataPath = "/data"
rekogPath = "/rekognition"
comprehendPath = "/comprehend"
transcribePath = "/transcribe"
healthPath = dataPath + "/health"

#main handler
def lambda_handler(event, context):
    try:
        logger.info(event)
        httpMethod = event['httpMethod']
        path = event['path']

        if httpMethod == getMethod and path == healthPath:
            response = healthResponse(200)
        elif httpMethod == getMethod and path == rekogPath:
            response = rekogResponse(200)
        elif httpMethod == getMethod and path == comprehendPath:
            response = comprehendResponse(200)
        elif httpMethod == getMethod and path == transcribePath:
            response = transcribeResponse(200) 
        else:
            response = buildResponse(404, 'Not Found')

        return response
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
        return buildResponse(500, f'Internal Server Error: {str(e)}')

# Response builder.
def buildResponse(statusCode, body = None):
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

def comprehendResponse(statusCode, body=None):
    try:
        item_id = "comprehend"

        dynamo_response = table.get_item(Key={'id': item_id})
        current_clicks = dynamo_response.get('Item', {}).get('clicks', 0)

        new_clicks = current_clicks + 1

    
        update_response = table.put_item(
            TableName=dynamoName,
            Item={'id': item_id, 'clicks': new_clicks}
        )

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

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
        return buildResponse(500, f'Internal Server Error: {str(e)}')

def transcribeResponse(statusCode, body = None):
    item_id = "transcribe"

    dynamo_response = table.get_item(Key ={'id':item_id})
    current_clicks = dynamo_response.get('Item', {}).get('clicks',0)

    new_clicks = current_clicks + 1

    try:
        update_response = table.put_item(
            TableName=dynamoName,
            Item={'id': item_id, 'clicks': new_clicks}
        )

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

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
        return buildResponse(500, f'Internal Server Error: {str(e)}')

def healthResponse(statusCode, body=None):
    item_id = "health"

    dynamo_response = table.get_item(Key ={'id':item_id})
    current_clicks = dynamo_response.get('Item', {}).get('clicks',0)

    new_clicks = current_clicks + 1

    try:
        update_response = table.put_item(
            TableName=dynamoName,
            Item={'id': item_id, 'clicks': new_clicks}
        )

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

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        traceback.print_exc()
        return buildResponse(500, f'Internal Server Error: {str(e)}')