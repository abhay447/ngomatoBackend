from __future__ import print_function
import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import decimal
import searchUtils
import datetime

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
table = dynamodb.Table('Donation')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):   # pylint: disable=E0202
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def putDonationFromUser(request):
    response = {}
    try:
        response = table.put_item(
            Item=request
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("PutItem succeeded:")
        print(response)

    response = {
        "statusCode": 200,
        "body": json.dumps(response,indent=4,cls=DecimalEncoder),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return response

def createDonationFromUser(event,context):
    response = {}
    request = json.loads(event["body"])
    if 'userId' in request and 'ngoId' in request:
        request['status'] = 'pending'
        request['pkey'] = request['ngoId'] + datetime.datetime.utcnow().isoformat()
        return putDonationFromUser(request)
    
    response = {
        "statusCode": 200,
        "body": "Missing Primary Key parameters",
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return response

def updateDonationFromUser(event,context):
    response = {}
    request = json.loads(event["body"])
    if 'userId' in request and 'pkey' in request:
        if request['status'] == 'approved':
            del request['status']
        return putDonationFromUser(request)
    
    response = {
        "statusCode": 200,
        "body": "Missing Primary Key parameters",
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return response

def getDonationsForUser(event, context):
    item  = {}
    response = {}
    request = json.loads(event["body"])
    if 'userId' in request :
        key_condition = Key('userId').eq(request['userId'])
        filter_conditions = []
        if 'status' in  request:
            filter_conditions = Attr('status').eq(request['status'])
        try:
            if filter_conditions != []:
                response = table.query(
                    KeyConditionExpression=key_condition,
                    FilterExpression=filter_conditions
                )
            else:
                response = table.query(
                    KeyConditionExpression=key_condition
                )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            item = json.dumps(response['Items'],indent=4,cls=DecimalEncoder)
            print("GetItem succeeded:")
            print(item)

        response = {
            "statusCode": 200,
            "body": item,
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            }
        }
    else:
        response = {
            "statusCode": 200,
            "body": "Missing Primary Key parameters",
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            }
        }
    return response

def updateDonationFromNGO(event,context):
    response = {}
    request = json.loads(event["body"])
    if 'userId' in request and 'pkey' in request:
        return putDonationFromUser(request)
    
    response = {
        "statusCode": 200,
        "body": "Missing Primary Key parameters",
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return response

def getDonationsForNGOID(event, context):
    item  = {}
    response = {}
    request = json.loads(event["body"])
    if 'ngoId' in request :
        filter_conditions = Attr('ngoId').eq(request['ngoId'])
        if 'status' in  request:
            filter_conditions = filter_conditions&Attr('status').eq(request['status'])
        try:
            response = table.scan(
                FilterExpression=filter_conditions
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            item = json.dumps(response['Items'],indent=4,cls=DecimalEncoder)
            print("GetItem succeeded:")
            print(item)

        response = {
            "statusCode": 200,
            "body": item,
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            }
        }
    else:
        response = {
            "statusCode": 200,
            "body": "Missing Primary Key parameters",
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
            }
        }
    return response