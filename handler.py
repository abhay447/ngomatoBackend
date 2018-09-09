from __future__ import print_function
import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import decimal
import searchUtils

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
table = dynamodb.Table('NGO')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):   # pylint: disable=E0202
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def putNGO(event,context):
    response = {}
    request = json.loads(event["body"])
    if 'city' in request and 'name' in request and 'email' in request:
        request["pkey"]  = request["name"]+"|"+request["email"]
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

def getNGO(event, context):
    item  = {}
    response = {}
    request = json.loads(event["body"])
    if 'city' in request and 'name' in request and 'email' in request:
        request["pkey"]  = request["name"]+"|"+request["email"]
        del request["name"]
        del request["email"]
        try:
            response = table.get_item(
                Key=request
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            item = json.dumps(response['Item'],indent=4,cls=DecimalEncoder)
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

def getAllNGO(event, context):
    item  = {}
    response = {}
    request = json.loads(event["body"])
    key_condition = {}
    filter_conditions = []
    if 'city' in request:
        key_condition = Key('city').eq(request['city'])
        if 'name' in request:
            key_condition = key_condition & Key('name').eq(request['name'])
        category_filters = searchUtils.getCatFilters(request)
        requirement_filters = searchUtils.getRequirementFilters(request)
        filter_conditions = category_filters
        # filter_conditions = requirement_filters
        print(filter_conditions)
        if requirement_filters != []:
            if filter_conditions != []:
                filter_conditions = filter_conditions&requirement_filters
            else:
                filter_conditions = requirement_filters

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
            print("Get all Items succeeded:")
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