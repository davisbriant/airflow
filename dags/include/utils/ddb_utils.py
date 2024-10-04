import boto3
import botocore
import pprint

def putItem(keyId, secretAccessKey, tableName, region, partKey, partKeyVal, column, value):
    resource = boto3.resource('dynamodb',region_name=region,aws_access_key_id=keyId,aws_secret_access_key=secretAccessKey,endpoint_url="http://dynamodb.{}.amazonaws.com".format(region))
    client = resource.meta.client
    try:
        response = client.put_item(
            TableName=tableName,
            Item={
                partKey: partKeyVal,
                column: value
            }
        )
        return response
    except Exception as e:
        response = "error uploading item: {}".format(e)
        return response

def getItem(keyId, secretAccessKey, tableName, region, partKey, partKeyVal):
    resource = boto3.resource('dynamodb',region_name=region,aws_access_key_id=keyId,aws_secret_access_key=secretAccessKey,endpoint_url="http://dynamodb.{}.amazonaws.com".format(region))
    client = resource.meta.client
    try:
        response = client.get_item(
            TableName=tableName,
            Key={
                partKey: partKeyVal,
            }
        )
        return response
    except Exception as e:
       response = "error getting item: {}".format(e)
       return response