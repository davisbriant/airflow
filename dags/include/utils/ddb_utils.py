import boto3
import botocore

class ddbUtils:
    def __init__(self, config):
        self.config = config 
        self.secretAccessKey = config['aws']['secretAccessKey']
        self.region = config['dynamodb']['region']
        self.keyId = config['aws']['keyId']

    def putItem(self, tableName, partKey, partKeyVal, column, value):
        resource = boto3.resource('dynamodb',region_name=self.region,aws_access_key_id=self.keyId,aws_secret_access_key=self.secretAccessKey,endpoint_url="http://dynamodb.{}.amazonaws.com".format(self.region))
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

    def getItem(self, tableName, partKey, partKeyVal, **kwargs):
        resource = boto3.resource('dynamodb',region_name=self.region,aws_access_key_id=self.keyId,aws_secret_access_key=self.secretAccessKey,endpoint_url="http://dynamodb.{}.amazonaws.com".format(self.region))
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