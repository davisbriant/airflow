import boto3
from io import StringIO
import sys
import simplejson as json

def getS3(accessKey, accessSecret):
    resource = boto3.resource('s3',
        aws_access_key_id= accessKey, 
        aws_secret_access_key=accessSecret)
    return resource

def deleteFromS3(accessKey, accessSecret, bucket, key):
    try: 
        obj = getS3(accessKey, accessSecret).Object(bucket, key)
        obj.delete()
        msg = "{}/{} deleted".format(bucket, key)
        return msg
    except Exception as e:
        return e
        
def writeToS3(accessKey, accessSecret, data, bucket, key):
    try:
        s3 = getS3(accessKey, accessSecret).Bucket(bucket)
        dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=obj)
        print("writing data to s3 location {}/{}".format(bucket,key))
        dump_s3(data, key)
    except Exception as e:
        return e
    
def getFromS3(accessKey, accessSecret, bucket, key):
    obj = getS3(accessKey, accessSecret).ObjectSummary(bucket, key)
    return obj

def readFromS3(accessKey, accessSecret, bucket, key):
    obj = getS3(accessKey, accessSecret).Object(bucket, key)
    json.load_s3 = lambda f: json.loads(f.get()['Body'].read().decode('utf-8'))
    data = json.load_s3(obj)
    return data

def listObjectsFromS3(accessKey, accessSecret, bucket, prefix):
    client = getS3(accessKey, accessSecret).meta.client
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    return response