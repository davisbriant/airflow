import boto3
import botocore
import simplejson as json
import awswrangler as wr
wr.engine.set('python')
import pandas as pd
from datetime import date, datetime, time, timedelta

class s3Utils:
    def __init__(self, config):
        self.config = config 
        self.accessSecret = config['aws']['secretAccessKey']
        self.keyId = config['aws']['keyId']
        self.region = config['aws']['region']
        self.bucket = config['s3']['bucket']
        self.prefix = config['s3']['prefix']

    def getS3(self):
        resource = boto3.resource('s3',
            aws_access_key_id=self.keyId, 
            aws_secret_access_key=self.accessSecret)
        return resource

    def deleteFromS3(self, key):
        obj = self.getS3(self.keyId, self.accessSecret).Object(self.bucket,f"{self.prefix}{key}")
        obj.delete()
        msg = "{}/{} deleted".format(self.bucket, f"{self.prefix}{key}")
        return msg
            
    def writeToS3(self, data, key):
        s3 = self.getS3().Bucket(self.bucket)
        dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=obj)
        print("writing data to s3 location {}/{}".format(self.bucket,f"{self.prefix}{key}"))
        dump_s3(data, f"{self.prefix}{key}")
 
    def getFromS3(self, key):
        obj = self.getS3(self.keyId, self.accessSecret).ObjectSummary(self.bucket,f"{self.prefix}{key}")
        return obj

    def readFromS3(self, key):
        obj = self.getS3(self.keyId, self.accessSecret).Object(self.bucket,f"{self.prefix}{key}")
        json.load_s3 = lambda f: json.loads(f.get()['Body'].read().decode('utf-8'))
        data = json.load_s3(obj)
        return data

    def listObjectsFromS3(self, prefix):
        client = self.getS3(self.keyId, self.accessSecret).meta.client
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        return response

    def getSession(self):
        session = boto3.Session(
            region_name=self.region,
            aws_access_key_id=self.keyId,
            aws_secret_access_key=self.accessSecret
        )
        return session

    def csvToJsonS3(self, path, file):
        convertedAt = datetime.now()
        session = self.getSession()
        csv_path = f's3://{self.bucket}/{self.prefix}/{path}/csv/{file}.csv'
        json_path = f's3://{self.bucket}/{self.prefix}/{path}/json/{convertedAt}/{file}.json'
        df = wr.s3.read_csv(path = [csv_path], boto3_session = session)
        wr.s3.to_json(
            df = df, 
            path = json_path,
            orient = 'records',
            lines = True,
            boto3_session = session
        )
        msg =  f'{csv_path} converted to {json_path} successfully!'
        return msg