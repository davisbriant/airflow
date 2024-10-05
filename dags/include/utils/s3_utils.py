import boto3
import botocore
import simplejson as json

class s3Utils:
    def __init__(self, config):
        self.config = config 
        self.secretAccessKey = config['aws']['secretAccessKey']
        self.keyId = config['aws']['keyId']
        self.bucket = config['s3']['bucket']
        self.prefix = config['s3']['prefix']

    def getS3(self):
        resource = boto3.resource('s3',
            aws_access_key_id=self.keyId, 
            aws_secret_access_key=self.accessSecret)
        return resource

    def deleteFromS3(self, key):
        try: 
            obj = getS3(self.keyId, self.accessSecret).Object(self.bucket,f"{self.prefix}/{key}")
            obj.delete()
            msg = "{}/{} deleted".format(self.bucket, key)
            return msg
        except Exception as e:
            return e
            
    def writeToS3(self, data, key):
        try:
            s3 = getS3(self.keyId, self.accessSecret).Bucket(self.bucket)
            dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=obj)
            print("writing data to s3 location {}/{}".format(self.bucket,f"{self.prefix}/{key}"))
            dump_s3(data, key)
        except Exception as e:
            return e
        
    def getFromS3(self, key):
        obj = getS3(self.keyId, self.accessSecret).ObjectSummary(self.bucket,f"{self.prefix}/{key}")
        return obj

    def readFromS3(self, key):
        obj = getS3(self.keyId, self.accessSecret).Object(self.bucket,f"{self.prefix}/{key}")
        json.load_s3 = lambda f: json.loads(f.get()['Body'].read().decode('utf-8'))
        data = json.load_s3(obj)
        return data

    def listObjectsFromS3(self, prefix):
        client = getS3(self.keyId, self.accessSecret).meta.client
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        return response