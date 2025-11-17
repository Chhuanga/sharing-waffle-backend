import json 
import boto3
import time
import os
from boto3.dynamodb.conditions import Key

dynamodb=boto3.resource('dynamodb')
s3=boto3.client('s3')

TABLE_NAME= os.environ["DDB_TABLE"]
BUCKET_NAME=os.environ["BUCKET_NAME"]

table=dynamodb.Table(TABLE_NAME)

def handler(events, context):
    try:

        #Extract fileId from path_parameters

        file_id=events['pathParameters']['fileId']

        res=table.get_item(
            Key={
                "fileId":file_id
            })
        item=res.get("Item")
        if not item:
            return {
                "statusCode":404,
                "body":json.dumps({"message":"File not found"})
            }
        now=int(time.time())
        if item["expiresAt"]<now:
            return {
                "statusCode":410,
                "body":json.dumps({"message":"File has expired"})
            }
        
        presigned_url=s3.generate_presigned_url(
            "get_object",
            Params={
                'Bucket':BUCKET_NAME,
                'Key':item["s3Key"]
            },
            ExpiresIn=3600
        )
        table.update_item(
            Key={
                "fileId":file_id},
                UpdateExpression="SET downloadCount = if_not_exists(downloadCount, :zero) + :inc",
                ExpressionAttributeValues={
                    ":inc":1,
                    ":zero":0
                }
            
        )

        return {
            "statusCode":200,
            "body":json.dumps({"downloadUrl":presigned_url})   
        }
    
    except Exception as e:
        return {
            "statusCode":500,
            "body":json.dumps({"error":str(e)})
        }
             
        

