import json
import boto3
import uuid
import time
import os


s3=boto3.client('s3')
dynamodb=boto3.resource('dynamodb')

TABLE_NAME= os.environ["DDB_TABLE"]
BUCKET_NAME=os.environ["BUCKET_NAME"]
API_BASE= os.environ.get("API_BASE", "")

table=dynamodb.Table(TABLE_NAME)

def handler(events, context):
    try:
        body=json.loads(events["body"])

        file_name=body["FileName"]
        file_type=body.get("fileType","binary")

        expiry_hours=body.get("expiryHours",24)

        file_id=str(uuid.uuid4())
        s3_key=f"uploads/{file_id}/{file_name}"

        ##Generating a presigned URL

        upload_url=s3.generate_presigned_url(
            ClientMethod='put_object',
            Params={
                'Bucket':BUCKET_NAME,
                'Key':s3_key,
                'ContentType':file_type
            },
            ExpiresIn=3600
        )

        expires_at_timestamp=int(time.time())+expiry_hours*3600
        table.put_item(
            Item={
                "fileId":file_id,
                "s3Key":s3_key,
                "expiresAt":expires_at_timestamp,
                "ownerId":"anonymous",
                "downloadCount":0
            }
        )

        download_url = None
        if API_BASE:
            download_url = f"{API_BASE}/download/{file_id}"

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "fileId": file_id,
                "uploadURL": upload_url,
                "downloadURL": download_url
            })
        }
    
    except Exception as e:
        return{
            "statusCode":500,
            "body":json.dumps({"error":str(e)})
        }

