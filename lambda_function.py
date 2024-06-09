import boto3

s3_client = boto3.client('s3')
db_client = boto3.client('dynamodb')
table = 'S3_object_metadata'


def get_metadata(bucket, key):
    metadata = s3_client.head_object(
        Bucket=bucket,
        Key=key
    )
    content_type = metadata['ContentType']
    return content_type


def get_userid(bucket, key):
    tagset = s3_client.get_object_tagging(
        Bucket=bucket,
        Key=key
    )

    user_id = tagset['TagSet'][0]['Value']
    return user_id


def delete_item(bucket, key):
    db_client.delete_item(
        TableName=table,
        Key={
            'bucket': {'S': bucket},
            'key': {'S': key},
        }
    )


def lambda_handler(event, context):
    event_name = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    if event_name == 'ObjectRemoved:Delete':
        delete_item(bucket, key)
        return {
            'statusCode': 200,
        }

    size = event['Records'][0]['s3']['object']['size']
    timestamp = event['Records'][0]['eventTime']
    region = event['Records'][0]['awsRegion']

    content_type = get_metadata(bucket, key)
    user_id = get_userid(bucket, key)

    db_client.put_item(
        TableName=table,
        Item={
            'bucket': {'S': bucket},
            'key': {'S': key},
            'region': {'S': region},
            'user_id': {'S': user_id},
            'size': {'S': str(size)},
            'content_type': {'S': content_type},
            'timestamp': {'S': timestamp},
        }
    )

    return {
        'statusCode': 200,
    }

