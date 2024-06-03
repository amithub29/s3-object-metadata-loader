import boto3

s3_client = boto3.client('s3')


def get_metadata(bucket, key):

    metadata = s3_client.head_object(
        Bucket=bucket,
        Key=key
    )
    content_type = metadata['ContentType']

    return content_type


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    size = event['Records'][0]['s3']['object']['size']
    timestamp = event['Records'][0]['eventTime']
    region = event['Records'][0]['awsRegion']

    content_type = get_metadata(bucket, key)

    return {
        'statusCode': 200,
        'contentType': content_type,
        'region': region,
        'timestamp': timestamp,
        'bucket': bucket,
        'key': key,
        'size': size,
    }
