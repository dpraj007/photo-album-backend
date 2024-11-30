import json
import boto3
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def lambda_handler(event, context):
    # Get the S3 bucket and file information from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Initialize AWS clients
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition')
    
    # Use Rekognition to detect labels
    try:
        response = rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            MaxLabels=10,
            MinConfidence=70
        )
        labels = [label['Name'] for label in response['Labels']]
    except Exception as e:
        print("Error detecting labels:", e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error detecting labels')
        }
    
    # Get custom labels from S3 object metadata
    try:
        metadata = s3.head_object(Bucket=bucket, Key=key)
        custom_labels = metadata.get('Metadata', {}).get('customlabels', '').split(',')
    except Exception as e:
        print("Error retrieving metadata:", e)
        custom_labels = []
    
    # Combine detected and custom labels
    all_labels = labels + custom_labels
    
    # Index the photo in OpenSearch
    try:
        index_to_opensearch(bucket, key, all_labels)
    except Exception as e:
        print("Error indexing photo:", e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error indexing photo')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('Photo indexed successfully!')
    }

def index_to_opensearch(bucket, key, labels):
    region = 'us-east-1' 
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    host = 'search-photos-2iks6dqb6qtlhwbubkkgjpk3qy.us-east-1.es.amazonaws.com'
    index = 'photos'
    
    opensearch = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    document = {
        'objectKey': key,
        'bucket': bucket,
        'createdTimestamp': datetime.now().isoformat(),
        'labels': labels
    }
    
    response = opensearch.index(index=index, body=document)
    print("Document indexed successfully:", response)
