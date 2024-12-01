

import json
import boto3
import re
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def lambda_handler(event, context):
    try:
        
        query = event['queryStringParameters']['q']
        
        
        keywords = extract_keywords(query)
        
        
        results = search_opensearch(keywords)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'results': results}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET,OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            }
        }
    except Exception as e:
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET,OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            }
        }


def extract_keywords(query):
    
    cleaned_query = re.sub(r'[^\w\s]', '', query.lower())
    
    
    words = cleaned_query.split()
    
    # Remove common stop words (you can expand this list)
    stop_words = set(['the', 'a', 'an', 'in', 'on', 'at', 'for', 'to', 'of', 'with', 'by','show','me','images','pictures','search'])
    keywords = [word for word in words if word not in stop_words]
    
    return keywords

def search_opensearch(keywords):
    # OpenSearch configuration
    host = 'search-photos-2iks6dqb6qtlhwbubkkgjpk3qy.us-east-1.es.amazonaws.com'
    region = 'us-east-1'
    index = 'photos'

    # AWS credentials
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    # Initialize OpenSearch client
    opensearch = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Construct the search query
    query = {
        'query': {
            'bool': {
                'should': [{'match': {'labels': keyword}} for keyword in keywords]
            }
        }
    }

    # Perform the search
    response = opensearch.search(index=index, body=query)

    # Extract and format the results
    results = []
    for hit in response['hits']['hits']:
        results.append({
            'objectKey': hit['_source']['objectKey'],
            'bucket': hit['_source']['bucket'],
            'createdTimestamp': hit['_source']['createdTimestamp'],
            'labels': hit['_source']['labels']
        })
    # Return the results
    return results


