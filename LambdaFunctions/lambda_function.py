import json
import pandas as pd
import io
import urllib
import boto3 
import requests
import bs4 as bs
from bs4 import BeautifulSoup
import ExperimentalSynonymFeatureForAmazonLex as ex
import time
import urllib.request
from awslexsdk import constructBot

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    print(event)
    
    
    # Getting bucket
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Getting file name
    name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    print(bucket, name)
    
   
    
    # Fetching file
    try:
        resp = s3.get_object(Bucket=bucket, Key=name)
        df = pd.read_csv(resp['Body'], sep=',')
        
        columns = []
        
        for type in df.columns:
            print(type)
            columns.append(type)
            
        print(columns)
        constructBot(columns)
        
        
        return {
            'status': 200, 
            'body': columns
        }
        
    except Exception as e:
        print(e)
        
        return {
            'status': 400,
            'body': str(e)
        }