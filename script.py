import boto3
import json
import csv
from collections import namedtuple

s3client = boto3.client("s3")

dynamodb = boto3.resource('dynamodb')


def create(tableName):
    try:
        table = dynamodb.create_table (
            TableName = tableName,
               KeySchema = [
                       {
                           'AttributeName': 'SKU',
                           'KeyType': 'HASH'
                       },
                   ],
                   AttributeDefinitions = [
                       {
                           'AttributeName': 'SKU',
                           'AttributeType': 'S'
                       }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits':1,
                        'WriteCapacityUnits':1
                    }
                  
            )
    except:
        print("Table already exists")


def lambda_handler(event, context):
    
    table_name = "ProductDB"
    
    create(table_name)
    
    table = dynamodb.Table(table_name)
    
    # TODO implement
    bucket = "democsvbuckets"
    file_name = "file.csv"
    
    csvfile = s3client.get_object(Bucket=bucket, Key=file_name)
    csvcontent = csvfile['Body'].read().decode('utf-8').splitlines()

    lines = csv.reader(csvcontent)
    
    Product = namedtuple("Product", "sku product_name product_image status category")
    
    header = next(lines)
    rows = []
    
    for line in lines:
            rows.append(Product(line[0], line[1], line[2], line[3], line[4].split(',')))
    
    print(rows)
    
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={"SKU":row.sku,"product_name":row.product_name,"status":row.status,"category":row.category})
        print(batch)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Added to dynamodb succesfully')
    }
