# CSV to DynamoDB


##  Steps to deploy

1. Add the policy document to add the dynamodb and s3 access to the lambda function

2. Export the zip file for python 

3. Add an api gateway to the lambda function

4. Make a post request with the following key value pair 
    bucket_name = <bucket_name>
    file_name = <file_name>
    table_name = <table_name>

5. The data will be saved in the tablename with key table_name accesing the file from bucket bucket_name with filename file_name







## Configs
