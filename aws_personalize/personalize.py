import logging
import boto
from botocore.exceptions 
import ClientError
import os
import time


session = boto3.Session(
    aws_access_key_id='AWS_ACCESS_KEY_ID',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY',
)
s3 = session.resource('s3')
personalize = boto3.client('personalize')

def upload_file(file_name, bucket, object_name):
    try:
        # file_name - File to upload
        # bucket - Bucket to upload to (the top level directory under AWS S3)
        # object_name - S3 object name (can contain subdirectories). If not specified then file_name is used
        response = s3.meta.client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_files_to_s3():
    upload_file("items.csv", "items", "items")
    upload_file("users.csv", "items", "items")
    upload_file("interactions.csv", "items", "items")

def create_dataset_group():
    response = personalize.create_dataset_group(name = 'dataset group name')
    dsg_arn = response['datasetGroupArn']

    description = personalize.describe_dataset_group(datasetGroupArn = dsg_arn)['datasetGroup']

    while description.status != "ACTIVE":
        if description.status == "CREATE FAILED":
            logging.error("***** Dataset Creation FAILED ******")
            logging.error(response['failureReason'])
            break
        time.sleep(10)
        description = personalize.describe_dataset_import_job(datasetImportJobArn = dsij_arn)['datasetImportJob']

    with open('schemaFile.json') as f:
        createSchemaResponse = personalize.create_schema(
            name = 'schema name',
            schema = f.read()
        )

    schema_arn = createSchemaResponse['schemaArn']

    items_response = personalize.create_dataset(
        name = 'datase_name',
        schemaArn = schema_arn,
        datasetGroupArn = dsg_arn,
        datasetType = 'dataset_type'
    )


def import_dataset(jobName, datasetArn, dataSource):
    response = personalize.create_dataset_import_job(
        jobName = 'YourImportJob',
        datasetArn = 'dataset_arn',
        dataSource = {'dataLocation':'s3://bucket/file.csv'},
        roleArn = 'role_arn'
    )

    dsij_arn = response['datasetImportJobArn']

    print ('Dataset Import Job arn: ' + dsij_arn)

    description = personalize.describe_dataset_import_job(datasetImportJobArn = dsij_arn)['datasetImportJob']

    while description.status != "ACTIVE":
        if description.status == "CREATE FAILED":
            logging.error("***** Dataset Creation FAILED ******")
            logging.error(response['failureReason'])
            break
        time.sleep(10)
        description = personalize.describe_dataset_import_job(datasetImportJobArn = dsij_arn)['datasetImportJob']

    # print('Name: ' + description['jobName'])
    # print('ARN: ' + description['datasetImportJobArn'])
    # print('Status: ' + description['status'])

def import_datasets():
    import_dataset("items", "...", "s3://bucket/items.csv")
    import_dataset("users", "...", "s3://bucket/users.csv")
    import_dataset("interactions", "...", "s3://bucket/items.csv")

def create_solution():
    create_solution_response = personalize.create_solution(
        name='solution name', 
        recipeArn= 'recipe arn', 
        datasetGroupArn = 'dataset group arn'
    )

    solution_arn = create_solution_response['solutionArn']

    # Use the solution ARN to get the solution status.
    solution_description = personalize.describe_solution(solutionArn = solution_arn)['solution']
    print('Solution status: ' + solution_description['status'])

    # Use the solution ARN to create a solution version.
    print ('Creating solution version')
    response = personalize.create_solution_version(solutionArn = solution_arn)
    solution_version_arn = response['solutionVersionArn']
    print('Solution version ARN: ' + solution_version_arn)

    # Use the solution version ARN to get the solution version status.
    solution_version_description = personalize.describe_solution_version(
        solutionVersionArn = solution_version_arn)['solutionVersion']
    print('Solution version status: ' + solution_version_description['status'])

def update_campaign(solution_version_arn):
    response = personalize.update_campaign(
        campaignArn = 'campaign arn',
        solutionVersionArn = solution_version_arn,
        minProvisionedTPS = 1,
    )

    arn = response['campaignArn']

description = personalize.describe_campaign(campaignArn = arn)['campaign']
print('Name: ' + description['name'])
print('ARN: ' + description['campaignArn'])
print('Status: ' + description['status'])



description = personalize.describe_campaign(campaignArn = arn)['campaign']
print('Name: ' + description['name'])
print('ARN: ' + description['campaignArn'])
print('Status: ' + description['status'])

upload_files_to_s3()
create_dataset_group()
import_datasets()
solution_version_arn = create_solution()
update_campaign(solution_version_arn)

