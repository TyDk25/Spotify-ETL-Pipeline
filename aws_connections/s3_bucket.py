import boto3
import pathlib


def upload_file_to_s3(file_name, object_name=None):
    if object_name is None:
        object_name = pathlib.Path(file_name).name
    s3 = boto3.client('s3')
    bucket_name = 'ds-spotify-extraction-bucket'
    if not s3.list_buckets().get('Buckets'):
        print(f'Creating bucket {bucket_name}')
        s3.create_bucket(Bucket=bucket_name)
    try:
        s3.upload_file(file_name,'ds-spotify-extraction-bucket', object_name)
    except Exception as e:
        print(f'Error uploading file {file_name} to bucket {bucket_name}: {e}')
