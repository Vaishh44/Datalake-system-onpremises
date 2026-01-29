import boto3
from botocore.exceptions import ClientError

def test_upload():
    endpoint = 'http://localhost:9878'
    print(f"Connecting to Ozone S3 Gateway at {endpoint}...")
    
    s3 = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id='minioadmin',
                      aws_secret_access_key='minioadmin',
                      region_name='us-east-1')

    bucket_name = 'warehouse'
    key_name = 'test_write.txt'
    
    try:
        # Check if bucket exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except ClientError as e:
        print(f"Error checking bucket: {e}")
        return

    try:
        print("Attempting to put object...")
        s3.put_object(Bucket=bucket_name, Key=key_name, Body=b'Hello Ozone!')
        print("SUCCESS: Object written successfully!")
    except ClientError as e:
        print(f"FAILURE: Write failed with error: {e}")

if __name__ == "__main__":
    test_upload()
