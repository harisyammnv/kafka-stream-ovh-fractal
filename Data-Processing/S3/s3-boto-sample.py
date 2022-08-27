import boto3
import io
import toml


if __name__ == "__main__":

    config = toml.load("config.toml")
    # here we use boto3 to access the container through the Swift S3 API
    # so we have to define some attributes to provide an access to the swift container
    s3 = boto3.client('s3',
                      aws_access_key_id=config["S3"].get('s3_access_key'),
                      aws_secret_access_key=config["S3"].get('s3_secret_key'),
                      endpoint_url="https://s3.gra.cloud.ovh.net/",
                      region_name="gra")

    bytes_to_write = b"This a string to be written in a file."
    response = s3.list_buckets()['Buckets']
    for bucket in response:
        print('Bucket name: {}, Created on: {}'.format(bucket['Name'], bucket['CreationDate']))
    file = io.BytesIO(bytes_to_write)

    # write string through the S3 API with boto3
    
    s3.upload_fileobj(file, "test-data-wc", "test_write_string.txt")
