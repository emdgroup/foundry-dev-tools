import boto3
from foundry_dev_tools import FoundryContext, Config

# This will make sure config is only loaded from environment variables
ctx = FoundryContext(config=Config())
print(f"Running as user {ctx.multipass.get_user_info()['username']}")

fdt_s3_client = ctx.s3.get_boto3_client()
source_s3_client = boto3.client('s3')


def lambda_handler(event, context):
    print(f"{event=}")

    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]  # dataset_rid/<subpaths>/file

        splitted = object_key.split("/")
        dataset_rid = splitted[0]  # extract target dataset_rid
        dataset_path = "/".join(splitted[1:])

        response = source_s3_client.get_object(Bucket=bucket_name, Key=object_key)

        fdt_s3_client.upload_fileobj(response['Body'], dataset_rid, dataset_path)
