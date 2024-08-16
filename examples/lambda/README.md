
# Foundry DevTools on AWS Lambda Example

Foundry DevTools can be used a utility to feed data into Foundry on an event-trigger basis.
This page describes an example where Foundry DevTools is deployed as a lambda function and copies objects from an
S3 Bucket to a Foundry dataset, whenever new objects are uploaded to the bucket.

![architecture_dark](../../docs/pictures/aws_lambda_example_dark.svg#gh-dark-mode-only)
![architecture_light](../../docs/pictures/aws_lambda_example_light.svg#gh-light-mode-only)

## Pre-requisites

This example uses pixi as package manager. Pixi manages all other dependencies, except `docker`.
This example needs a Third-Party-Application (TPA) registration in Foundry. 
The TPA Service User needs -editor permissions on the project where the target dataset resides in.


## Build Lambda Docker Image

You will need a working installation of `docker`.

```shell
pixi run --environment ci docker-build-lambda
```

## Setup AWS

Make sure your shell has valid AWS Credentials (either by exporting as environment variable or through SSO Login). 
Export Region and Account Id.
Replace 123456789 with your Account Id.

```shell
export AWS_REGION="eu-central-1" && \
export AWS_ACCOUNT_ID="123456789"
```


## Deploy CloudFormation with ECR to store Lambda Docker Image

Make sure your shell has valid AWS Credentials.

```shell
pixi run --environment ci cfn-deploy-staging
```

## Push Lambda Image

Login to ECR and push the docker image.

```shell
pixi run --environment ci docker-login && pixi run --environment ci docker-push-lambda
```

## Deploy Lambda with CloudFormation

Note, that the TPA credentials will be stored in the Lambda environment variables.
If your AWS Account is shared it is recommended to leverage AWS Secrets Manager.

Export your TPA Credentials and Foundry Hostname in the shell session

```shell
 export  FOUNDRY_STACK_HOSTNAME="stack.palantirfoundry.com" \
export FOUNRDY_TPA_CLIENT_ID="client_id" \
export FOUNDRY_TPA_CLIENT_SECRET="client_secret"
```

Deploy the Cloudformation Template

```shell
pixi run --environment ci cfn-deploy-lambda
```

If your Lambda should run in a VPC to reach Foundry, pass the VPC Id and Subnet Ids as well:

```shell
VPC_ID="vpc-123456" \
SUBNET_ID1="subnet-12345" \
SUBNET_ID2="subnet-12345" \
pixi run --environment ci cfn-deploy-lambda
```

## Upload sample object

The lambda will move the object to the dataset that you specify as TARGET_DATASET_RID.

```shell
TARGET_DATASET_RID="ri.foundry.main.dataset.1234" \
pixi run --environment ci upload-sample-object
```