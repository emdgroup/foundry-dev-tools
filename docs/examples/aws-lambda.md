# Deploy as AWS Lambda function

Foundry DevTools can be used a utility to feed data into Foundry on an event-trigger basis.
This page describes an example where Foundry DevTools is deployed as a lambda function and copies objects from an
S3 Bucket to a Foundry dataset, whenever new objects are uploaded to the bucket.
The full example including the CloudFormation templates can be found in the GitHub repository in the examples folder.


## Architecture
```{image} /pictures/aws_lambda_example_light.svg
---
class: only-light
---
```

```{image} /pictures/aws_lambda_example_dark.svg
---
class: only-dark
---
```

## Full Example

You can find the full example including a CloudFormation templates in the repository examples [folder](https://github.com/emdgroup/foundry-dev-tools/tree/master/examples/lambda).