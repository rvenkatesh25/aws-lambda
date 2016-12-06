# Replicating DynamoDB to BigQuery using lambda function

DynamoDB streams provide a changelog of updates to a table, order maintained per key. It is backed by a Kinesis like system, not sure exactly what is used. 
AWS Lambda integrates with these streams out-of-the-box[1], shielding the users from managing the stream sharding topology (parallel sibling shards vs parent-child shards)

Data is retained in the stream for 24 hours[2], and the lambda function is retried on failures until data is available. 
We need not integrate this with any other pub/sub system, the lambda function can directly be programmed to call BigQuery streaming inserts

[1] http://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html

[2] http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html

## Create a table with Streams enabled
- Go to AWS console for dynamodb
- Create a table
- Table settings -> Overview -> Manage Streams
- Enable Streams, select type "New Image" i.e. the entire new record is available in the stream after a write
- Follow the IAM role steps as per http://docs.aws.amazon.com/lambda/latest/dg/with-userapp-walkthrough-custom-events-create-test-function.html
- For TT: table `venky-test` created in `us-west-2` region

## Create Lambda function
- Compile this code with `mvn package`
- Upload to AWS
``` 
$ aws lambda create-function \
	--region us-west-2 \
	--function-name lambda-bq \
	--zip-file fileb://target/lambda-bq-1.0-SNAPSHOT.jar \
	--role arn:aws:iam::767177213176:role/venky-test-lambda \
	--handler example.DDBEventProcessor \
	--runtime java8 \
	--timeout 15 \
	--memory-size 512
{
    "CodeSha256": "Rq3+ZsREvu2oxemyMc0b6gnh2a4S3BDgc1urFk/Lts0=",
    "FunctionName": "lambda-bq",
    "CodeSize": 9435382,
    "MemorySize": 512,
    "FunctionArn": "arn:aws:lambda:us-west-2:767177213176:function:lambda-bq",
    "Version": "$LATEST",
    "Role": "arn:aws:iam::767177213176:role/venky-test-lambda",
    "Timeout": 15,
    "LastModified": "2016-12-06T17:57:19.215+0000",
    "Handler": "example.DDBEventProcessor",
    "Runtime": "java8",
    "Description": ""
}
```
- Lambda function invocations and logs are available at: https://us-west-2.console.aws.amazon.com/lambda/home?region=us-west-2#/functions/lambda-bq?tab=code
- NOTE: To enable CloudWatch logging, the IAM role should have a custom policy with this definition:
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
    ],
      "Resource": [
        "arn:aws:logs:*:*:*"
    ]
  }
 ]
}
```

## Enable BigQuery Permissions
- Create table with same schema
- Create a service account, grant permissions for the relevant BigQuery DataSet

## Play
- Create new records in DDB table
- TODO: this was still not writing to BQ table. WIP
