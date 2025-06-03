import boto3
import os
import json

# Initialize AWS clients
s3 = boto3.client('s3')
redshift_data = boto3.client('redshift-data')
sns = boto3.client('sns')

# Environment variables
REDSHIFT_WORKGROUP = os.environ['REDSHIFT_WORKGROUP']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
S3_BUCKET = os.environ['S3_BUCKET']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']  # For success/failure notifications

def publish_notification(success, message):
    subject = "✅ Redshift Load Success" if success else "❌ Redshift Load FAILED"
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
    except Exception as e:
        print(f"Failed to publish SNS notification: {e}")

def lambda_handler(event, context):
    try:
        # Parse SNS message
        record = event['Records'][0]
        sns_message = json.loads(record['Sns']['Message'])
        s3_path = sns_message['copy_command']  # Expected format: s3://bucket/path/to/copy_command.txt

        # Parse bucket and key from s3_path
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1]

        # Read the COPY command from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        copy_command = obj['Body'].read().decode('utf-8').strip()

        print(f"Executing COPY command:\n{copy_command}")

        # Execute the COPY command
        response = redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=copy_command,
            WithEvent=True
        )

        success_message = (
            f"COPY command executed successfully.\n"
            f"Execution ID: {response['Id']}\n"
            f"Workgroup: {REDSHIFT_WORKGROUP}\n"
            f"S3: {s3_path}"
        )
        publish_notification(True, success_message)

        return {
            'statusCode': 200,
            'body': success_message
        }

    except Exception as e:
        error_message = (
            f"Failed to execute COPY command.\n"
            f"Error: {str(e)}\n"
            f"S3: {s3_path if 's3_path' in locals() else 'Unknown'}"
        )
        publish_notification(False, error_message)

        return {
            'statusCode': 500,
            'body': str(e)
        }
