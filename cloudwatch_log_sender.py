import json
import gzip
import base64
import os
import re
import boto3

FILTER_PATTERNS = (
    'REPORT', 'Task timed out', 'Process exited before completing', 'Traceback',
    'module initialization error:', 'Unable to import module', 'errorMessage',
    '.java:0', '.java:1', '.java:2', '.java:3', '.java:4', '.java:5', '.java:6',
    '.java:7', '.java:8', '.java:9'
)

AWS_ID = 'AKIAJNG7PBJNO2UE3CIQ'
AWS_KEY = '009IAN/wMCpGslVdkX2BoeGxyRZeZtgUgkbJNkVX'
AWS_REGION = 'us-east-1'
KINESIS_NAME = os.environ.get('EPSAGON_KINESIS')

kinesis = boto3.client(
    'kinesis',
    aws_access_key_id=AWS_ID,
    aws_secret_access_key=AWS_KEY,
    region_name=AWS_REGION
)


def handler(event, _):
    """
    Send filtered CloudWatch logs to Epsagon Kinesis.
    :param event: The triggered event from Kinesis.
    """
    regex = re.compile(
        '|'.join([f'.*{pattern}.*' for pattern in FILTER_PATTERNS]),
        re.DOTALL
    )
    records_to_send = []
    for record in event['Records']:
        compressed_record_data = record['kinesis']['data']
        record_data = json.loads(
            gzip.decompress(
                base64.b64decode(compressed_record_data)
            )
        )

        if record_data['messageType'] == 'DATA_MESSAGE':
            events = []
            for event in record_data['logEvents']:
                if regex.match(event['message']) is not None:
                    events.append(event)
            if not events:
                continue
                record_data['logEvents'] = events
            records_to_send.append(
                {
                    'Data': gzip.compress(
                        json.dumps(record_data).encode('ascii')),
                    'PartitionKey': record['kinesis']['partitionKey']
                }
            )
    kinesis.put_records(StreamName=KINESIS_NAME, Records=records_to_send)

    return True
