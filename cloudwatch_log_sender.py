"""Epsagon Logs Parser"""

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

AWS_ID = os.environ.get('AWS_SECRET_ID')
AWS_KEY = os.environ.get('AWS_SECRET_KEY')
REGION = os.environ.get('REGION')
KINESIS_NAME = os.environ.get('EPSAGON_KINESIS')
REGEX = re.compile(
    '|'.join([f'.*{pattern}.*' for pattern in FILTER_PATTERNS]),
    re.DOTALL
)
kinesis = boto3.client(
    'kinesis',
    aws_access_key_id=AWS_ID,
    aws_secret_access_key=AWS_KEY,
    region_name=REGION
)


def filter_events(record_data, partition_key):
    """
    Filter events relevant for Epsagon.
    :param record_data: Record data that holds the vents.
    :param partition_key: The record's partition key.
    :return: dict / None.
    """
    if record_data['messageType'] == 'DATA_MESSAGE':
        events = []
        for event in record_data['logEvents']:
            if REGEX.match(event['message']) is not None:
                events.append(event)
        if events:
            record_data['logEvents'] = events
            return {
                'Data': gzip.compress(json.dumps(record_data).encode('ascii')),
                'PartitionKey': partition_key
            }
    return


def handler(event, _):
    """
    Send filtered CloudWatch logs to Epsagon Kinesis.
    :param event: The triggered event from Kinesis.
    """
    try:
        records_to_send = []
        for record in event['Records']:
            partition_key = record['kinesis']['partitionKey']
            compressed_record_data = record['kinesis']['data']
            record_data = json.loads(
                gzip.decompress(
                    base64.b64decode(compressed_record_data)
                )
            )
            filtered_events = filter_events(record_data, partition_key)
            if filtered_events:
                records_to_send.append(filtered_events)

        kinesis.put_records(StreamName=KINESIS_NAME, Records=records_to_send)
    except Exception as e:
        print(e)

    return True
