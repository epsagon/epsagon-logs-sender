"""Epsagon Logs Parser, parse CloudWatch Logs event"""

import json
import gzip
import base64
import os
import re
import traceback
import boto3

FILTER_PATTERNS = (
    'REPORT', 'Task timed out', 'Process exited before completing', 'Traceback',
    'module initialization error:', 'Unable to import module', 'errorMessage',
    '.java:0', '.java:1', '.java:2', '.java:3', '.java:4', '.java:5', '.java:6',
    '.java:7', '.java:8', '.java:9'
)


AWS_SECRET = os.environ.get('EPSAGON_AWS_SECRET_ACCESS_KEY').strip()
AWS_KEY = os.environ.get('EPSAGON_AWS_ACCESS_KEY_ID').strip()
REGION = os.environ.get('EPSAGON_REGION').strip()
KINESIS_NAME = os.environ.get('EPSAGON_KINESIS_NAME').strip()
REGEX = re.compile(
    '|'.join([f'.*{pattern}.*' for pattern in FILTER_PATTERNS]),
    re.DOTALL
)

kinesis = boto3.client(
    'kinesis',
    aws_access_key_id=AWS_ID,
    aws_secret_access_key=AWS_KEY,
    region_name=REGION,
)


def epsagon_debug(message):
    if os.getenv('EPSAGON_DEBUG', '').upper() == 'TRUE':
        print(message)


def filter_events(record_data):
    """
    Filter events relevant for Epsagon.
    :param record_data: Record data that holds the vents.
    :return: dict / None.
    """
    record = None
    if record_data['messageType'] == 'DATA_MESSAGE':
        original_events = record_data['logEvents']
        partition_key = record_data['logStream']
        record_data['subscriptionFilters'] = (
            [f'Epsagon#{record_data["owner"]}#{os.getenv("AWS_REGION")}']
        )
        events = []
        epsagon_debug(f'Found total of {len(original_events)} events')
        epsagon_debug(f'Original events: {original_events}')
        for event in original_events:
            if REGEX.match(event['message']) is not None:
                events.append(event)
        epsagon_debug(f'Filtered total of {len(events)} events.')
        if events:
            record_data['logEvents'] = events
            record = {
                'Data': gzip.compress(json.dumps(record_data).encode('ascii')),
                'PartitionKey': partition_key
            }
    return record


def forward_logs_to_epsagon(event):
    """
    Send filtered CloudWatch logs to Epsagon Kinesis.
    :param event: The triggered event from CloudWatch logs.
    """
    try:
        record_data = json.loads(
            gzip.decompress(
                base64.b64decode(event['awslogs']['data'])
            )
        )
        filtered_event = filter_events(record_data)
        if not filtered_event:
            epsagon_debug('No logs match')
            return False

        original_access_key = os.environ.pop('AWS_ACCESS_KEY_ID')
        original_secret_key = os.environ.pop('AWS_SECRET_ACCESS_KEY')
        original_region = os.environ.pop('AWS_REGION')
        os.environ['AWS_ACCESS_KEY_ID'] = AWS_KEY
        os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET
        os.environ['AWS_REGION'] = REGION
        try:
            kinesis.put_record(
                StreamName=KINESIS_NAME,
                Data=filtered_event['Data'],
                PartitionKey=filtered_event['PartitionKey'],
            )
        finally:
            os.environ['AWS_ACCESS_KEY_ID'] = original_access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = original_secret_key
            os.environ['AWS_REGION'] = original_region

    except Exception as err:
        epsagon_debug('Encountered error: {}'.format(err))
        epsagon_debug(traceback.format_exc())

    return True
