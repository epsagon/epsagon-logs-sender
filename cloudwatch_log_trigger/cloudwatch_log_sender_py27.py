"""
Epsagon Logs Parser, parse CloudWatch Logs event

1. Add this code snippet, and call forward_logs_to_epsagon(event) with the original event.
2. Set for your Lambda the following enviroment variables:
	- EPSAGON_REGION: us-east-1
	- EPSAGON_KINESIS_NAME: logs-sender-logs-stream-kinesis-us-east-1-production
	- EPSAGON_AWS_ROLE: xxx
"""

import json
import gzip
import base64
import os
import re
import traceback
from StringIO import StringIO
import boto3

FILTER_PATTERNS = (
    'REPORT', 'Task timed out', 'Process exited before completing', 'Traceback',
    'module initialization error:', 'Unable to import module', 'errorMessage',
    '.java:0', '.java:1', '.java:2', '.java:3', '.java:4', '.java:5', '.java:6',
    '.java:7', '.java:8', '.java:9'
)


AWS_ROLE = os.environ.get('EPSAGON_AWS_ROLE').strip()
REGION = os.environ.get('EPSAGON_REGION').strip()
KINESIS_NAME = os.environ.get('EPSAGON_KINESIS_NAME').strip()
REGEX = re.compile(
    '|'.join(['.*{}.*'.format(pattern) for pattern in FILTER_PATTERNS]),
    re.DOTALL
)

sts = boto3.client('sts')
assumed_role = sts.assume_role(
    RoleArn=AWS_ROLE,
    RoleSessionName='EpsagonRole'
)

credentials = assumed_role['Credentials']
kinesis = boto3.client(
    'kinesis',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
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
            ['Epsagon#{}#{}'.format(record_data['owner'], os.getenv('AWS_REGION'))]
        )
        events = []
        epsagon_debug('Found total of {} events'.format(len(original_events)))
        epsagon_debug('Original events: {}'.format(original_events))
        for event in original_events:
            if REGEX.match(event['message']) is not None:
                events.append(event)
        epsagon_debug('Filtered total of {} events.'.format(len(events)))
        if events:
            record_data['logEvents'] = events
            out = StringIO()
            with gzip.GzipFile(fileobj=out, mode='w') as f:
               f.write(json.dumps(record_data).encode('ascii'))

            record = {
                'Data': out.getvalue(),
                'PartitionKey': partition_key
            }
    return record


def forward_logs_to_epsagon(event):
    """
    Send filtered CloudWatch logs to Epsagon Kinesis.
    :param event: The triggered event from CloudWatch logs.
    """
    try:
        logs_data_decoded = event['awslogs']['data'].decode('base64')
        logs_data_unzipped = gzip.GzipFile(fileobj=StringIO(logs_data_decoded)).read()
        record_data = json.loads(logs_data_unzipped)

        filtered_event = filter_events(record_data)
        if not filtered_event:
            epsagon_debug('No logs match')
            return False

        kinesis.put_record(
            StreamName=KINESIS_NAME,
            Data=filtered_event['Data'],
            PartitionKey=filtered_event['PartitionKey'],
        )

    except Exception as err:
        epsagon_debug('Encountered error: {}'.format(err))
        epsagon_debug(traceback.format_exc())

    return True
