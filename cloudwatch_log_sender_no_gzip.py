"""Epsagon Logs Parser"""

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

DEGUG_STRING = 'DEBUG'
STAGE = os.environ.get('STAGE', '').strip()
AWS_ID = os.environ.get('AWS_ID').strip()
AWS_KEY = os.environ.get('AWS_KEY').strip()
REGION = os.environ.get('EPSAGON_REGION').strip()
CURRENT_REGION = os.environ.get('AWS_REGION').strip()
KINESIS_NAME = os.environ.get('EPSAGON_KINESIS').strip()
OVERRIDE_SUBSCRIPTIONS = os.environ.get('OVERRIDE_SUBSCRIPTIONS', '').strip()

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


def filter_events(record_data, partition_key):
    """
    Filter events relevant for Epsagon.
    :param record_data: Record data that holds the vents.
    :param partition_key: The record's partition key.
    :return: dict / None.
    """
    print_if_needed(f'record data: {record_data}')
    print_if_needed(f'partition key: {partition_key}')
    if record_data['messageType'] == 'DATA_MESSAGE':
        original_events = record_data['logEvents']
        if OVERRIDE_SUBSCRIPTIONS.lower() == 'true':
            record_data['subscriptionFilters'] = (
                [f'Epsagon#{record_data["owner"]}#{CURRENT_REGION}']
            )
        events = []
        print_if_needed(f'Found total of {len(original_events)} events')
        print_if_needed(f'Original events: {original_events}')
        for event in original_events:
            if REGEX.match(event['message']) is not None:
                events.append(event)
        print_if_needed(f'Filtered total of {len(events)} events.')
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
                base64.b64decode(compressed_record_data)
            )
            filtered_events = filter_events(record_data, partition_key)
            if filtered_events:
                records_to_send.append(filtered_events)

        original_access_key = os.environ.pop('AWS_ACCESS_KEY_ID')
        original_secret_key = os.environ.pop('AWS_SECRET_ACCESS_KEY')
        os.environ['AWS_ACCESS_KEY_ID'] = AWS_ID
        os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_KEY
        try:
            if records_to_send:
                print_if_needed(
                    f'Sending {len(records_to_send)} events to Kinesis'
                )
                kinesis.put_records(StreamName=KINESIS_NAME,
                                    Records=records_to_send)
        finally:
            os.environ['AWS_ACCESS_KEY_ID'] = original_access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = original_secret_key

    except Exception as e:
        print(traceback.format_exc())

    return True


def print_if_needed(message):
    if STAGE.lower() == DEGUG_STRING.lower():
        print(message)
