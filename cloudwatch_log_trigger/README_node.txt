1. Add all relevant code, globals and imports to your code.
2. Call forwardLogs with the event that triggered your Lambda.
	* Note - forwardLogs functions returns a Promise!
3. Set for your Lambda the following enviroment variables:
	- EPSAGON_REGION: us-east-1
	- EPSAGON_KINESIS_NAME: logs-sender-logs-stream-kinesis-us-east-1-production
	- EPSAGON_AWS_ACCESS_KEY_ID: xxx
	- EPSAGON_AWS_SECRET_ACCESS_KEY: xxx