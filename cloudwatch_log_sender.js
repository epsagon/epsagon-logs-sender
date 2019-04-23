var zlib = require('zlib');
var util = require('util');
var AWS = require('aws-sdk');


const FILTER_PATTERNS = [
        'REPORT', 'Task timed out', 'Process exited before completing', 'Traceback',
        'module initialization error:', 'Unable to import module', 'errorMessage',
        '.java:0', '.java:1', '.java:2', '.java:3', '.java:4', '.java:5', '.java:6',
        '.java:7', '.java:8', '.java:9'
    ];
const REGEX = new RegExp(FILTER_PATTERNS.map(function(item) {
        return util.format('.*%s.*', item)
    }).join('|'));

const KINESIS_CLIENT = new AWS.Kinesis({
    region: process.env.EPSAGON_REGION,
    accessKeyId: process.env.EPSAGON_AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.EPSAGON_AWS_SECRET_ACCESS_KEY
});

function epsagon_debug(error) {
    if ((process.env.EPSAGON_DEBUG || '').toUpperCase() === 'TRUE') {
        // eslint-disable-next-line no-console
        console.log(error);
    }
}

function forwardLogs(event) {
    return new Promise((resolve, reject) => {
        var zippedInput = new Buffer(event.awslogs.data, 'base64');
        zlib.gunzip(zippedInput, function (e, buffer) {
            if (e) {
                epsagon_debug(e);
                resolve();
                return;
            }

            var awslogsData = JSON.parse(buffer.toString('utf-8'));

            if (awslogsData.messageType !== 'DATA_MESSAGE') {
                epsagon_debug('Message other than DATA_MESSAGE received');
                resolve();
                return;
            }
            
            var forwadedMsgs = [];

            awslogsData.logEvents.forEach(function (log, idx, arr) {
                if (log.message.match(REGEX)) {
                    forwadedMsgs.push(log)
                }
            });

            if (forwadedMsgs.length != 0) {
                awslogsData.logEvents = forwadedMsgs;
                awslogsData.subscriptionFilters = [];
                awslogsData.subscriptionFilters.push(util.format('Epsagon#%s#%s', awslogsData.owner, process.env.AWS_REGION));

                zlib.gzip(new Buffer(JSON.stringify(awslogsData), 'ascii'), function (e, buffer) {
                    if (e) {
                        epsagon_debug('Failed compressing result');
                        resolve();
                        return;
                    }

                    // Forward to Epsagon Kinesis
                    try {
                        var params = {
                          Data: buffer,
                          PartitionKey: awslogsData.logStream,
                          StreamName: process.env.EPSAGON_KINESIS_NAME
                        };

                        KINESIS_CLIENT.putRecord(params, function(err, data) {
                          if (err) {
                            epsagon_debug(err);
                          }
                          resolve();
                        });
                    }
                    catch (e) {
                        epsagon_debug(e);
                        resolve();
                    }     
                })
            }
        });
    });
}
