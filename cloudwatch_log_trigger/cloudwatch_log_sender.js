var zlib = require('zlib');
var util = require('util');
var AWS = require('aws-sdk');


const PREFIX_PATTERNS = [
    'REPORT', 'Unable to import module'
    ];
const INCLUDES_PATTERNS = [
    'Task timed out', 'Process exited before completing',
    'Traceback', 'module initialization error:', 'errorMessage'
    ];

const FILTER_PATTERN = PREFIX_PATTERNS.map(function(item) {
        return util.format('%s.*', item)
    }).join('|')
const REGEX = new RegExp(FILTER_PATTERN + INCLUDES_PATTERNS.map(function(item) {
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

module.exports.forwardLogs = function forwardLogs(event) {
    return new Promise((resolve, reject) => {
        epsagon_debug('Attempting to forward logs');

        var zippedInput = new Buffer(event.awslogs.data, 'base64');
        epsagon_debug(util.format('Size before compression %d bytes', zippedInput.length));
        zlib.gunzip(zippedInput, function (e, buffer) {
            if (e) {
                epsagon_debug(e);
                resolve();
                return;
            }

            epsagon_debug(util.format('Size after decompression %d bytes', buffer.length));

            var awslogsData = JSON.parse(buffer.toString('utf-8'));

            if (awslogsData.messageType !== 'DATA_MESSAGE') {
                epsagon_debug('Message other than DATA_MESSAGE received');
                resolve();
                return;
            }
            
            var forwadedMsgs = [];

            epsagon_debug(util.format('Scanning %d lines', awslogsData.logEvents.length));
            
            awslogsData.logEvents.forEach(function (log, idx, arr) {
                if (log.message.slice(0, 100).match(REGEX)) {
                    epsagon_debug(util.format('Match found for line %d', idx));
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

                        epsagon_debug(util.format('About to forward %d records', forwadedMsgs.length));

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
