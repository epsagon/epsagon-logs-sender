/**
 * Forward logs to epsagon using AWS kinesis
 */

const util = require("util");
const zlib = require("zlib");
const AWS = require("aws-sdk");

const gunzip = util.promisify(zlib.gunzip);
const kinesisClient = new AWS.Kinesis({ region: process.env.EPSAGON_REGION });

const epsagonToken = process.env.EPSAGON_TOKEN;
const userLogsKinesis = process.env.EPSAGON_LOGS_KINESIS;

function epsagon_debug(error) {
  if ((process.env.EPSAGON_DEBUG || "").toUpperCase() === "TRUE") {
    // eslint-disable-next-line no-console
    console.log(error);
  }
}

function createRecordData(logEvents, transformEvent) {
  transformEvent = transformEvent || ((event) => event);
  logEvents = logEvents.map(transformEvent);

  return JSON.stringify({ token: epsagonToken, events: logEvents });
}

function createRecord(logsData) {
  return {
    Data: createRecordData(logsData.logEvents),
    PartitionKey: logsData.logStream,
    StreamName: userLogsKinesis,
  };
}

function mapToEpsagonRecord(record) {
  const data = record.kinesis.data;
  const zippedBuf = new Buffer.from(data, "base64");

  return gunzip(zippedBuf)
    .then((unzipped) => JSON.parse(unzipped.toString("utf-8")))
    .then((logsData) => logsData.messageType === "DATA_MESSAGE" && logsData)
    .then((logsData) => logsData && createRecord(logsData))
    .then((logsData) => {
      if (!logsData) {
        epsagon_debug("could not extract data, check message type");
      }

      return logsData;
    });
}

function forwardLambdaHandler(event, _) {
  const records = event.Records;

  for (const record of records) {
    const epsagonRecord = mapToEpsagonRecord(record);
    kinesisClient.putRecord(epsagonRecord, function (err, data) {
      epsagon_debug("Record sent");
      epsagon_debug(data);
      if (err) {
        epsagon_debug(err);
      }
    });
  }
}

module.exports = { forwardLambdaHandler };
