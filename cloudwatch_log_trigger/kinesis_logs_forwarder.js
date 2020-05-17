/**
 * Forward logs to epsagon using AWS kinesis
 */

const util = require("util");
const zlib = require("zlib");
const AWS = require("aws-sdk");

const epsagonToken = process.env.EPSAGON_TOKEN;
const userLogsKinesis = process.env.EPSAGON_LOGS_KINESIS;
const gunzip = util.promisify(zlib.gunzip);
const kinesisClient = new AWS.Kinesis({
  region: process.env.EPSAGON_REGION,
  accessKeyId: process.env.EPSAGON_AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.EPSAGON_AWS_SECRET_ACCESS_KEY,
});

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
    PartitionKey: epsagonToken,
  };
}

async function mapToEpsagonRecord(record) {
  const data = record.kinesis.data;
  const decoded = new Buffer.from(data, "base64");
  const unzipped = await gunzip(decoded);
  const logsData = JSON.parse(unzipped.toString("utf-8"));

  if (logsData.messageType !== "DATA_MESSAGE") {
    epsagon_debug("invalid messageType");
    return;
  }

  return createRecord(logsData);
}

async function forwardLambdaHandler(event, _) {
  const records = await Promise.all(event.Records.map(mapToEpsagonRecord));
  const filtered = records.filter((record) => record);

  epsagon_debug(records);
  epsagon_debug(userLogsKinesis);

  if (filtered.length === 0) return;

  try {
    const data = await kinesisClient
      .putRecords({
        Records: filtered,
        StreamName: userLogsKinesis,
      })
      .promise();

    epsagon_debug("Record sent");
    epsagon_debug(data);
  } catch (e) {
    epsagon_debug(e);
    return;
  }
}

module.exports = { forwardLambdaHandler };
