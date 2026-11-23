"use strict";

/**
 * Live function stub for AWS Emulator.
 *
 * The emulator runs function code in a sandbox with no network and no native binaries, so the Go
 * bridge stub cannot run there. This stub speaks the same packet protocol through the AWS SDK the
 * emulator supplies: packets go to the dev CLI through an SQS FIFO queue, and the response is read
 * from a DynamoDB table. Plain CommonJS with no bundled dependencies, because the sandbox lacks
 * most Node built-ins.
 */
const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");
const { DynamoDBClient, QueryCommand, GetItemCommand } = require("@aws-sdk/client-dynamodb");

/** Packet types, mirroring bridge.MessageType in the CLI. */
const MESSAGE_INIT = 0;
const MESSAGE_NEXT = 2;
const MESSAGE_RESPONSE = 3;
const MESSAGE_ERROR = 4;

/** Raw bytes per packet, matching BUFFER_SIZE in the CLI bridge. */
const CHUNK_BYTES = 128 * 1024;
const SQS_BATCH_LIMIT = 10;
/**
 * The wait between checks for the response starts here and grows by POLL_GROWTH after each check.
 * The emulator allows one invocation 256 AWS SDK calls, so a fixed interval would run out a few
 * seconds in. Growing by 5% covers a 15 minute timeout in about 155 checks, and the wait added to
 * an invocation stays under 5% of its duration.
 */
const INITIAL_POLL_INTERVAL_MS = 25;
const POLL_GROWTH = 1.05;
/** How long to wait for the CLI's ping before reporting that `sst dev` is not running. */
const PING_TIMEOUT_MS = 16000;
const PING_CHECK_INTERVAL_MS = 1000;
/** Leave time to return an error before the function itself times out. */
const DEADLINE_MARGIN_MS = 300;
/**
 * The emulator stops a function this long after it starts, whatever its timeout, while telling it
 * that the whole timeout is left. The stub keeps its own clock, so the worker gets the deadline
 * that really applies and a slow invocation still ends with the stub's own error.
 */
const EMULATOR_RUN_LIMIT_MS = 30000;
/** Allowance for clock difference between the emulator's sandbox and the machine running the CLI. */
const PING_CLOCK_SLACK_MS = 5000;

/** Variables that describe the sandbox, not the function, and must not reach the local worker. */
const ENV_BLOCKLIST = new Set([
  "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
  "AWS_LAMBDA_LOG_GROUP_NAME",
  "AWS_LAMBDA_LOG_STREAM_NAME",
  "LD_LIBRARY_PATH",
  "LAMBDA_TASK_ROOT",
  "AWS_LAMBDA_RUNTIME_API",
  "AWS_EXECUTION_ENV",
  "AWS_XRAY_DAEMON_ADDRESS",
  "AWS_LAMBDA_INITIALIZATION_TYPE",
  "PATH",
  "PWD",
  "LAMBDA_RUNTIME_DIR",
  "LANG",
  "NODE_PATH",
  "SHLVL",
  "AWS_XRAY_DAEMON_PORT",
  "AWS_XRAY_CONTEXT_MISSING",
  "_HANDLER",
]);

const sqs = new SQSClient({});
const dynamo = new DynamoDBClient({});

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/** Splits a payload into bridge packets. */
function toPackets(type, source, id, payload) {
  const bytes = Buffer.from(payload, "utf8");
  const packets = [];
  let index = 0;
  for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
    packets.push({ type, source, id, index, data: bytes.subarray(offset, offset + CHUNK_BYTES).toString("base64"), final: false });
    index += 1;
  }
  packets.push({ type, source, id, index, data: "", final: true });
  return packets;
}

async function sendPackets(queueUrl, workerID, packets) {
  for (let offset = 0; offset < packets.length; offset += SQS_BATCH_LIMIT) {
    const batch = packets.slice(offset, offset + SQS_BATCH_LIMIT);
    const result = await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: batch.map((packet, position) => ({
          Id: String(position),
          MessageBody: JSON.stringify(packet),
          MessageGroupId: workerID,
          MessageDeduplicationId: `${packet.id}-${packet.index}`,
        })),
      }),
    );
    if (result.Failed && result.Failed.length > 0) {
      throw new Error(`sst live bridge: could not queue the invocation: ${result.Failed[0].Message}`);
    }
  }
}

/** The time left before the invocation is stopped, by its own timeout or by the emulator's run limit. */
function getRemainingMs(context, startedAt) {
  return Math.min(context.getRemainingTimeInMillis(), EMULATOR_RUN_LIMIT_MS - (Date.now() - startedAt));
}

/** The invocation as the Lambda runtime API would hand it to a worker. */
function toRuntimeResponse(event, context, startedAt) {
  const body = JSON.stringify(event === undefined ? null : event);
  const headers = [
    "HTTP/1.1 200 OK",
    "Content-Type: application/json",
    `Content-Length: ${Buffer.byteLength(body, "utf8")}`,
    `Lambda-Runtime-Aws-Request-Id: ${context.awsRequestId}`,
    `Lambda-Runtime-Deadline-Ms: ${Date.now() + getRemainingMs(context, startedAt)}`,
    `Lambda-Runtime-Invoked-Function-Arn: ${context.invokedFunctionArn}`,
  ];
  if (process.env._X_AMZN_TRACE_ID) {
    headers.push(`Lambda-Runtime-Trace-Id: ${process.env._X_AMZN_TRACE_ID}`);
  }
  if (context.clientContext) {
    headers.push(`Lambda-Runtime-Client-Context: ${JSON.stringify(context.clientContext)}`);
  }
  if (context.identity) {
    headers.push(`Lambda-Runtime-Cognito-Identity: ${JSON.stringify(context.identity)}`);
  }
  return `${headers.join("\r\n")}\r\n\r\n${body}`;
}

/** Reads the response packets for a request, or returns undefined while they are incomplete. */
async function readResponse(table, requestID) {
  const items = [];
  let startKey;
  do {
    const page = await dynamo.send(
      new QueryCommand({
        TableName: table,
        KeyConditionExpression: "#id = :id",
        ExpressionAttributeNames: { "#id": "id" },
        ExpressionAttributeValues: { ":id": { S: `response:${requestID}` } },
        ConsistentRead: true,
        ExclusiveStartKey: startKey,
      }),
    );
    items.push(...(page.Items || []));
    startKey = page.LastEvaluatedKey;
  } while (startKey);

  if (items.length === 0 || !items.some((item) => item.final && item.final.BOOL === true)) {
    return undefined;
  }
  items.sort((left, right) => Number(left.idx.N) - Number(right.idx.N));
  const chunks = items.map((item) => Buffer.from((item.data && item.data.S) || "", "base64"));
  return { type: Number(items[0].type.N), body: Buffer.concat(chunks).toString("utf8") };
}

async function wasPingedSince(table, workerID, startedAt) {
  const found = await dynamo.send(
    new GetItemCommand({ TableName: table, Key: { id: { S: `ping:${workerID}` }, idx: { N: "0" } }, ConsistentRead: true }),
  );
  return found.Item !== undefined && Number(found.Item.at.N) >= startedAt - PING_CLOCK_SLACK_MS;
}

exports.handler = async (event, context) => {
  const arnParts = context.invokedFunctionArn.split(":");
  const queueUrl = `https://sqs.${arnParts[3]}.amazonaws.com/${arnParts[4]}/${process.env.SST_LIVE_QUEUE}`;
  const table = process.env.SST_LIVE_TABLE;
  const workerID = context.functionName;
  const requestID = context.awsRequestId;
  const startedAt = Date.now();

  const environment = Object.entries(process.env)
    .filter(([key]) => !ENV_BLOCKLIST.has(key) && !key.startsWith("_LAMBDA_"))
    .map(([key, value]) => `${key}=${value}`);

  await sendPackets(queueUrl, workerID, [
    ...toPackets(MESSAGE_INIT, workerID, `init-${requestID}`, JSON.stringify({ functionID: process.env.SST_FUNCTION_ID, environment })),
    ...toPackets(MESSAGE_NEXT, workerID, `next-${requestID}`, toRuntimeResponse(event, context, startedAt)),
  ]);

  let isPinged = false;
  let lastPingCheck = 0;
  let pollIntervalMs = INITIAL_POLL_INTERVAL_MS;
  while (getRemainingMs(context, startedAt) > DEADLINE_MARGIN_MS) {
    const response = await readResponse(table, requestID);
    if (response !== undefined) {
      if (response.type === MESSAGE_RESPONSE) {
        return response.body === "" ? undefined : JSON.parse(response.body);
      }
      if (response.type === MESSAGE_ERROR) {
        const details = response.body === "" ? {} : JSON.parse(response.body);
        const error = new Error(details.errorMessage || "The function failed in sst dev");
        error.name = details.errorType || "Error";
        if (Array.isArray(details.trace)) {
          error.stack = details.trace.join("\n");
        }
        throw error;
      }
    }
    if (!isPinged && Date.now() - lastPingCheck > PING_CHECK_INTERVAL_MS) {
      lastPingCheck = Date.now();
      isPinged = await wasPingedSince(table, workerID, startedAt);
      if (!isPinged && Date.now() - startedAt > PING_TIMEOUT_MS) {
        return { statusCode: 500, body: `sst dev is not running (worker: ${workerID})` };
      }
    }
    // Never sleep past the point where the timeout error below must still be returned.
    await sleep(Math.min(pollIntervalMs, Math.max(getRemainingMs(context, startedAt) - DEADLINE_MARGIN_MS, 0)));
    pollIntervalMs *= POLL_GROWTH;
  }
  throw new Error(`sst live bridge: no response from the local worker for ${workerID} before the function timeout`);
};
