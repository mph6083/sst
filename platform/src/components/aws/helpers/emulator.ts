import { Output } from "@pulumi/pulumi";

/**
 * True when the app targets AWS Emulator (`SST_AWS_EMULATOR=1`), a local emulator of the AWS APIs
 * that serves every service on the endpoint in `AWS_ENDPOINT_URL`.
 */
export const isAwsEmulator = ["1", "true"].includes(
  process.env.SST_AWS_EMULATOR ?? "",
);

/**
 * Moves a queue URL onto the emulator's own host.
 *
 * The emulator reports queue URLs on `sqs.<region>.amazonaws.com`. The AWS SDK for JavaScript sends
 * a request to the host in the queue URL, so code using the reported URL would call real AWS. The
 * emulator accepts the same path on its own endpoint.
 */
export function toEmulatorQueueUrl(url: Output<string>) {
  const endpoint = process.env.AWS_ENDPOINT_URL;
  if (!isAwsEmulator || endpoint === undefined || endpoint === "") {
    return url;
  }
  return url.apply(
    (reported) => `${endpoint.replace(/\/$/, "")}${new URL(reported).pathname}`,
  );
}

/** The host form AWS gives an HTTP API: `<apiId>.execute-api.<region>.amazonaws.com`. */
const EXECUTE_API_HOST = /^([a-z0-9]+)\.execute-api\.[a-z0-9-]+\.amazonaws\.com$/;

/**
 * Moves an HTTP API endpoint onto the emulator's own host.
 *
 * The emulator reports an HTTP API's endpoint on `<apiId>.execute-api.<region>.amazonaws.com`, a
 * host it does not serve. It serves the API under `/_aws/execute-api/<apiId>` on its own endpoint.
 * An endpoint already on another host is returned unchanged.
 */
export function toEmulatorApiUrl(url: Output<string>) {
  const endpoint = process.env.AWS_ENDPOINT_URL;
  if (!isAwsEmulator || endpoint === undefined || endpoint === "") {
    return url;
  }
  return url.apply((reported) => {
    const parsed = new URL(reported);
    const apiId = EXECUTE_API_HOST.exec(parsed.hostname)?.[1];
    if (apiId === undefined) {
      return reported;
    }
    const path = parsed.pathname === "/" ? "" : parsed.pathname;
    return `${endpoint.replace(/\/$/, "")}/_aws/execute-api/${apiId}${path}`;
  });
}
