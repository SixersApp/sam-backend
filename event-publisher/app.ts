import { SignatureV4 } from '@smithy/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import type { SQSEvent } from 'aws-lambda';

const APPSYNC_EVENTS_ENDPOINT = process.env.APPSYNC_EVENTS_ENDPOINT!;
const AWS_REGION = process.env.AWS_REGION!;

interface MatchEventMessage {
  channel: string;
  event: any;
}

async function publishToChannel(channel: string, events: any[]) {
  const url = new URL(APPSYNC_EVENTS_ENDPOINT);
  const body = JSON.stringify({
    channel,
    events: events.map(e => JSON.stringify(e)),
  });

  const request = new HttpRequest({
    method: 'POST',
    hostname: url.hostname,
    path: '/event',
    headers: {
      'Content-Type': 'application/json',
      host: url.hostname,
    },
    body,
  });

  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region: AWS_REGION,
    service: 'appsync',
    sha256: Sha256,
  });

  const signed = await signer.sign(request);
  const response = await fetch(`https://${url.hostname}/event`, {
    method: 'POST',
    headers: signed.headers as Record<string, string>,
    body,
  });

  if (!response.ok) {
    const text = await response.text();
    console.error('AppSync Events publish failed:', response.status, text);
    throw new Error(`AppSync Events publish failed: ${response.status}`);
  }
}

export const lambdaHandler = async (event: SQSEvent): Promise<void> => {
  for (const record of event.Records) {
    const message: MatchEventMessage = JSON.parse(record.body);
    console.log('Publishing match event:', JSON.stringify(message));

    await publishToChannel(message.channel, [message.event]);
  }
};
