import { SignatureV4 } from '@smithy/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import type { SQSEvent } from 'aws-lambda';

interface AutoPickMessage {
  action: 'autoPick' | 'endDraft';
  leagueId: string;
  expectedPickNumber?: number;
}

const APPSYNC_ENDPOINT = process.env.APPSYNC_ENDPOINT!;
const AWS_REGION = process.env.AWS_REGION!;

const autoPickMutation = `
  mutation AutoPick($input: AutoPickInput!) {
    autoPickDraftPlayer(input: $input) {
      pick {
        teamId
        playerId
        roundNumber
        pickNumber
        leagueId
        timestamp
      }
      nextTeamId
      nextPickExpiresAt
      leagueId
    }
  }
`;

const endDraftMutation = `
  mutation EndDraft($leagueId: String!) {
    endDraft(leagueId: $leagueId) {
      leagueId
      status
    }
  }
`;

async function callAppSyncMutation(query: string, variables: Record<string, any>): Promise<any> {
  const url = new URL(APPSYNC_ENDPOINT);

  const body = JSON.stringify({ query, variables });

  const request = new HttpRequest({
    method: 'POST',
    hostname: url.hostname,
    path: url.pathname,
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

  const signedRequest = await signer.sign(request);

  const response = await fetch(APPSYNC_ENDPOINT, {
    method: 'POST',
    headers: signedRequest.headers as Record<string, string>,
    body,
  });

  return response.json();
}

export const lambdaHandler = async (event: SQSEvent): Promise<void> => {
  for (const record of event.Records) {
    const message: AutoPickMessage = JSON.parse(record.body);
    console.log('AutoPickInvoker message:', JSON.stringify(message));

    if (message.action === 'endDraft') {
      const result = await callAppSyncMutation(endDraftMutation, { leagueId: message.leagueId });
      console.log('EndDraft AppSync response:', JSON.stringify(result));
      if (result.errors) {
        console.error('EndDraft mutation errors:', JSON.stringify(result.errors));
      }
    } else {
      const result = await callAppSyncMutation(autoPickMutation, {
        input: { leagueId: message.leagueId, expectedPickNumber: message.expectedPickNumber },
      });
      console.log('AutoPick AppSync response:', JSON.stringify(result));
      if (result.errors) {
        const isExpected = result.errors.some((e: any) => e.message?.includes('Pick already made'));
        if (!isExpected) {
          console.error('AutoPick mutation errors:', JSON.stringify(result.errors));
        }
      }
    }
  }
};
