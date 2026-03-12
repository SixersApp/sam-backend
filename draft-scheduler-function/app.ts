import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

const sqsClient = new SQSClient({});
const AUTOPICK_QUEUE_URL = process.env.AUTOPICK_QUEUE_URL!;

interface SchedulerEvent {
  field: string;
  result: any;
}

async function scheduleAutoPick(leagueId: string, expiresAt: string, expectedPickNumber: number): Promise<void> {
  const delaySeconds = Math.max(0, Math.floor((new Date(expiresAt).getTime() - Date.now()) / 1000));

  await sqsClient.send(new SendMessageCommand({
    QueueUrl: AUTOPICK_QUEUE_URL,
    MessageBody: JSON.stringify({
      action: 'autoPick',
      leagueId,
      expectedPickNumber,
    }),
    DelaySeconds: Math.min(delaySeconds, 900),
  }));

  console.log(`Scheduled autopick pick-${expectedPickNumber} with ${delaySeconds}s delay`);
}

async function scheduleEndDraft(leagueId: string): Promise<void> {
  await sqsClient.send(new SendMessageCommand({
    QueueUrl: AUTOPICK_QUEUE_URL,
    MessageBody: JSON.stringify({
      action: 'endDraft',
      leagueId,
    }),
    DelaySeconds: 0,
  }));

  console.log(`Scheduled endDraft for league ${leagueId}`);
}

export const lambdaHandler = async (event: SchedulerEvent): Promise<any> => {
  console.log('DraftScheduler event:', JSON.stringify(event));
  const { field, result } = event;

  try {
    if (field === 'postDraftPick' || field === 'autoPickDraftPlayer') {
      if (result.draftComplete) {
        await scheduleEndDraft(result.leagueId);
      } else if (result.nextTeamId && result.nextPickExpiresAt) {
        const nextPickNumber = result.pick.pickNumber + 1;
        await scheduleAutoPick(result.leagueId, result.nextPickExpiresAt, nextPickNumber);
      }
    } else if (field === 'startDraft') {
      if (result.pickExpiresAt) {
        await scheduleAutoPick(result.leagueId, result.pickExpiresAt, 1);
      }
    }
  } catch (err) {
    // Log but don't fail — the DB mutation already committed
    console.error('Scheduling error:', err);
  }

  return result;
};
