import { util } from '@aws-appsync/utils';

export function request(ctx) {
  return {
    operation: 'Invoke',
    payload: {
      field: 'getDraftPicks',
      arguments: ctx.args,
    },
  };
}

export function response(ctx) {
  return ctx.result;
}
