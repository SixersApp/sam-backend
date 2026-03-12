export function request(ctx) {
  return {
    operation: 'Invoke',
    payload: {
      field: ctx.stash.field || ctx.info.fieldName,
      result: ctx.prev.result,
    },
  };
}

export function response(ctx) {
  // Always return the original mutation result from the DB step
  // Even if scheduling fails, the DB work already committed
  return ctx.prev.result;
}
