function invalidCursorError(cursor: string): Error {
  const err = new Error(
    `Invalid cursor for queryUntilLimitReached(...) function (cursor=${cursor})`
  );
  (err as any).retryable = false;
  return err;
}

export function encodeQueryUntilLimitCursor(
  lastKey: string | undefined,
  skip: number | undefined
): string {
  return `${skip || 0}|${lastKey || ''}`;
}

export function decodeQueryUntilLimitCursor(cursor: string | undefined): {
  skip: number;
  lastKey: string | undefined;
} {
  if (!cursor) {
    return {
      skip: 0,
      lastKey: undefined,
    };
  }

  const pos = cursor.indexOf('|');
  if (pos === -1) {
    throw invalidCursorError(cursor);
  }

  const skip = parseInt(cursor.substring(0, pos), 10);
  if (Number.isNaN(skip)) {
    throw invalidCursorError(cursor);
  }

  const lastKey = cursor.substring(pos + 1);
  return { skip, lastKey };
}
