/**
 * encode start key into a base64 encoded string
 * that can be used for pagination
 */
export function encodeExclusiveStartKey<KeySchema>(obj: KeySchema): string {
  return Buffer.from(JSON.stringify(obj)).toString('base64');
}

/**
 * Decode the key the start key
 */
export function decodeExclusiveStartKey<KeySchema>(token: string): KeySchema {
  try {
    return JSON.parse(Buffer.from(token, 'base64').toString('utf8'));
  } catch (err) {
    throw new Error('Invalid pagination token provided');
  }
}
