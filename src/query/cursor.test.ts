import {
  decodeQueryUntilLimitCursor,
  encodeQueryUntilLimitCursor,
} from './cursor';

test('#encodeQueryUntilLimitCursor should handle falsy "skip" and falsy "lastKey"', () => {
  expect(encodeQueryUntilLimitCursor('', 0)).toBe('0|');
});
test('#decodeQueryUntilLimitCursor should handle empty cursor', () => {
  expect(decodeQueryUntilLimitCursor(undefined)).toEqual({
    lastKey: undefined,
    skip: 0,
  });
});
test('#decodeQueryUntilLimitCursor should throw error for invalid skip in cursor', () => {
  expect(() => {
    decodeQueryUntilLimitCursor('blah');
  }).toThrow(/Invalid cursor/);
});
test('#decodeQueryUntilLimitCursor should throw error for invalid skip in cursor with pipe', () => {
  expect(() => {
    decodeQueryUntilLimitCursor('blah|blah');
  }).toThrow(/Invalid cursor/);
});
