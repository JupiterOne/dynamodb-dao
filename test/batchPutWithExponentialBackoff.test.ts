import { BatchGetCommand } from '@aws-sdk/lib-dynamodb';
import { randomUUID as uuid } from 'crypto';
import mockLogger from './helpers/mockLogger';
import TestContext, { documentClient } from './helpers/TestContext';

let context: TestContext;
const items: any[] = [];

const testHashKey = uuid();

beforeAll(async () => {
  context = await TestContext.setup();

  for (let i = 0; i < 40; i++) {
    // put data into dynamodb
    const item = {
      id: '' + i,
      index: i,
      test: testHashKey,
    };

    items.push(item);
  }
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test('should allow for bulk put operations to be performed', async () => {
  await context.dao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items,
  });

  const results = await documentClient.send(
    new BatchGetCommand({
      RequestItems: {
        [context.tableName]: { Keys: items.map((item) => ({ id: item.id })) },
      },
    })
  );

  const returnedItems = results.Responses![context.tableName];

  expect(returnedItems.length).toEqual(items.length);
  expect(returnedItems).toEqual(expect.arrayContaining(items));
});
