import { v4 as uuid } from 'uuid';
import TestContext, { documentClient, KeySchema } from './helpers/TestContext';

let context: TestContext;
const items: any[] = [];

const testHashKey = uuid();

beforeAll(async () => {
  context = await TestContext.setup();

  for (let i = 0; i < 10; i++) {
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

test('should allow for bulk get operations to be performed', async () => {
  await documentClient.batchWrite({
    RequestItems: {
      [context.tableName]: items.map((item) => ({
        PutRequest: {
          Item: item,
        },
      })),
    },
  });

  const { items: returnedItems } = await context.dao.batchGet(
    items.map((item) => ({ id: item.id }))
  );

  expect(returnedItems.length).toEqual(items.length);
  expect(returnedItems).toEqual(expect.arrayContaining(items));
});

test('should return unprocessed keys if there are any', async () => {
  jest.spyOn(documentClient, 'send').mockResolvedValue({
    UnprocessedKeys: {
      [context.tableName]: {
        Keys: [{ id: items[0].id }],
      },
    },
  } as never); // jest is wrong

  const { unprocessedKeys } = await context.dao.batchGet(
    items.map((item) => ({ id: item.id }))
  );

  expect(unprocessedKeys).toEqual([{ id: items[0].id }]);
});

test('should reject if the size of the operation is over 25', () => {
  const keys: KeySchema[] = [];

  for (let i = 0; i < 26; i++) {
    keys.push({ id: items[0].id });
  }

  return expect(context.dao.batchGet(keys)).rejects.toThrow(
    /Cannot fetch more than 25 items/
  );
});
