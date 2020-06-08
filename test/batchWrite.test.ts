import TestContext, {
  documentClient,
  DataModel,
  KeySchema,
} from './helpers/TestContext';
import { v4 as uuid } from 'uuid';
import partition from 'lodash.partition';
import { BatchWriteOperation } from 'src';

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

test('should allow for bulk put operations to be performed', async () => {
  const putOperations = items.map((item) => ({ put: item }));

  await context.dao.batchWrite(putOperations);

  const results = await documentClient
    .batchGet({
      RequestItems: {
        [context.tableName]: { Keys: items.map((item) => ({ id: item.id })) },
      },
    })
    .promise();

  const returnedItems = results.Responses![context.tableName];

  expect(returnedItems.length).toEqual(items.length);
  expect(returnedItems).toEqual(expect.arrayContaining(items));
});

test('should allow for bulk delete operations to be performed', async () => {
  const result = await documentClient
    .batchWrite({
      RequestItems: {
        [context.tableName]: items.map((item) => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    })
    .promise();

  expect(result.UnprocessedItems).toEqual({});

  await context.dao.batchWrite(
    items.map((item) => ({
      delete: { id: item.id },
    })),
  );

  const results = await documentClient
    .batchGet({
      RequestItems: {
        [context.tableName]: { Keys: items.map((item) => ({ id: item.id })) },
      },
    })
    .promise();

  const returnedItems = results.Responses![context.tableName];

  expect(returnedItems.length).toEqual(0);
});

test('should allow for a mix of put and delete operations to be performed', async () => {
  const [itemsToStore, itemsToDelete] = partition(
    items,
    (item) => item.index % 2,
  );

  const storeItemsToBeDeletedResult = await documentClient
    .batchWrite({
      RequestItems: {
        [context.tableName]: itemsToDelete.map((item) => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    })
    .promise();

  expect(storeItemsToBeDeletedResult.UnprocessedItems).toEqual({});

  await context.dao.batchWrite([
    ...itemsToDelete.map((item) => ({
      delete: { id: item.id },
    })),
    ...itemsToStore.map((item) => ({
      put: item,
    })),
  ]);

  const results = await documentClient
    .batchGet({
      RequestItems: {
        [context.tableName]: { Keys: items.map((item) => ({ id: item.id })) },
      },
    })
    .promise();

  const returnedItems = results.Responses![context.tableName];

  expect(returnedItems.length).toEqual(itemsToStore.length);
  expect(returnedItems).toEqual(expect.arrayContaining(itemsToStore));
});

test('should reject if the size of the operation is over 25', () => {
  const operations: BatchWriteOperation<DataModel, KeySchema>[] = [];

  for (let i = 0; i < 26; i++) {
    operations.push({ put: items[0] });
  }

  return expect(context.dao.batchWrite(operations)).rejects.toThrow(
    /Cannot send more than 25 operations/,
  );
});

test('should return unprocessed items if there are any', async () => {
  const [itemsToStore, itemsToDelete] = partition(
    items,
    (item) => item.index % 2,
  );

  jest.spyOn(documentClient, 'batchWrite').mockReturnValue({
    promise: () =>
      Promise.resolve({
        UnprocessedItems: {
          [context.tableName]: [
            {
              PutRequest: {
                Item: items[0],
              },
            },
            {
              DeleteRequest: {
                Key: { id: items[1].id },
              },
            },
          ],
        },
      }),
  } as any);

  const { unprocessedItems } = await context.dao.batchWrite([
    ...itemsToDelete.map((item) => ({
      delete: { id: item.id },
    })),
    ...itemsToStore.map((item) => ({
      put: item,
    })),
  ]);

  expect(unprocessedItems).toEqual([
    { put: items[0] },
    { delete: { id: items[1].id } },
  ]);
});
