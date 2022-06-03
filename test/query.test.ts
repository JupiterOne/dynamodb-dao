import chunk from 'lodash.chunk';
import pMap from 'p-map';
import { v4 as uuid } from 'uuid';
import { QueryInputWithLimit } from '../src/types';
import TestContext, { documentClient } from './helpers/TestContext';

let context: TestContext;
const items: any[] = [];

const testHashKey = uuid();

beforeAll(async () => {
  context = await TestContext.setup();

  const putRequests = [];

  for (let i = 0; i < 10; i++) {
    // put data into dynamodb
    const item = {
      id: '' + i,
      index: i,
      test: testHashKey,
    };

    items.push(item);
    putRequests.push({
      PutRequest: {
        Item: item,
      },
    });
  }

  await documentClient
    .batchWrite({
      RequestItems: {
        [context.tableName]: putRequests,
      },
    })
    .promise();
});

afterAll(() => {
  if (context) {
    // return context.teardown();
  }
});

test('should allow for data to be queried', async () => {
  const { indexName, dao } = context;

  const { items: results } = await dao.query({
    index: indexName,
    keyConditionExpression: 'test = :test',
    attributeValues: {
      ':test': testHashKey,
    },
  });

  expect(results).toEqual(items);
});

test('should allow for scanIndexForward to be set', async () => {
  const { indexName, dao } = context;

  const { items: results } = await dao.query({
    index: indexName,
    keyConditionExpression: 'test = :test',
    scanIndexForward: false,
    attributeValues: {
      ':test': testHashKey,
    },
  });

  expect(results).toEqual(items.slice().reverse());
});

test('should allow for limit to be applied', async () => {
  const { indexName, dao } = context;

  const { items: results } = await dao.query({
    index: indexName,
    keyConditionExpression: 'test = :test',
    attributeValues: {
      ':test': testHashKey,
    },
    limit: 1,
  });

  expect(results).toEqual([items[0]]);
});

test('should allow for paging via lastKey', async () => {
  const { indexName, dao } = context;

  const { items: results, lastKey } = await dao.query({
    index: indexName,
    keyConditionExpression: 'test = :test',
    attributeValues: {
      ':test': testHashKey,
    },
    limit: 1,
  });

  expect(results).toEqual([items[0]]);

  const { items: moreResults } = await dao.query({
    index: indexName,
    keyConditionExpression: 'test = :test',
    startAt: lastKey,
    attributeValues: {
      ':test': testHashKey,
    },
    limit: 1,
  });

  expect(results).toEqual([items[0]]);
  expect(moreResults).toEqual([items[1]]);
});

test('should allow for filterExpression to be provided', async () => {
  const { indexName, dao } = context;

  const { items: results } = await dao.query({
    index: indexName,
    keyConditionExpression: 'test = :test',
    filterExpression: '#i < :index',
    attributeNames: {
      '#i': 'index',
    },
    attributeValues: {
      ':test': testHashKey,
      ':index': 2,
    },
  });

  expect(results).toEqual([items[0], items[1]]);
});

test('#queryUntilLimitReached should automatically keep querying until user-provided limit is reached', async () => {
  const { indexName, dao } = context;

  const params: QueryInputWithLimit = {
    index: indexName,
    keyConditionExpression: 'test = :test',
    filterExpression: '#i > :index',
    attributeNames: {
      '#i': 'index',
    },
    attributeValues: {
      ':test': testHashKey,
      ':index': 4,
    },
    limit: 2,
  };

  const result1 = await dao.queryUntilLimitReached(params);

  expect(result1.items).toEqual([
    {
      id: '5',
      index: 5,
      test: testHashKey,
    },
    {
      id: '6',
      index: 6,
      test: testHashKey,
    },
  ]);

  expect(result1.lastKey).toBeDefined();

  params.startAt = result1.lastKey;

  const result2 = await dao.queryUntilLimitReached(params);

  expect(result2.items).toEqual([
    {
      id: '7',
      index: 7,
      test: testHashKey,
    },
    {
      id: '8',
      index: 8,
      test: testHashKey,
    },
  ]);

  params.startAt = result2.lastKey;

  // increase limit to 3 but there is only 1 item left to be read
  params.limit = 3;

  const result3 = await dao.queryUntilLimitReached(params);

  expect(result3.items).toEqual([
    {
      id: '9',
      index: 9,
      test: testHashKey,
    },
  ]);

  // Using `limit` of 1 to hit another branch in the code regarding
  // ending at a page boundary
  params.startAt = result2.lastKey;
  // `limit` here is exactly the number of items left across all of the pages
  params.limit = 1;

  const result4 = await dao.queryUntilLimitReached(params);

  expect(result4.items).toEqual([
    {
      id: '9',
      index: 9,
      test: testHashKey,
    },
  ]);
});

/**
 * There was a bizarre error that showed up in a couple different services, where a specific combination of
 * Dynamo filter expressions and item lengths would cause a cursor to be returned of "0|" when it should have
 * an undefined or null cursor.  This would cause an infinite loop if the caller was just checking for the
 * presence of the cursor, because there was no skip or lastKey as a part of that returned cursor.
 *
 * This showed up as infinite loops in the policy-service and task-service, and I'm suspect that it has showed
 * up elsewhere undetected as well - Henry
 */
test('#queryUntilLimitReached should return an undefined cursor when the filterExpression only filters out minimal items', async () => {
  /**
   * The error would show up in particular if the second page of a queryUntilLimitReached request only had one item.
   *
   * So here we can test this by adding 1 more item than the page size, and making one item in the first page hit the filter expression
   */
  const { indexName, dao } = context;

  const putRequests = [];
  const hashKey = uuid();

  for (let i = 0; i < 101; i++) {
    // put data into dynamodb
    const item = {
      id: '' + i,
      index: i,
      test: hashKey,
    };

    if (i == 90) {
      (item as any).deletedOn = Date.now();
    }

    items.push(item);
    putRequests.push({
      PutRequest: {
        Item: item,
      },
    });
  }

  await pMap(
    chunk(putRequests, 25),
    async (putRequestChunk) => {
      await documentClient
        .batchWrite({
          RequestItems: {
            [context.tableName]: putRequestChunk,
          },
        })
        .promise();
    },
    { concurrency: 1 }
  );

  const params: QueryInputWithLimit = {
    index: indexName,
    keyConditionExpression: '#test = :test',
    filterExpression: 'attribute_not_exists(#deletedOn)',
    attributeNames: {
      '#test': 'test',
      '#deletedOn': 'deletedOn',
    },
    attributeValues: {
      ':test': hashKey,
    },
    limit: 100,
    startAt: undefined,
  };

  const result1 = await dao.queryUntilLimitReached(params);

  expect(result1.lastKey).toBeUndefined();
  expect(result1.items).toHaveLength(100);
});
