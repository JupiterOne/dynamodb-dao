import { v4 as uuid } from 'uuid';
import TestContext, { documentClient } from './helpers/TestContext';
import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

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

  await documentClient.send(
    new BatchWriteCommand({
      RequestItems: {
        [context.tableName]: putRequests,
      },
    })
  );
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test('should allow for data to be scanned', async () => {
  const { dao } = context;

  const { items: results } = await dao.scan();

  expect(results).toEqual(expect.arrayContaining(items));
});

test('should allow for data to be scanned by index', async () => {
  const { indexName, dao } = context;

  const { items: results } = await dao.scan({ index: indexName });

  expect(results).toEqual(items);
});

test('should allow for limit to be applied', async () => {
  const { dao } = context;

  const { items: results } = await dao.scan({
    limit: 1,
  });

  expect(results.length).toEqual(1);
});

test('should allow for paging via lastKey', async () => {
  const { dao } = context;

  const { items: results, lastKey } = await dao.scan({
    limit: 1,
  });

  const firstItem = results[0];

  const { items: moreResults } = await dao.scan({
    startAt: lastKey,
    limit: 1,
  });

  const secondItem = moreResults[0];

  expect(firstItem).not.toEqual(secondItem);
});

test('should allow for filterExpression to be provided', async () => {
  const { indexName, dao } = context;

  const { items: results } = await dao.scan({
    index: indexName,
    filterExpression: '#i < :index',
    attributeNames: {
      '#i': 'index',
    },
    attributeValues: {
      ':index': 2,
    },
  });

  expect(results).toEqual(expect.arrayContaining([items[0], items[1]]));
});

async function runSegmentTillFinished(
  context: TestContext,
  segment: number,
  totalSegments: number
): Promise<any[]> {
  const allItems: any[] = [];
  const { dao } = context;
  let cursor: string | undefined;
  do {
    const { items, lastKey } = await dao.scan({
      segment,
      totalSegments,
    });
    allItems.push(...items);
    cursor = lastKey;
  } while (cursor);
  return allItems;
}

test('should allow for segment and totalSegments to be provided for parallel scans', async () => {
  const TOTAL_SEGMENTS = 2;

  const promises: Promise<any[]>[] = [];
  for (let segment = 0; segment < TOTAL_SEGMENTS; segment++) {
    promises.push(runSegmentTillFinished(context, segment, TOTAL_SEGMENTS));
  }

  const results = await Promise.all(promises);

  const allItems: any[] = [];

  for (const result of results) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    allItems.push(...result);
  }

  expect(allItems).toEqual(expect.arrayContaining(items));
});
