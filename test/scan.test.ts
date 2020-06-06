import TestContext, { documentClient } from './helpers/TestContext';
import { v4 as uuid } from 'uuid';

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
