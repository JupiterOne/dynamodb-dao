import { v4 as uuid } from 'uuid';
import { CountOutput } from '../src/types';
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

test('should return count of data in an index', async () => {
  const { indexName, dao } = context;

  const count = await dao.count({
    index: indexName,
    keyConditionExpression: 'test = :test',
    attributeValues: {
      ':test': testHashKey,
    },
  });

  const countOutput: CountOutput = {
    count: items.length,
    scannedCount: items.length,
    lastKey: undefined,
  };

  expect(count).toEqual(countOutput);
});

test('should return count of data in an index when attributeNames provided', async () => {
  const { indexName, dao } = context;

  const count = await dao.count({
    index: indexName,
    keyConditionExpression: '#test = :test',
    attributeValues: {
      ':test': testHashKey,
    },
    attributeNames: {
      '#test': 'test',
    },
  });

  const countOutput: CountOutput = {
    count: items.length,
    scannedCount: items.length,
    lastKey: undefined,
  };

  expect(count).toEqual(countOutput);
});
