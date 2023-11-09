import { v4 as uuid } from 'uuid';
import TestContext, { documentClient } from './helpers/TestContext';

let context: TestContext;

beforeAll(async () => {
  context = await TestContext.setup();
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test('should be able to put an item into dynamodb', async () => {
  const { tableName, dao } = context;

  const key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  // put data into dynamodb
  await dao.put(input);

  // ensure it exists
  const { Item: item } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(item).toEqual(input);
});

test('should allow for a condition expression to be provided', async () => {
  const { tableName, dao } = context;

  const item = {
    id: uuid(),
    test: uuid(),
  };

  // put data into dynamodb
  await documentClient
    .put({
      TableName: tableName,
      Item: item,
    })
    .promise();

  const promise = dao.put(item, {
    // this will cause a failure, because the condition won't match
    conditionExpression: 'attribute_not_exists(id)',
  });

  return expect(promise).rejects.toThrow('The conditional request failed');
});

test('should allow for a expression attribute names to be provided', async () => {
  const { tableName, dao } = context;

  const item = {
    id: uuid(),
    test: uuid(),
  };

  // put data into dynamodb
  await documentClient
    .put({
      TableName: tableName,
      Item: item,
    })
    .promise();

  const promise = dao.put(item, {
    // this will cause a failure, because the condition won't match
    conditionExpression: 'NOT #id = :id',
    attributeNames: {
      '#id': 'id',
    },
    attributeValues: {
      ':id': item.id,
    },
  });

  return expect(promise).rejects.toThrow('The conditional request failed');
});
