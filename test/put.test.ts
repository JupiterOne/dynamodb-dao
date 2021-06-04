import TestContext, { documentClient } from './helpers/TestContext';
import { v4 as uuid } from 'uuid';

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

test('returns empty object with returnOldValues upon CREATE', async () => {
  const { dao } = context;
  const createOrUpdate = { id: uuid(), test: uuid() };
  const result = await dao.put(createOrUpdate, {
    returnOldValues: true,
  });

  // assuming a new entry, we should see {}:
  expect(result).toMatchObject({});
});

test('returns old values with returnOldValues upon UPDATE', async () => {
  const { tableName, dao } = context;
  const id = uuid();
  const getItem = () => ({
    id: id,
    test: uuid(),
  });
  const old_write = getItem(),
    createOrUpdate = getItem();

  // put data into dynamodb: after update, we should see this returned:
  await documentClient
    .put({
      TableName: tableName,
      Item: old_write,
    })
    .promise();

  const oldValues = await dao.put(createOrUpdate, {
    // pass a boolean to get back pre-UPDATE data:
    returnOldValues: true,
  });

  // the data has been updated from `old_write` to `update`
  // the function should return old_write's properties:
  expect(oldValues).toMatchObject(old_write);
});
