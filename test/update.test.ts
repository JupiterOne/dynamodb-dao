import TestContext, { documentClient } from './helpers/TestContext';
import { v4 as uuid } from 'uuid';

import reservedWords from './fixtures/reservedWords';

let context: TestContext;

beforeAll(async () => {
  context = await TestContext.setup();
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

let key: any;
let item: any;

beforeEach(async () => {
  const { tableName } = context;

  key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  // put data into dynamodb
  await documentClient
    .put({
      TableName: tableName,
      Item: input,
    })
    .promise();

  // ensure it exists
  const { Item: storedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  // eslint-disable-next-line jest/no-standalone-expect
  expect(storedItem).toEqual(input);
  item = storedItem;
});

test('should be able to update an item in the table', async () => {
  const { tableName, dao } = context;
  const updateData = { test: uuid(), newField: uuid() };
  await dao.update(key, updateData);

  // ensure it exists
  const { Item: updatedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(updatedItem).toEqual({
    ...item,
    ...updateData,
  });
});

test('should explicitly remove a key if a field is explicitly set to undefined', async () => {
  const { tableName, dao } = context;
  const updateData = { test: undefined };
  await dao.update(key, updateData);

  // ensure it exists
  const { Item: updatedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(updatedItem).toEqual({
    ...item,
    ...updateData,
  });
});

test('should be able to mix updates and removals', async () => {
  const { tableName, dao } = context;
  const updateData = { test: undefined, apple: true };
  await dao.update(key, updateData);

  // ensure it exists
  const { Item: updatedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(updatedItem).toEqual({
    ...item,
    ...updateData,
  });
});

describe('reserved words', () => {
  for (const word of reservedWords) {
    test(`should allow for reserved word "${word}" to be used for updates`, async () => {
      const { tableName, dao } = context;
      const updateData = { [word]: uuid() };
      await dao.update(key, updateData);

      // ensure it exists
      const { Item: updatedItem } = await documentClient
        .get({
          TableName: tableName,
          Key: key,
        })
        .promise();

      expect(updatedItem).toEqual({
        ...item,
        ...updateData,
      });
    });
  }
});

test('should allow for a condition expression to be provided', async () => {
  const { dao } = context;
  const updateData = { test: uuid(), newField: uuid() };
  const promise = dao.update(key, updateData, {
    // this will cause a failure, because the condition won't match
    conditionExpression: 'NOT test = :testValue',
    attributeValues: {
      ':testValue': item.test,
    },
  });

  return expect(promise).rejects.toThrow('The conditional request failed');
});

test('should allow for attribute names to be provided for condition expressions', async () => {
  const { tableName, dao } = context;

  const testItem = {
    id: uuid(),
    status: 'ACTIVE', // add field ths is reserved word in ddb
  };

  await documentClient
    .put({
      TableName: tableName,
      Item: testItem,
    })
    .promise();

  const updateData = { status: uuid() };

  await dao.update(key, updateData, {
    conditionExpression: 'NOT #status = :newStatus',
    attributeNames: {
      '#status': 'status',
    },
    attributeValues: {
      ':newStatus': updateData.status,
    },
  });

  const { Item: updatedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(updatedItem).toEqual({
    ...item,
    ...updateData,
  });
});
