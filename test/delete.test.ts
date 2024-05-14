import { v4 as uuid } from 'uuid';
import TestContext, { documentClient } from './helpers/TestContext';
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';

let context: TestContext;

beforeAll(async () => {
  context = await TestContext.setup();
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test('should be able to delete an item from the table', async () => {
  const { tableName, dao } = context;

  const key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  // put data into dynamodb
  await documentClient.send(
    new PutCommand({
      TableName: tableName,
      Item: input,
    })
  );

  // ensure it exists
  const { Item: item } = await documentClient.send(
    new GetCommand({
      TableName: tableName,
      Key: key,
    })
  );

  expect(item).toEqual(input);

  await dao.delete(key);

  // ensure it deleted
  const { Item: deletedItem } = await documentClient.send(
    new GetCommand({
      TableName: tableName,
      Key: key,
    })
  );

  expect(deletedItem).toEqual(undefined);
});

test('should allow for a condition expression to be provided', async () => {
  const { tableName, dao } = context;

  const item = {
    id: uuid(),
    test: uuid(),
  };

  // put data into dynamodb
  await documentClient.send(
    new PutCommand({
      TableName: tableName,
      Item: item,
    })
  );

  const promise = dao.delete(
    { id: item.id },
    {
      // this will cause a failure, because the condition won't match
      conditionExpression: 'NOT test = :testValue',
      attributeValues: {
        ':testValue': item.test,
      },
    }
  );

  return expect(promise).rejects.toThrow('The conditional request failed');
});

test('should allow for a expression attribute names to be provided', async () => {
  const { tableName, dao } = context;

  const item = {
    id: uuid(),
    test: uuid(),
  };

  // put data into dynamodb
  await documentClient.send(
    new PutCommand({
      TableName: tableName,
      Item: item,
    })
  );

  const promise = dao.delete(
    { id: item.id },
    {
      // this will cause a failure, because the condition won't match
      conditionExpression: 'NOT #t = :testValue',
      attributeNames: {
        '#t': 'test',
      },
      attributeValues: {
        ':testValue': item.test,
      },
    }
  );

  return expect(promise).rejects.toThrow('The conditional request failed');
});
