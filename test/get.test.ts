import { v4 as uuid } from 'uuid';
import TestContext, { documentClient } from './helpers/TestContext';
import { PutCommand } from '@aws-sdk/lib-dynamodb';

let context: TestContext;

beforeAll(async () => {
  context = await TestContext.setup();
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test('should be able to get an item from the table', async () => {
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

  // put data into dynamodb
  const item = await dao.get(key);

  expect(item).toEqual(input);
});

test('should be able to do a consistent read on a get', async () => {
  const { tableName, dao } = context;
  const key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  await documentClient.send(
    new PutCommand({ TableName: tableName, Item: input })
  );

  const item = await dao.get(key, { consistentRead: true });
  expect(item).toEqual(input);
});
