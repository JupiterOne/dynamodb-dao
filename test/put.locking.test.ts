import { v4 as uuid } from 'uuid';
import TestContext, { documentClient } from './helpers/TestContext';

let context: TestContext;

beforeAll(async () => {
  context = await TestContext.setup(true);
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test('should add version number on first put', async () => {
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

  expect(item).toEqual({
    ...input,
    version: 1,
  });
});

test('should throw error if version number is not supplied on second update', async () => {
  const { tableName, dao } = context;

  const key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  // put data into dynamodb
  await dao.put(input);

  expect(1).toEqual(1);

  // ensure it exists
  const { Item: item } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(item).toEqual({
    ...input,
    version: 1,
  });

  await expect(async () => {
    await dao.put({
      ...key,
      test: uuid(),
    });
  }).rejects.toThrow('The conditional request failed');
});

test('should allow multiple puts if version number is incremented', async () => {
  const { tableName, dao } = context;

  const key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  // put data into dynamodb
  await dao.put(input);
  await dao.put({
    ...input,
    version: 1,
  });

  // ensure it exists
  const { Item: item } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(item).toEqual({
    ...input,
    version: 2,
  });
});

test('should allow multiple puts if version number is incremented when multiple conditions exist', async () => {
  const { tableName, dao } = context;

  const key = { id: uuid() };

  const input = {
    ...key,
    test: uuid(),
  };

  // put data into dynamodb
  await dao.put(input);
  await dao.put(
    {
      ...input,
      version: 1,
    },
    {
      attributeNames: { '#test': 'test' },
      attributeValues: { ':test': input.test, ':test2': '2' },
      conditionExpression: '#test = :test or #test = :test2',
    }
  );

  // ensure it exists
  const { Item: item } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(item).toEqual({
    ...input,
    version: 2,
  });
});
