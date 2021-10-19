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

test('should set the version number to 1 on first update', async () => {
  const { tableName, dao } = context;
  const updateData = { test: uuid(), newField: uuid() };
  await dao.update(key, updateData);

  const { Item: updatedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(updatedItem).toEqual({
    ...item,
    ...updateData,
    version: 1,
  });
});

test('should increment the version number by 1 on subsequent updates', async () => {
  const { tableName, dao } = context;
  const updateData = { test: uuid(), newField: uuid() };
  await dao.update(key, updateData);
  await dao.update(key, updateData);
  await dao.update(key, updateData);

  const { Item: updatedItem } = await documentClient
    .get({
      TableName: tableName,
      Key: key,
    })
    .promise();

  expect(updatedItem).toEqual({
    ...item,
    ...updateData,
    version: 3,
  });
});
