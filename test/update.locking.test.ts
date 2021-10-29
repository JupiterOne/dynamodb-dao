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
  await dao.update(key, { ...updateData, version: 1 });
  await dao.update(key, { ...updateData, version: 2 });

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

test('should error if update does not supply the correct version number', async () => {
  const { dao } = context;
  const updateData = { test: uuid(), newField: uuid() };
  // sets the initial version to 1
  await dao.update(key, updateData);

  await expect(async () => {
    // doesn't supply a version, throws error
    await dao.update(key, updateData);
  }).rejects.toThrow('The conditional request failed');
});

test('should allow update without correct version number if ignore flag is set', async () => {
  const { tableName, dao } = context;
  const updateData = { test: uuid(), newField: uuid() };
  // sets the initial version to 1
  await dao.update(key, updateData);
  // Still increments the version
  await dao.update(key, updateData, {
    ignoreOptimisticLocking: true,
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
    version: 2,
  });
});
