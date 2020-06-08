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

test('should be able to get an item from the table', async () => {
  const { tableName, dao } = context;

  const key = { id: uuid() };

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

  // put data into dynamodb
  const item = await dao.get(key);

  expect(item).toEqual(input);
});
