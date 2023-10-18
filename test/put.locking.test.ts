import { GetCommand } from '@aws-sdk/lib-dynamodb';
import { randomUUID as uuid } from 'crypto';
import TestContext, { documentClient } from './helpers/TestContext';

let context: TestContext;

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

describe.each([true, false])(
  'put with locking and auto-initiate %s',
  (autoInitiateLockingAttribute) => {
    beforeAll(async () => {
      context = await TestContext.setup(true, autoInitiateLockingAttribute);
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
      const { Item: item } = await documentClient.send(
        new GetCommand({
          TableName: tableName,
          Key: key,
        })
      );

      expect(item).toEqual({
        ...input,
        version: autoInitiateLockingAttribute ? 1 : undefined,
      });
    });

    test('should / should not add version number on first put when version undefined', async () => {
      const { tableName, dao } = context;

      const key = { id: uuid() };

      const input = {
        ...key,
        test: uuid(),
        version: undefined,
      };

      // put data into dynamodb
      await dao.put(input);

      // ensure it exists
      const { Item: item } = await documentClient.send(
        new GetCommand({
          TableName: tableName,
          Key: key,
        })
      );

      expect(item).toEqual({
        ...input,
        version: autoInitiateLockingAttribute ? 1 : undefined,
      });
    });

    test('should throw error if version number is not supplied on second update', async () => {
      const { dao } = context;

      const key = { id: uuid() };

      const input = {
        ...key,
        test: uuid(),
        version: 0,
      };

      // put data into dynamodb
      await dao.put(input);

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
        version: 0,
      };

      // put data into dynamodb
      await dao.put(input);
      await dao.put({
        ...input,
        version: 1,
      });

      const { Item: item } = await documentClient.send(
        new GetCommand({
          TableName: tableName,
          Key: key,
        })
      );

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
        version: 0,
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
      const { Item: item } = await documentClient.send(
        new GetCommand({
          TableName: tableName,
          Key: key,
        })
      );

      expect(item).toEqual({
        ...input,
        version: 2,
      });
    });
  }
);
