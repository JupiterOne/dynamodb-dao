import * as AWS from 'aws-sdk';
import { v4 as uuid } from 'uuid';
import DynamoDbDao from '.';
import mockLogger from '../test/helpers/mockLogger';
import { DEFAULT_QUERY_LIMIT } from './constants';
import { CountOutput } from './types';
import { generateUpdateParams } from './update/generateUpdateParams';

const dynamodb = new AWS.DynamoDB({
  apiVersion: '2012-08-10',
  endpoint: process.env.DYNAMODB_ENDPOINT,
});

const documentClient = new AWS.DynamoDB.DocumentClient({
  service: dynamodb,
});

interface TestModel {
  id: string;
  description: string;
}

interface KeySchema {
  id: string;
}

const testModelInstance: TestModel = {
  id: uuid(),
  description: uuid(),
};

const tableName = 'test-table';

const testDao = new DynamoDbDao<TestModel, KeySchema>({
  tableName,
  documentClient,
});

afterEach(() => {
  jest.resetAllMocks();
});

test(`Dao constructor should generate a prefixed tableName`, async () => {
  expect(testDao.tableName).toBe(tableName);
});

test(`#get should pass in the table name and key as input \
and return the result item`, async () => {
  const key = { id: uuid() };

  jest.spyOn(documentClient, 'get').mockReturnValue({
    promise: () => Promise.resolve({ Item: testModelInstance }),
  } as any);

  const result = await testDao.get(key);

  expect(documentClient.get).toHaveBeenCalledWith({
    TableName: tableName,
    Key: key,
    ConsistentRead: false,
  });

  expect(result).toEqual(testModelInstance);
});

test(`#get should pass the consistentRead option if supplied \
and return the result item`, async () => {
  const key = { id: uuid() };

  jest.spyOn(documentClient, 'get').mockReturnValue({
    promise: () => Promise.resolve({ Item: testModelInstance }),
  } as any);

  const result = await testDao.get(key, { consistentRead: true });

  expect(documentClient.get).toHaveBeenCalledWith({
    TableName: tableName,
    Key: key,
    ConsistentRead: true,
  });

  expect(result).toEqual(testModelInstance);
});

test(`#put should pass in the table name and data as input \
and return the result item`, async () => {
  jest.spyOn(documentClient, 'put').mockReturnValue({
    promise: () => Promise.resolve({}),
  } as any);

  const result = await testDao.put(testModelInstance);

  expect(documentClient.put).toHaveBeenCalledWith({
    TableName: tableName,
    Item: testModelInstance,
  });

  expect(result).toEqual(testModelInstance);
});

test(`#delete should return pass in the table name, key, \
and return the old attributes`, async () => {
  jest.spyOn(documentClient, 'delete').mockReturnValue({
    promise: () => Promise.resolve({ Attributes: testModelInstance }),
  } as any);

  const key = { id: testModelInstance.id };
  const result = await testDao.delete(key);

  expect(documentClient.delete).toHaveBeenCalledWith({
    TableName: tableName,
    Key: key,
    ReturnValues: 'ALL_OLD',
  });

  expect(result).toEqual(testModelInstance);
});

test(`#query should return pass in the table name, index, \
keyConditionExpression, and attributeValues`, async () => {
  const lastEvaluatedKey = { id: uuid() };
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [testModelInstance],
        LastEvaluatedKey: lastEvaluatedKey,
      }),
  } as any);

  const result = await testDao.query({
    index,
    limit,
    attributeValues,
    keyConditionExpression,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    Limit: limit,
    ExclusiveStartKey: undefined,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
  });

  expect(result).toEqual({
    items: [testModelInstance],
    lastKey: Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64'),
  });
});

test('#query should allow consistent reads', async () => {
  const lastEvaluatedKey = { id: uuid() };
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [testModelInstance],
        LastEvaluatedKey: lastEvaluatedKey,
      }),
  } as any);

  await testDao.query({
    index,
    limit,
    attributeValues,
    keyConditionExpression,
    consistentRead: true,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    Limit: limit,
    ExclusiveStartKey: undefined,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
    ConsistentRead: true,
  });
});

test(`#query should have default query limit`, async () => {
  const lastEvaluatedKey = { id: uuid() };
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [testModelInstance],
        LastEvaluatedKey: lastEvaluatedKey,
      }),
  } as any);

  await testDao.query({
    index,
    attributeValues,
    keyConditionExpression,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    Limit: DEFAULT_QUERY_LIMIT,
    ExclusiveStartKey: undefined,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
  });
});

test(`#count should be supported`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const mockCount = 5;

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Count: mockCount,
        ScannedCount: mockCount,
      }),
  } as any);

  const result = await testDao.count({
    index,
    attributeValues,
    keyConditionExpression,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
    Select: 'COUNT',
  });

  const countOutput: CountOutput = {
    count: mockCount,
    scannedCount: mockCount,
    lastKey: undefined,
  };

  expect(result).toEqual(countOutput);
});

test(`#count should properly decode the input start key`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const mockCount = 5;
  const index = uuid();

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Count: mockCount,
        ScannedCount: mockCount,
      }),
  } as any);

  const exclusiveStartKey = { id: uuid() };
  const startAt = Buffer.from(JSON.stringify(exclusiveStartKey)).toString(
    'base64'
  );

  const result = await testDao.count({
    index,
    startAt,
    attributeValues,
    keyConditionExpression,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
    ExclusiveStartKey: exclusiveStartKey,
    Select: 'COUNT',
  });

  const countOutput: CountOutput = {
    count: mockCount,
    scannedCount: mockCount,
    lastKey: undefined,
  };

  expect(result).toEqual(countOutput);
});

test(`#count should allow returning encoded exclusive start key`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const lastEvaluatedKey = uuid();
  const mockCount = 5;

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Count: mockCount,
        ScannedCount: mockCount,
        LastEvaluatedKey: lastEvaluatedKey,
      }),
  } as any);

  const result = await testDao.count({
    index,
    attributeValues,
    keyConditionExpression,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
    Select: 'COUNT',
  });

  const countOutput: CountOutput = {
    count: mockCount,
    scannedCount: mockCount,
    lastKey: Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64'),
  };

  expect(result).toEqual(countOutput);
});

test(`#query should properly decode the input start key`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [],
        LastEvaluatedKey: undefined,
      }),
  } as any);

  const exclusiveStartKey = { id: uuid() };
  const startAt = Buffer.from(JSON.stringify(exclusiveStartKey)).toString(
    'base64'
  );

  await testDao.query({
    index,
    limit,
    startAt,
    attributeValues,
    keyConditionExpression,
  });

  expect(documentClient.query).toHaveBeenCalledWith({
    TableName: tableName,
    IndexName: index,
    Limit: limit,
    ExclusiveStartKey: exclusiveStartKey,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeValues: attributeValues,
  });
});

test(`#query should throw error for invalid exclusiveStartKey`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  jest.spyOn(documentClient, 'query').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [],
        LastEvaluatedKey: undefined,
      }),
  } as any);

  const startAt = 'blah blah blah';

  await expect(
    testDao.query({
      index,
      limit,
      startAt,
      attributeValues,
      keyConditionExpression,
    })
  ).rejects.toThrow('Invalid pagination token provided');
});

test(`#update should be supported`, async () => {
  const data = {
    description: uuid(),
  };

  const key: KeySchema = {
    id: uuid(),
  };

  const updateParams = generateUpdateParams({
    tableName,
    key,
    data,
  });

  jest.spyOn(documentClient, 'update').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Attributes: data,
      }),
  } as any);

  const result = await testDao.update(
    {
      id: key.id,
    },
    data
  );

  expect(documentClient.update).toHaveBeenCalledWith(updateParams);
  expect(result).toEqual(data);
});

test(`#queryUntilLimitReached should call #query once if "filterExpression" not provided and the limit is reached`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 5;

  jest.spyOn(testDao, 'query').mockResolvedValue({
    lastKey: uuid(),
    items: Array.from({ length: 10 }, (_, i) => ({
      id: '' + i,
      index: i,
      test: uuid(),
      description: uuid(),
    })),
  });

  const params = {
    // `filterExpression` is intentionally not provided
    index,
    limit,
    attributeValues,
    keyConditionExpression,
  };

  await testDao.queryUntilLimitReached(params);

  expect(testDao.query).toHaveBeenCalledTimes(1);
  expect(testDao.query).toHaveBeenCalledWith(params);
});

test(`#queryUntilLimitReached should call #query the number of times needed until the limit is reached if "filterExpression" not provided`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 5;

  jest.spyOn(testDao, 'query').mockResolvedValue({
    lastKey: uuid(),
    items: Array.from({ length: 2 }, (_, i) => ({
      id: '' + i,
      index: i,
      test: uuid(),
      description: uuid(),
    })),
  });

  const params = {
    // `filterExpression` is intentionally not provided
    index,
    limit,
    attributeValues,
    keyConditionExpression,
  };

  await testDao.queryUntilLimitReached(params);

  expect(testDao.query).toHaveBeenCalledTimes(3);
});

test('#scan should allow consistent reads', async () => {
  jest.spyOn(documentClient, 'scan').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [],
        LastEvaluatedKey: undefined,
      }),
  } as any);

  await testDao.scan({
    consistentRead: true,
  });

  expect(documentClient.scan).toHaveBeenCalledWith({
    ConsistentRead: true,
    Limit: 50,
    TableName: tableName,
  });
});

test('#scan should error if segment is provided but totalSegments is not', async () => {
  jest.spyOn(documentClient, 'scan').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [],
        LastEvaluatedKey: undefined,
      }),
  } as any);

  await expect(
    testDao.scan({
      segment: 1,
    })
  ).rejects.toThrow(
    'If segment is defined, totalSegments must also be defined.'
  );
});

test('#scan should error if totalSegments is provided but segment is not', async () => {
  jest.spyOn(documentClient, 'scan').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [],
        LastEvaluatedKey: undefined,
      }),
  } as any);

  await expect(
    testDao.scan({
      totalSegments: 10039912993994,
    })
  ).rejects.toThrow(
    'If totalSegments is defined, segment must also be defined.'
  );
});

test('#batchWriteWithExponentialBackoff should error when a batchWrite fails', async () => {
  jest.spyOn(documentClient, 'batchWrite').mockReturnValue({
    promise: () => {
      return Promise.reject(new Error('you failed!'));
    },
  } as any);

  await expect(
    testDao.batchPutWithExponentialBackoff({
      logger: mockLogger,
      items: [testModelInstance, testModelInstance, testModelInstance],
    })
  ).rejects.toThrowError();
});

test('#batchWriteWithExponentialBackoff should retry unprocessed items', async () => {
  const batchWriteSpy = jest
    .spyOn(documentClient, 'batchWrite')
    .mockReturnValueOnce({
      promise: () =>
        Promise.resolve({
          UnprocessedItems: {
            [tableName]: [testModelInstance, testModelInstance],
          },
        }),
    } as any)
    .mockReturnValue({
      promise: () =>
        Promise.resolve({
          UnprocessedItems: {
            [tableName]: [],
          },
        }),
    } as any);

  const result = await testDao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items: [testModelInstance, testModelInstance, testModelInstance],
  });

  expect(result).toBeUndefined();
  expect(batchWriteSpy).toHaveBeenCalledTimes(2);
});

test('#batchWriteWithExponentialBackoff should stop retrying after hitting max attempts', async () => {
  jest.setTimeout(15000);
  const batchWriteSpy = jest
    .spyOn(documentClient, 'batchWrite')
    .mockReturnValue({
      promise: () =>
        Promise.resolve({
          UnprocessedItems: {
            [tableName]: [testModelInstance, testModelInstance],
          },
        }),
    } as any);

  await expect(
    testDao.batchPutWithExponentialBackoff({
      logger: mockLogger,
      items: [testModelInstance, testModelInstance, testModelInstance],
      maxRetries: 2,
    })
  ).rejects.toThrowError();

  // original request, plus two retries 3 retries (it's 0 based)
  expect(batchWriteSpy).toHaveBeenCalledTimes(4);
});

test('#batchWriteWithExponentialBackoff should respect the batchWriteLimit', async () => {
  jest.setTimeout(15000);
  const batchWriteSpy = jest
    .spyOn(documentClient, 'batchWrite')
    .mockReturnValue({
      promise: () =>
        Promise.resolve({
          UnprocessedItems: {
            [tableName]: [],
          },
        }),
    } as any);

  const result = await testDao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items: [testModelInstance, testModelInstance, testModelInstance],
    batchWriteLimit: 1,
  });

  expect(result).toBeUndefined();
  // should result in 3 batches of 1
  expect(batchWriteSpy).toHaveBeenCalledTimes(3);
});

test('#batchWriteWithExponentialBackoff should return if no items are supplied', async () => {
  jest.setTimeout(15000);
  const batchWriteSpy = jest
    .spyOn(documentClient, 'batchWrite')
    .mockReturnValue({
      promise: () =>
        Promise.resolve({
          UnprocessedItems: {
            [tableName]: [],
          },
        }),
    } as any);

  const result = await testDao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items: [],
    batchWriteLimit: 1,
  });

  expect(result).toBeUndefined();
  expect(batchWriteSpy).toHaveBeenCalledTimes(0);
  expect(mockLogger.info).toHaveBeenCalledWith(
    expect.anything(),
    expect.stringMatching(/Nothing to batch put/)
  );
});
