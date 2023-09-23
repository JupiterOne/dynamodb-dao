import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommand,
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { randomUUID as uuid } from 'crypto';
import DynamoDbDao, { CountOutput, DEFAULT_QUERY_LIMIT } from '.';
import mockLogger from '../test/helpers/mockLogger';
import { generateUpdateParams } from './update/generateUpdateParams';

const ddbClient = new DynamoDBClient({
  endpoint: process.env.DYNAMODB_ENDPOINT,
  apiVersion: '2012-08-10',
});

const documentClient = DynamoDBDocumentClient.from(ddbClient);

const mockedDocumentClient = mockClient(documentClient);

beforeEach(() => {
  mockedDocumentClient.reset();
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
  documentClient: mockedDocumentClient as unknown as DynamoDBClient,
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

  mockedDocumentClient.on(GetCommand).resolves({ Item: testModelInstance });

  const result = await testDao.get(key);

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new GetCommand({
        TableName: tableName,
        Key: key,
        ConsistentRead: false,
      })
    )
  );
  expect(result).toEqual(testModelInstance);
});

test(`#get should pass the consistentRead option if supplied \
and return the result item`, async () => {
  const key = { id: uuid() };

  mockedDocumentClient.on(GetCommand).resolves({ Item: testModelInstance });

  const result = await testDao.get(key, { consistentRead: true });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new GetCommand({
        TableName: tableName,
        Key: key,
        ConsistentRead: true,
      })
    )
  );

  expect(result).toEqual(testModelInstance);
});

test(`#put should pass in the table name and data as input \
and return the result item`, async () => {
  const result = await testDao.put(testModelInstance);

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new PutCommand({
        TableName: tableName,
        Item: testModelInstance,
      })
    )
  );
  expect(result).toEqual(testModelInstance);
});

test(`#delete should return pass in the table name, key, \
and return the old attributes`, async () => {
  mockedDocumentClient
    .on(DeleteCommand)
    .resolves({ Attributes: testModelInstance });

  const key = { id: testModelInstance.id };
  const result = await testDao.delete(key);

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new DeleteCommand({
        TableName: tableName,
        Key: key,
        ReturnValues: 'ALL_OLD',
      })
    )
  );

  expect(result).toEqual(testModelInstance);
});

test(`#query should return pass in the table name, index, \
keyConditionExpression, and attributeValues`, async () => {
  const lastEvaluatedKey = { id: uuid() };
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  mockedDocumentClient.on(QueryCommand).resolves({
    Items: [testModelInstance],
    LastEvaluatedKey: lastEvaluatedKey,
  });

  const result = await testDao.query({
    index,
    limit,
    attributeValues,
    keyConditionExpression,
  });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        Limit: limit,
        ExclusiveStartKey: undefined,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
      })
    )
  );

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

  mockedDocumentClient.on(QueryCommand).resolves({
    Items: [testModelInstance],
    LastEvaluatedKey: lastEvaluatedKey,
  });

  const result = await testDao.query({
    index,
    limit,
    attributeValues,
    keyConditionExpression,
    consistentRead: true,
  });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        Limit: limit,
        ExclusiveStartKey: undefined,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
        ConsistentRead: true,
      })
    )
  );

  expect(result).toEqual({
    items: [testModelInstance],
    lastKey: Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64'),
  });
});

test(`#query should have default query limit`, async () => {
  const lastEvaluatedKey = { id: uuid() };
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();

  mockedDocumentClient.on(QueryCommand).resolves({
    Items: [testModelInstance],
    LastEvaluatedKey: lastEvaluatedKey,
  });

  const result = await testDao.query({
    index,
    attributeValues,
    keyConditionExpression,
  });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        Limit: DEFAULT_QUERY_LIMIT,
        ExclusiveStartKey: undefined,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
      })
    )
  );

  expect(result).toEqual({
    items: [testModelInstance],
    lastKey: Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64'),
  });
});

test(`#count should be supported`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const mockCount = 5;

  mockedDocumentClient.on(QueryCommand).resolves({
    Count: mockCount,
    ScannedCount: mockCount,
  });

  const result = await testDao.count({
    index,
    attributeValues,
    keyConditionExpression,
  });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
        Select: 'COUNT',
      })
    )
  );

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

  mockedDocumentClient.on(QueryCommand).resolves({
    Count: mockCount,
    ScannedCount: mockCount,
  });

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

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
        ExclusiveStartKey: exclusiveStartKey,
        Select: 'COUNT',
      })
    )
  );

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
  const lastEvaluatedKey = { id: uuid() };
  const mockCount = 5;

  mockedDocumentClient.on(QueryCommand).resolves({
    Count: mockCount,
    ScannedCount: mockCount,
    LastEvaluatedKey: lastEvaluatedKey,
  });

  const result = await testDao.count({
    index,
    attributeValues,
    keyConditionExpression,
  });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
        Select: 'COUNT',
      })
    )
  );

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

  mockedDocumentClient.on(QueryCommand).resolves({
    Items: [],
    LastEvaluatedKey: undefined,
  });

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

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new QueryCommand({
        TableName: tableName,
        IndexName: index,
        Limit: limit,
        ExclusiveStartKey: exclusiveStartKey,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: attributeValues,
      })
    )
  );
});

test(`#query should throw error for invalid exclusiveStartKey`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  mockedDocumentClient.on(QueryCommand).resolves({
    Items: [],
    LastEvaluatedKey: undefined,
  });

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

  mockedDocumentClient.on(UpdateCommand).resolves({
    Attributes: data,
  });

  const result = await testDao.update(
    {
      id: key.id,
    },
    data
  );

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(new UpdateCommand(updateParams))
  );
  expect(result).toEqual(data);
});

test(`#queryUntilLimitReached should call #query if "filterExpression" not provided`, async () => {
  const keyConditionExpression = 'id = :id';
  const attributeValues = { id: uuid() };
  const index = uuid();
  const limit = 50;

  jest.spyOn(testDao, 'query').mockResolvedValue({
    lastKey: '',
    items: [],
  });

  const params = {
    // `filterExpression` is intentionally not provided
    index,
    limit,
    attributeValues,
    keyConditionExpression,
  };

  await testDao.queryUntilLimitReached(params);

  expect(testDao.query).toHaveBeenCalledWith(params);
});

test('#scan should allow consistent reads', async () => {
  mockedDocumentClient.on(ScanCommand).resolves({
    Items: [],
    LastEvaluatedKey: undefined,
  });

  await testDao.scan({
    consistentRead: true,
  });

  expect(mockedDocumentClient.calls()).toHaveLength(1);
  expect(mockedDocumentClient.call(0).args).toHaveLength(1);
  expect(JSON.stringify(mockedDocumentClient.call(0).firstArg)).toEqual(
    JSON.stringify(
      new ScanCommand({
        TableName: tableName,
        Limit: 50,
        ConsistentRead: true,
      })
    )
  );
});

test('#scan should error if segment is provided but totalSegments is not', async () => {
  mockedDocumentClient.on(ScanCommand).resolves({
    Items: [],
    LastEvaluatedKey: undefined,
  });

  await expect(
    testDao.scan({
      segment: 1,
    })
  ).rejects.toThrow(
    'If segment is defined, totalSegments must also be defined.'
  );
});

test('#scan should error if totalSegments is provided but segment is not', async () => {
  mockedDocumentClient.on(ScanCommand).resolves({
    Items: [],
    LastEvaluatedKey: undefined,
  });

  await expect(
    testDao.scan({
      totalSegments: 10039912993994,
    })
  ).rejects.toThrow(
    'If totalSegments is defined, segment must also be defined.'
  );
});

test('#batchWriteWithExponentialBackoff should error when a batchWrite fails', async () => {
  mockedDocumentClient.on(BatchWriteCommand).rejects(new Error('you failed!'));

  await expect(
    testDao.batchPutWithExponentialBackoff({
      logger: mockLogger,
      items: [testModelInstance, testModelInstance, testModelInstance],
    })
  ).rejects.toThrowError();
});

test('#batchWriteWithExponentialBackoff should retry unprocessed items', async () => {
  mockedDocumentClient
    .on(BatchWriteCommand)
    .resolvesOnce({
      UnprocessedItems: {
        [tableName]: [testModelInstance, testModelInstance],
      },
    })
    .resolvesOnce({
      UnprocessedItems: {
        [tableName]: [],
      },
    });

  const result = await testDao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items: [testModelInstance, testModelInstance, testModelInstance],
  });

  expect(result).toBeUndefined();
  expect(mockedDocumentClient.commandCalls(BatchWriteCommand)).toHaveLength(2);
});

test('#batchWriteWithExponentialBackoff should stop retrying after hitting max attempts', async () => {
  jest.setTimeout(15000);

  mockedDocumentClient.on(BatchWriteCommand).resolves({
    UnprocessedItems: {
      [tableName]: [testModelInstance, testModelInstance],
    },
  });

  await expect(
    testDao.batchPutWithExponentialBackoff({
      logger: mockLogger,
      items: [testModelInstance, testModelInstance, testModelInstance],
      maxRetries: 2,
    })
  ).rejects.toThrowError();

  // original request, plus two retries 3 retries (it's 0 based)
  expect(mockedDocumentClient.commandCalls(BatchWriteCommand)).toHaveLength(4);
});

test('#batchWriteWithExponentialBackoff should respect the batchWriteLimit', async () => {
  jest.setTimeout(15000);

  mockedDocumentClient.on(BatchWriteCommand).resolves({
    UnprocessedItems: {
      [tableName]: [],
    },
  });

  const result = await testDao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items: [testModelInstance, testModelInstance, testModelInstance],
    batchWriteLimit: 1,
  });

  expect(result).toBeUndefined();
  // should result in 3 batches of 1
  //
  expect(mockedDocumentClient.commandCalls(BatchWriteCommand)).toHaveLength(3);
});

test('#batchWriteWithExponentialBackoff should return if no items are supplied', async () => {
  jest.setTimeout(15000);

  mockedDocumentClient.on(BatchWriteCommand).resolves({
    UnprocessedItems: {
      [tableName]: [],
    },
  });

  const result = await testDao.batchPutWithExponentialBackoff({
    logger: mockLogger,
    items: [],
    batchWriteLimit: 1,
  });

  expect(result).toBeUndefined();
  expect(mockedDocumentClient.commandCalls(BatchWriteCommand)).toHaveLength(0);
  expect(mockLogger.info).toHaveBeenCalledWith(
    expect.anything(),
    expect.stringMatching(/Nothing to batch put/)
  );
});
