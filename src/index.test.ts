import AWS from 'aws-sdk';

import DynamoDbDao, {
  generateUpdateParams,
  DEFAULT_QUERY_LIMIT,
  encodeQueryUntilLimitCursor,
  decodeQueryUntilLimitCursor,
  CountOutput,
} from '.';

import { v4 as uuid } from 'uuid';

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
    'base64',
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
    'base64',
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
    }),
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
    data,
  );

  expect(documentClient.update).toHaveBeenCalledWith(updateParams);
  expect(result).toEqual(data);
});

test('#generateUpdateParams should generate set params for documentClient.update(...)', () => {
  {
    const options = {
      tableName: 'blah',
      key: {
        HashKey: 'abc',
      },
      data: {
        a: 123,
        b: 'abc',
        c: true,
      },
    };

    expect(generateUpdateParams(options)).toEqual({
      TableName: options.tableName,
      Key: options.key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: 'set #a0 = :a, #a1 = :b, #a2 = :c',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
      },
      ExpressionAttributeValues: {
        ':a': options.data.a,
        ':b': options.data.b,
        ':c': options.data.c,
      },
    });
  }

  {
    const options = {
      tableName: 'blah',
      key: {
        HashKey: 'abc',
      },
      data: {
        a: {
          something: 'else',
        },
      },
    };

    expect(generateUpdateParams(options)).toEqual({
      TableName: options.tableName,
      Key: options.key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: 'set #a0 = :a',
      ExpressionAttributeNames: {
        '#a0': 'a',
      },
      ExpressionAttributeValues: {
        ':a': options.data.a,
      },
    });
  }
});

test('#generateUpdateParams should generate remove params for documentClient.update(...)', () => {
  {
    const options = {
      tableName: 'blah',
      key: {
        HashKey: 'abc',
      },
      data: {
        a: undefined,
        b: undefined,
        c: undefined,
      },
    };

    expect(generateUpdateParams(options)).toEqual({
      TableName: options.tableName,
      Key: options.key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: 'remove #a0, #a1, #a2',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
      },
      ExpressionAttributeValues: undefined,
    });
  }
});

test('#generateUpdateParams should generate both update and remove params for documentClient.update(...)', () => {
  {
    const options = {
      tableName: 'blah',
      key: {
        HashKey: 'abc',
      },
      data: {
        a: 123,
        b: 'abc',
        c: undefined,
      },
    };

    expect(generateUpdateParams(options)).toEqual({
      TableName: options.tableName,
      Key: options.key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: 'set #a0 = :a, #a1 = :b remove #a2',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
      },
      ExpressionAttributeValues: {
        ':a': options.data.a,
        ':b': options.data.b,
      },
    });
  }
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

test('#encodeQueryUntilLimitCursor should handle falsy "skip" and falsy "lastKey"', () => {
  expect(encodeQueryUntilLimitCursor('', 0)).toBe('0|');
});

test('#decodeQueryUntilLimitCursor should handle empty cursor', () => {
  expect(decodeQueryUntilLimitCursor(undefined)).toEqual({
    lastKey: undefined,
    skip: 0,
  });
});

test('#decodeQueryUntilLimitCursor should throw error for invalid skip in cursor', () => {
  expect(() => {
    decodeQueryUntilLimitCursor('blah');
  }).toThrowError(/Invalid cursor/);
});

test('#decodeQueryUntilLimitCursor should throw error for invalid skip in cursor with pipe', () => {
  expect(() => {
    decodeQueryUntilLimitCursor('blah|blah');
  }).toThrowError(/Invalid cursor/);
});
