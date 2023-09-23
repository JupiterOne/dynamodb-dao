/* istanbul ignore file */

import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { randomUUID as uuid } from 'crypto';
import DynamoDbDao, { DynamoDbDaoInput } from '../../src';

const { DYNAMODB_ENDPOINT = 'http://localhost:8000' } = process.env;

export interface KeySchema {
  id: string;
}

export interface DataModel extends KeySchema {
  test: string;
  status?: string;
  version?: number;
  extra?: number;
}

/*
const dynamodb = new DynamoDB({
  apiVersion: '2012-08-10',
  region: 'us-east-1',
  endpoint: DYNAMODB_ENDPOINT,
  accessKeyId: 'blah',
  secretAccessKey: 'blah',
});
*/

const client = new DynamoDBClient({
  apiVersion: '2012-08-10',
  region: 'us-east-1',
  endpoint: DYNAMODB_ENDPOINT,
  credentials: {
    accessKeyId: 'blah',
    secretAccessKey: 'blah',
  },
});

export const documentClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: {
    removeUndefinedValues: true,
  },
});

//export const documentClient = new DynamoDB.DocumentClient({
//  service: dynamodb,
//});

export default class TestContext {
  tableName: string;
  indexName: string;
  dao: DynamoDbDao<DataModel, KeySchema>;

  constructor(
    tableName: string,
    indexName: string,
    dao: DynamoDbDao<DataModel, KeySchema>
  ) {
    this.tableName = tableName;
    this.indexName = indexName;
    this.dao = dao;
  }

  static async setup(
    useOptimisticLocking: boolean = false,
    autoInitiateLockingAttribute: boolean = true
  ): Promise<TestContext> {
    const tableName = uuid();
    const indexName = uuid();

    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [
          {
            AttributeName: 'id',
            AttributeType: 'S',
          },
          {
            AttributeName: 'test',
            AttributeType: 'S',
          },
        ],
        KeySchema: [
          {
            AttributeName: 'id',
            KeyType: 'HASH',
          },
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: indexName,
            KeySchema: [
              {
                AttributeName: 'test',
                KeyType: 'HASH',
              },
              {
                AttributeName: 'id',
                KeyType: 'RANGE',
              },
            ],
            Projection: {
              ProjectionType: 'ALL',
            },
            ProvisionedThroughput: {
              ReadCapacityUnits: 1,
              WriteCapacityUnits: 1,
            },
          },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1,
        },
      })
    );

    const dao = new DynamoDbDao<DataModel, KeySchema>({
      tableName,
      documentClient,
      optimisticLockingAttribute: useOptimisticLocking ? 'version' : undefined,
      autoInitiateLockingAttribute,
    } as DynamoDbDaoInput<DataModel>);

    return new TestContext(tableName, indexName, dao);
  }

  teardown() {
    return client.send(new DeleteTableCommand({ TableName: this.tableName }));
  }
}
