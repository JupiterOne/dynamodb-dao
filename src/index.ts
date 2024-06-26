import { sleep } from '@lifeomic/attempt';
import chunk from 'lodash.chunk';
import pMap from 'p-map';
import { DEFAULT_LOCK_INCREMENT, MAX_BATCH_OPERATIONS } from './constants';
import { buildOptimisticLockOptions } from './locking/buildOptimisticLockOptions';
import {
  decodeQueryUntilLimitCursor,
  encodeQueryUntilLimitCursor,
} from './query/cursor';
import {
  decodeExclusiveStartKey,
  encodeExclusiveStartKey,
} from './scan/startKey';
import { isBatchPutOperation } from './typeGuards';
import {
  AttributeNames,
  AttributeValues,
  BatchDeleteOperation,
  BatchGetResult,
  BatchPutOperation,
  BatchWriteOperation,
  BatchWriteResult,
  BatchWriteWithExponentialBackoffParams,
  CountOutput,
  GetItemOptions,
  QueryInput,
  QueryInputWithLimit,
  QueryResult,
  ScanInput,
} from './types';
import {
  DataModelAsMap,
  generateUpdateParams,
} from './update/generateUpdateParams';
import {
  BatchGetCommand,
  BatchWriteCommand,
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  UpdateCommand,
  UpdateCommandInput,
} from '@aws-sdk/lib-dynamodb';

export * from './constants';
export * from './types';

export interface ConditionalOptions {
  conditionExpression?: string;
  attributeNames?: AttributeNames;
  attributeValues?: AttributeValues;
}

export interface MutateBehavior {
  ignoreOptimisticLocking?: boolean;
}

export type PutOptions = ConditionalOptions & MutateBehavior;
export type UpdateOptions = ConditionalOptions & MutateBehavior;
export type DeleteOptions = ConditionalOptions & MutateBehavior;

export interface DynamoDbDaoInput<T> {
  tableName: string;
  documentClient: DynamoDBDocumentClient;
  optimisticLockingAttribute?: keyof NumberPropertiesInType<T>;
  autoInitiateLockingAttribute?: boolean;
}

/**
 * This type is used to force functions like `incr` and `decr` to only take
 * properties from the `DataModel` that are type "number".
 *
 * See: https://stackoverflow.com/a/49797062
 */
export type NumberPropertiesInType<T> = Pick<
  T,
  {
    [K in keyof T]: T[K] extends number ? K : never;
  }[keyof T]
>;

type IncrMap<DataModel> = Partial<{
  [key in keyof NumberPropertiesInType<DataModel>]: number;
}>;

/**
 * A base dynamodb dao class that enforces types
 */
export default class DynamoDbDao<
  DataModel extends Record<string, unknown>,
  KeySchema extends Record<string, unknown>,
> {
  public readonly tableName: string;
  public readonly documentClient: DynamoDBDocumentClient;
  public readonly optimisticLockingAttribute?: keyof NumberPropertiesInType<DataModel>;
  public readonly autoInitiateLockingAttribute?: boolean;

  constructor(options: DynamoDbDaoInput<DataModel>) {
    this.tableName = options.tableName;
    this.documentClient = options.documentClient;
    // The prior version implemented auto-initiate, so
    // we'll default to true to retain backward compatibility
    this.autoInitiateLockingAttribute =
      options.autoInitiateLockingAttribute === undefined
        ? true
        : options.autoInitiateLockingAttribute;
    this.optimisticLockingAttribute = options.optimisticLockingAttribute;
  }

  /**
   * Fetches an item by it's key schema
   */
  async get(
    key: KeySchema,
    options: GetItemOptions = { consistentRead: false }
  ): Promise<DataModel | undefined> {
    const { consistentRead } = options;
    const { Item: item } = await this.documentClient.send(
      new GetCommand({
        TableName: this.tableName,
        Key: key,
        ConsistentRead: consistentRead,
      })
    );

    return item as DataModel;
  }

  /**
   * Deletes the item. Returns the deleted item
   * if it was deleted
   */
  async delete(
    key: KeySchema,
    options: DeleteOptions = {},
    data: Partial<DataModel> = {}
  ): Promise<DataModel | undefined> {
    let { attributeNames, attributeValues, conditionExpression } = options;

    if (this.optimisticLockingAttribute && !options.ignoreOptimisticLocking) {
      const versionAttribute = this.optimisticLockingAttribute.toString();
      ({ attributeNames, attributeValues, conditionExpression } =
        buildOptimisticLockOptions({
          versionAttribute,
          versionAttributeValue: (data as DataModelAsMap)[versionAttribute],
          conditionExpression: conditionExpression,
          attributeNames,
          attributeValues,
        }));
    }
    const { Attributes: attributes } =
      await // The `.promise()` call might be on an JS SDK v2 client API.
      // If yes, please remove .promise(). If not, remove this comment.
      this.documentClient.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: key,
          ReturnValues: 'ALL_OLD',
          ConditionExpression: conditionExpression,
          ExpressionAttributeNames: attributeNames,
          ExpressionAttributeValues: attributeValues,
        })
      );

    return attributes as DataModel;
  }

  /**
   * Creates/Updates an item in the table
   */
  async put(data: DataModel, options: PutOptions = {}): Promise<DataModel> {
    let { conditionExpression, attributeNames, attributeValues } = options;
    if (this.optimisticLockingAttribute) {
      // Must cast data to avoid tripping the linter, otherwise, it'll complain
      // about expression of type 'string' can't be used to index type 'unknown'
      const dataAsMap = data as DataModelAsMap;
      const versionAttribute = this.optimisticLockingAttribute.toString();

      if (!options.ignoreOptimisticLocking) {
        ({ conditionExpression, attributeNames, attributeValues } =
          buildOptimisticLockOptions({
            versionAttribute,
            versionAttributeValue: dataAsMap[versionAttribute],
            conditionExpression,
            attributeNames,
            attributeValues,
          }));
      }

      // If the version attribute is supplied, increment it, otherwise only
      // set the default if directed to do so
      if (
        versionAttribute in dataAsMap &&
        !isNaN(dataAsMap[versionAttribute] as number)
      ) {
        dataAsMap[versionAttribute] += DEFAULT_LOCK_INCREMENT;
      } else if (this.autoInitiateLockingAttribute) {
        dataAsMap[versionAttribute] = DEFAULT_LOCK_INCREMENT;
      }
    }

    await this.documentClient.send(
      new PutCommand({
        TableName: this.tableName,
        Item: data,
        ConditionExpression: conditionExpression,
        ExpressionAttributeNames: attributeNames,
        ExpressionAttributeValues: attributeValues,
      })
    );
    return data;
  }

  /**
   * Creates/Updates an item in the table
   */
  async update(
    key: KeySchema,
    data: Partial<DataModel>,
    updateOptions?: UpdateOptions
  ): Promise<DataModel> {
    const optimisticLockVersionAttribute =
      this.optimisticLockingAttribute?.toString();
    const params = generateUpdateParams({
      tableName: this.tableName,
      key,
      data,
      ...updateOptions,
      optimisticLockVersionAttribute,
      autoInitiateLockingAttribute: this.autoInitiateLockingAttribute,
    });

    const { Attributes: attributes } = await this.documentClient.send(
      new UpdateCommand(params)
    );

    return attributes as DataModel;
  }

  async incr(
    key: KeySchema,
    attr: keyof NumberPropertiesInType<DataModel>,
    incrBy = 1,
    conditionExpression?: string
  ): Promise<DataModel> {
    return this.multiIncr(
      key,
      { [attr]: incrBy } as IncrMap<DataModel>,
      conditionExpression
    );
  }

  async decr(
    key: KeySchema,
    attr: keyof NumberPropertiesInType<DataModel>,
    decrBy = 1,
    conditionExpression?: string
  ): Promise<DataModel> {
    return this.multiIncr(
      key,
      { [attr]: decrBy * -1 } as IncrMap<DataModel>,
      conditionExpression
    );
  }

  async multiIncr(
    key: KeySchema,
    incrMap: IncrMap<DataModel>,
    conditionExpression?: string
  ): Promise<DataModel> {
    const incrEntries = Object.entries(incrMap);
    const errorEntries = incrEntries.filter(
      ([_key, value]) => !Number.isInteger(value)
    );
    if (errorEntries.length) {
      throw new Error(
        `Increments must be integers: ${JSON.stringify(errorEntries)}`
      );
    }

    const updateParams: UpdateCommandInput = {
      TableName: this.tableName,
      Key: key,
      UpdateExpression: 'SET',
      ConditionExpression: conditionExpression,
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {
        ':start': 0,
      },
      ReturnValues: 'ALL_NEW',
    };

    incrEntries.forEach(([key, value], i) => {
      const includeComma = i !== incrEntries.length - 1;
      const attrName = `#incrAttr${i}`;
      const valueName = `:inc${i}`;
      updateParams.UpdateExpression += ` ${attrName} = if_not_exists(${attrName}, :start) + ${valueName}${
        includeComma ? ',' : ''
      }`;
      updateParams.ExpressionAttributeNames![attrName] = key;
      updateParams.ExpressionAttributeValues![valueName] = value;
    });

    const { Attributes: attributes } = await this.documentClient.send(
      new UpdateCommand(updateParams)
    );

    return attributes as DataModel;
  }

  /**
   * Executes a query to fetch a count
   */
  async count(input: QueryInput): Promise<CountOutput> {
    const {
      index,
      attributeValues,
      attributeNames,
      keyConditionExpression,
      filterExpression,
      startAt,
      limit,
    } = input;

    let startKey: KeySchema | undefined;

    if (startAt) {
      startKey = decodeExclusiveStartKey<KeySchema>(startAt);
    }

    const result = await this.documentClient.send(
      new QueryCommand({
        TableName: this.tableName,
        IndexName: index,
        KeyConditionExpression: keyConditionExpression,
        FilterExpression: filterExpression,
        ExpressionAttributeValues: attributeValues,
        ExpressionAttributeNames: attributeNames,
        ExclusiveStartKey: startKey,
        Limit: limit,
        Select: 'COUNT',
      })
    );

    return {
      count: result.Count,
      scannedCount: result.ScannedCount,
      lastKey: result.LastEvaluatedKey
        ? encodeExclusiveStartKey<KeySchema>(
            result.LastEvaluatedKey as KeySchema
          )
        : undefined,
    };
  }

  /**
   * Executes a query on the table
   */
  async query(input: QueryInput): Promise<QueryResult<DataModel>> {
    const {
      index,
      startAt,
      attributeNames,
      attributeValues,
      scanIndexForward,
      keyConditionExpression,
      filterExpression,
      limit,
      consistentRead,
    } = input;

    let startKey: KeySchema | undefined;

    if (startAt) {
      startKey = decodeExclusiveStartKey<KeySchema>(startAt);
    }

    const result = await this.documentClient.send(
      new QueryCommand({
        TableName: this.tableName,
        IndexName: index,
        Limit: limit,
        ScanIndexForward: scanIndexForward,
        ExclusiveStartKey: startKey,
        KeyConditionExpression: keyConditionExpression,
        FilterExpression: filterExpression,
        ExpressionAttributeNames: attributeNames,
        ExpressionAttributeValues: attributeValues,
        ConsistentRead: consistentRead,
      })
    );

    return {
      items: result.Items as DataModel[],
      lastKey: result.LastEvaluatedKey
        ? encodeExclusiveStartKey<KeySchema>(
            result.LastEvaluatedKey as KeySchema
          )
        : undefined,
    };
  }

  async queryUntilLimitReached(
    params: QueryInputWithLimit
  ): Promise<QueryResult<DataModel>> {
    // create a shallow copy of params since we mutate the top level properties
    params = {
      ...params,
    };

    const cursor = decodeQueryUntilLimitCursor(params.startAt);

    // Use `cursor.lastKey` for the actual params that will
    // be sent to query(...).
    //
    // `cursor.skip` will be used to skip items on our function.
    params.startAt = cursor.lastKey;

    const items: DataModel[] = [];
    const limit = params.limit;
    let lastKey: string | undefined;

    do {
      const queryResult = await this.query(params);
      const curItems = queryResult.items;
      const curLen = curItems.length;

      for (let i = cursor.skip; i < curLen; i++) {
        const item = curItems[i];
        items.push(item);
        if (items.length >= limit) {
          // we reached our limit so we need to stop iterator

          let newLastKey: string | undefined = undefined;
          if (i === curLen - 1) {
            // If i == curLen - 1, that means that we have seen all of the
            // items on this page, so we only need to return a lastKey if it
            // is deifned
            newLastKey = queryResult.lastKey
              ? encodeQueryUntilLimitCursor(queryResult.lastKey, 0)
              : undefined;
          } else {
            // If `(i < curLen - 1)` then that means that we did not read
            // one or more records on the current page. That means that
            // we will need to read this page again but skip the records
            // that we have already read.
            newLastKey =
              i < curLen - 1
                ? encodeQueryUntilLimitCursor(params.startAt, i + 1)
                : encodeQueryUntilLimitCursor(queryResult.lastKey, 0);
          }

          return {
            items,
            lastKey: newLastKey,
          };
        }
      }

      // if on the second page,
      //    i >= curLen - 1
      //    queryResult.items = 1

      // only apply skip after the first query so we reset it here
      cursor.skip = 0;
      lastKey = queryResult.lastKey;
      params.startAt = lastKey;
    } while (lastKey);

    // if we got here then we exhausted all of the pages
    return {
      items,
      lastKey: undefined,
    };
  }

  /**
   * Scans the table
   */
  async scan(input: ScanInput = {}): Promise<QueryResult<DataModel>> {
    const {
      index,
      startAt,
      attributeNames,
      attributeValues,
      filterExpression,
      segment,
      totalSegments,
      limit,
      consistentRead,
    } = input;

    if (segment !== undefined && totalSegments === undefined) {
      throw new Error(
        'If segment is defined, totalSegments must also be defined.'
      );
    }

    if (segment === undefined && totalSegments !== undefined) {
      throw new Error(
        'If totalSegments is defined, segment must also be defined.'
      );
    }

    let startKey: KeySchema | undefined;

    if (startAt) {
      startKey = decodeExclusiveStartKey<KeySchema>(startAt);
    }

    const result = await this.documentClient.send(
      new ScanCommand({
        TableName: this.tableName,
        IndexName: index,
        Limit: limit,
        ExclusiveStartKey: startKey,
        FilterExpression: filterExpression,
        ExpressionAttributeNames: attributeNames,
        ExpressionAttributeValues: attributeValues,
        Segment: segment,
        TotalSegments: totalSegments,
        ConsistentRead: consistentRead,
      })
    );

    return {
      items: result.Items as DataModel[],
      lastKey: result.LastEvaluatedKey
        ? encodeExclusiveStartKey<KeySchema>(
            result.LastEvaluatedKey as KeySchema
          )
        : undefined,
    };
  }

  async batchWrite(
    operations: BatchWriteOperation<DataModel, KeySchema>[]
  ): Promise<BatchWriteResult<DataModel, KeySchema>> {
    if (operations.length > MAX_BATCH_OPERATIONS) {
      throw new Error(
        `Cannot send more than ${MAX_BATCH_OPERATIONS} operations in a single call.`
      );
    }

    const result = await this.documentClient.send(
      new BatchWriteCommand({
        RequestItems: {
          [this.tableName]: operations.map((operation) => {
            if (isBatchPutOperation(operation)) {
              return {
                PutRequest: {
                  Item: operation.put,
                },
              };
            } else {
              return {
                DeleteRequest: {
                  Key: operation.delete,
                },
              };
            }
          }),
        },
      })
    );

    const unprocessedItems =
      result.UnprocessedItems && result.UnprocessedItems[this.tableName];

    return {
      unprocessedItems: unprocessedItems
        ? unprocessedItems.map((item) => {
            if (item.PutRequest) {
              return {
                put: item.PutRequest.Item,
              } as BatchPutOperation<DataModel>;
            } else {
              return {
                delete: item.DeleteRequest?.Key,
              } as BatchDeleteOperation<KeySchema>;
            }
          })
        : undefined,
    };
  }

  async batchGet(
    keys: KeySchema[]
  ): Promise<BatchGetResult<DataModel, KeySchema>> {
    if (keys.length > MAX_BATCH_OPERATIONS) {
      throw new Error(
        `Cannot fetch more than ${MAX_BATCH_OPERATIONS} items in a single call.`
      );
    }

    const result = await this.documentClient.send(
      new BatchGetCommand({
        RequestItems: {
          [this.tableName]: {
            Keys: keys,
          },
        },
      })
    );

    const items = result.Responses && result.Responses[this.tableName];
    const unprocessedKeys =
      result.UnprocessedKeys && result.UnprocessedKeys[this.tableName];

    return {
      items: (items || []) as DataModel[],
      unprocessedKeys: unprocessedKeys
        ? (unprocessedKeys.Keys as KeySchema[])
        : undefined,
    };
  }

  async batchPutWithExponentialBackoff(
    params: BatchWriteWithExponentialBackoffParams<DataModel, KeySchema>
  ): Promise<void> {
    const {
      items,
      delay = 100,
      attempts = 0,
      maxRetries = 5,
      batchWriteLimit = MAX_BATCH_OPERATIONS,
      logger,
    } = params;

    logger.info(
      { attempts },
      'Attempting to batch put with exponential backoff'
    );

    if (items.length === 0) {
      logger.info({ items: items.length }, 'Nothing to batch put.');
      return;
    }

    logger.info(
      { items: items.length, delay, attempts, maxRetries },
      'Attempting to batch put...'
    );

    const batches: DataModel[][] = chunk(items, batchWriteLimit);

    logger.info({ batches: batches.length }, 'Number of total batches');
    const unprocessedItems: BatchPutOperation<DataModel>[] = [];

    await pMap(
      batches,
      async (batch) => {
        const result = await this.batchWrite(
          batch.map((batchItem) => ({
            put: batchItem,
          }))
        );

        if (result.unprocessedItems) {
          unprocessedItems.push(
            ...(result.unprocessedItems as BatchPutOperation<DataModel>[])
          );
        }
      },
      { concurrency: 2 }
    );

    if (unprocessedItems.length && attempts > maxRetries) {
      logger.error(
        {
          unprocessedItems: unprocessedItems.length,
          attempts,
          maxRetries,
        },
        'Found unprocessed items, but reached max attempts.'
      );

      throw new Error(
        `Failed to process items after attempts (attempts=${attempts})`
      );
    } else if (unprocessedItems.length) {
      logger.warn(
        {
          unprocessedItems: unprocessedItems.length,
          attempts,
          maxRetries,
          delay,
        },
        'Found unprocessed items. Retrying after dely...'
      );

      await sleep(delay);

      await this.batchPutWithExponentialBackoff({
        logger,
        items: unprocessedItems.map((item) => item.put),
        delay: Math.round(Math.pow(delay, 1.2)),
        attempts: attempts + 1,
        maxRetries,
      });
    }

    logger.info('Successfully wrote all batches!');
  }
}
