import { sleep } from '@lifeomic/attempt';
import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import chunk from 'lodash.chunk';
import pMap from 'p-map';

type AttributeNames = Record<string, string>;
type AttributeValues = Record<string, any>;

interface BaseScanInput {
  index?: string;
  limit?: number;
  startAt?: string;
  filterExpression?: string;
  attributeNames?: AttributeNames;
  attributeValues?: AttributeValues;
  consistentRead?: boolean;
}

export interface ScanInput extends BaseScanInput {
  segment?: number;
  totalSegments?: number;
}

export interface CountOutput {
  count?: number;
  scannedCount?: number;
  lastKey?: string;
}

export interface QueryInput extends BaseScanInput {
  scanIndexForward?: boolean;
  keyConditionExpression: string;
  attributeValues: AttributeValues;
  consistentRead?: boolean;
}

export interface QueryInputWithLimit extends QueryInput {
  limit: number;
}

export interface QueryResult<T> {
  items: T[];
  lastKey?: string;
}

export interface BatchPutOperation<DataModel> {
  put: DataModel;
}

export interface BatchDeleteOperation<KeySchema> {
  delete: KeySchema;
}

export type BatchWriteOperation<DataModel, KeySchema> =
  | BatchPutOperation<DataModel>
  | BatchDeleteOperation<KeySchema>;

export interface BatchWriteResult<DataModel, KeySchema> {
  unprocessedItems?: BatchWriteOperation<DataModel, KeySchema>[];
}

export interface BatchGetResult<DataModel, KeySchema> {
  items: DataModel[];
  unprocessedKeys?: KeySchema[];
}

export interface BatchPutParams<T, U> {
  batch: T[];
  logger: any;
}

interface GetItemOptions {
  consistentRead?: boolean;
}

interface BaseBatchWriteWithExponentialBackoffParams<T, U> {
  logger: any;
  delay?: number;
  attempts?: number;
  maxRetries?: number;
  batchWriteLimit?: number;
}

interface BatchWriteWithExponentialBackoffParams<T, U>
  extends BaseBatchWriteWithExponentialBackoffParams<T, U> {
  items: T[];
}

export const DEFAULT_QUERY_LIMIT = 50;
export const MAX_BATCH_OPERATIONS = 25;

/**
 * encode start key into a base64 encoded string
 * that can be used for pagination
 */
export function encodeExclusiveStartKey<KeySchema>(obj: KeySchema): string {
  return Buffer.from(JSON.stringify(obj)).toString('base64');
}

/**
 * Decode the key the start key
 */
export function decodeExclusiveStartKey<KeySchema>(token: string): KeySchema {
  try {
    return JSON.parse(Buffer.from(token, 'base64').toString('utf8'));
  } catch (err) {
    throw new Error('Invalid pagination token provided');
  }
}

export function isBatchPutOperation<DataModel, KeySchema>(
  operation: BatchWriteOperation<DataModel, KeySchema>
): operation is BatchPutOperation<DataModel> {
  return (operation as any).put !== undefined;
}

export interface ConditionalOptions {
  conditionExpression?: string;
  attributeNames?: AttributeNames;
  attributeValues?: AttributeValues;
}

export interface SaveBehavior {
  optimisticLockVersionAttribute?: string;
  optimisticLockVersionIncrement?: number;
}

export interface MutateBehavior {
  ignoreOptimisticLocking?: boolean;
}

export type PutOptions = ConditionalOptions & MutateBehavior;
export type UpdateOptions = ConditionalOptions & MutateBehavior;
export type DeleteOptions = ConditionalOptions & MutateBehavior;

export interface BuildOptimisticLockOptionsInput extends ConditionalOptions {
  versionAttribute: string;
  versionAttributeValue: any;
}

export function buildOptimisticLockOptions(
  options: BuildOptimisticLockOptionsInput
): ConditionalOptions {
  const { versionAttribute, versionAttributeValue } = options;
  let { conditionExpression, attributeNames, attributeValues } = options;

  const lockExpression = versionAttributeValue
    ? `#${versionAttribute} = :${versionAttribute}`
    : `attribute_not_exists(${versionAttribute})`;

  conditionExpression = conditionExpression
    ? `(${conditionExpression}) AND ${lockExpression}`
    : lockExpression;

  if (versionAttributeValue) {
    attributeNames = {
      ...attributeNames,
      [`#${versionAttribute}`]: versionAttribute,
    };
    attributeValues = {
      ...attributeValues,
      [`:${versionAttribute}`]: versionAttributeValue,
    };
  }

  return {
    conditionExpression,
    attributeNames,
    attributeValues,
  };
}

type DataModelAsMap = { [key: string]: any };

export interface GenerateUpdateParamsInput extends UpdateOptions {
  tableName: string;
  key: any;
  data: object;
}

export function generateUpdateParams(
  options: GenerateUpdateParamsInput & SaveBehavior
): DocumentClient.UpdateItemInput {
  const setExpressions: string[] = [];
  const addExpressions: string[] = [];
  const removeExpressions: string[] = [];
  const expressionAttributeNameMap: AttributeNames = {};
  const expressionAttributeValueMap: AttributeValues = {};

  const {
    tableName,
    key,
    data,
    attributeNames,
    attributeValues,
    optimisticLockVersionAttribute: versionAttribute,
    optimisticLockVersionIncrement: versionInc,
    ignoreOptimisticLocking: ignoreLocking = false,
  } = options;

  let conditionExpression = options.conditionExpression;

  if (versionAttribute) {
    addExpressions.push(`#${versionAttribute} :${versionAttribute}Inc`);
    expressionAttributeNameMap[`#${versionAttribute}`] = versionAttribute;
    expressionAttributeValueMap[`:${versionAttribute}Inc`] = versionInc ?? 1;

    if (!ignoreLocking) {
      ({ conditionExpression } = buildOptimisticLockOptions({
        versionAttribute,
        versionAttributeValue: (data as DataModelAsMap)[versionAttribute],
        conditionExpression,
      }));
      expressionAttributeValueMap[`:${versionAttribute}`] = (
        data as DataModelAsMap
      )[versionAttribute];
    }
  }

  const keys = Object.keys(options.data).sort();

  for (let i = 0; i < keys.length; i++) {
    const name = keys[i];
    if (name === versionAttribute) {
      // versionAttribute is a special case and should always be handled
      // explicitly as above with the supplied value ignored
      continue;
    }

    const valueName = `:a${i}`;
    const attributeName = `#a${i}`;

    const value = (data as any)[name];
    expressionAttributeNameMap[attributeName] = name;

    if (value === undefined) {
      removeExpressions.push(attributeName);
    } else {
      expressionAttributeValueMap[valueName] = value;
      setExpressions.push(`${attributeName} = ${valueName}`);
    }
  }
  const expressionAttributeValues = {
    ...expressionAttributeValueMap,
    ...attributeValues,
  };

  const setString =
    setExpressions.length > 0 ? 'set ' + setExpressions.join(', ') : undefined;

  const removeString =
    removeExpressions.length > 0
      ? 'remove ' + removeExpressions.join(', ')
      : undefined;

  const addString =
    addExpressions.length > 0 ? 'add ' + addExpressions.join(', ') : undefined;
  return {
    TableName: tableName,
    Key: key,
    ConditionExpression: conditionExpression,
    UpdateExpression: [addString, setString, removeString]
      .filter((val) => val !== undefined)
      .join(' '),
    ExpressionAttributeNames: {
      ...expressionAttributeNameMap,
      ...attributeNames,
    },
    ExpressionAttributeValues:
      Object.keys(expressionAttributeValues).length > 0
        ? expressionAttributeValues
        : undefined,
    ReturnValues: 'ALL_NEW',
  };
}

interface DynamoDbDaoInput<T> {
  tableName: string;
  documentClient: DocumentClient;
  optimisticLockingAttribute?: keyof NumberPropertiesInType<T>;
}

function invalidCursorError(cursor: string): Error {
  const err = new Error(
    `Invalid cursor for queryUntilLimitReached(...) function (cursor=${cursor})`
  );
  (err as any).retryable = false;
  return err;
}

export function encodeQueryUntilLimitCursor(
  lastKey: string | undefined,
  skip: number | undefined
): string {
  return `${skip || 0}|${lastKey || ''}`;
}

export function decodeQueryUntilLimitCursor(cursor: string | undefined): {
  skip: number;
  lastKey: string | undefined;
} {
  if (!cursor) {
    return {
      skip: 0,
      lastKey: undefined,
    };
  }

  const pos = cursor.indexOf('|');
  if (pos === -1) {
    throw invalidCursorError(cursor);
  }

  const skip = parseInt(cursor.substring(0, pos), 10);
  if (Number.isNaN(skip)) {
    throw invalidCursorError(cursor);
  }

  const lastKey = cursor.substring(pos + 1);
  return { skip, lastKey };
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

/**
 * A base dynamodb dao class that enforces types
 */
export default class DynamoDbDao<DataModel, KeySchema> {
  public readonly tableName: string;
  public readonly documentClient: DocumentClient;
  public readonly optimisticLockingAttribute?: keyof NumberPropertiesInType<DataModel>;

  constructor(options: DynamoDbDaoInput<DataModel>) {
    this.tableName = options.tableName;
    this.documentClient = options.documentClient;
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
    const { Item: item } = await this.documentClient
      .get({
        TableName: this.tableName,
        Key: key,
        ConsistentRead: consistentRead,
      })
      .promise();

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
    const { Attributes: attributes } = await this.documentClient
      .delete({
        TableName: this.tableName,
        Key: key,
        ReturnValues: 'ALL_OLD',
        ConditionExpression: conditionExpression,
        ExpressionAttributeNames: attributeNames,
        ExpressionAttributeValues: attributeValues,
      })
      .promise();

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

      dataAsMap[versionAttribute] = dataAsMap[versionAttribute]
        ? dataAsMap[versionAttribute] + 1
        : 1;
    }

    await this.documentClient
      .put({
        TableName: this.tableName,
        Item: data,
        ConditionExpression: conditionExpression,
        ExpressionAttributeNames: attributeNames,
        ExpressionAttributeValues: attributeValues,
      })
      .promise();
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
    const params = generateUpdateParams({
      tableName: this.tableName,
      key,
      data,
      ...updateOptions,
      optimisticLockVersionAttribute: this.optimisticLockingAttribute
        ? this.optimisticLockingAttribute.toString()
        : undefined,
    });
    const { Attributes: attributes } = await this.documentClient
      .update(params)
      .promise();

    return attributes as DataModel;
  }

  async incr(
    key: KeySchema,
    attr: keyof NumberPropertiesInType<DataModel>,
    incrBy = 1
  ): Promise<DataModel> {
    const { Attributes: attributes } = await this.documentClient
      .update({
        TableName: this.tableName,
        Key: key,
        UpdateExpression:
          'SET #incrAttr = if_not_exists(#incrAttr, :start) + :inc',
        ExpressionAttributeNames: {
          '#incrAttr': attr as string,
        },
        ExpressionAttributeValues: {
          ':inc': incrBy,
          ':start': 0,
        },
        ReturnValues: 'ALL_NEW',
      })
      .promise();

    return attributes as DataModel;
  }

  async decr(
    key: KeySchema,
    attr: keyof NumberPropertiesInType<DataModel>,
    decrBy = 1
  ): Promise<DataModel> {
    const { Attributes: attributes } = await this.documentClient
      .update({
        TableName: this.tableName,
        Key: key,
        UpdateExpression:
          'SET #decrAttr = if_not_exists(#decrAttr, :start) - :dec',
        ExpressionAttributeNames: {
          '#decrAttr': attr as string,
        },
        ExpressionAttributeValues: {
          ':dec': decrBy,
          ':start': 0,
        },
        ReturnValues: 'ALL_NEW',
      })
      .promise();

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

    const result = await this.documentClient
      .query({
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
      .promise();

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
      limit = DEFAULT_QUERY_LIMIT,
      consistentRead,
    } = input;

    let startKey: KeySchema | undefined;

    if (startAt) {
      startKey = decodeExclusiveStartKey<KeySchema>(startAt);
    }

    const result = await this.documentClient
      .query({
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
      .promise();

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
    if (!params.filterExpression) {
      // Since there are no filter expressions, DynamoDB will automatically
      // fulfill the `limit` property.
      return this.query(params);
    }

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
          return {
            items,

            // If `(i < curLen - 1)` then that means that we did not read
            // one or more records on the current page. That means that
            // we will need to read this page again but skip the records
            // that we have already read.
            lastKey:
              i < curLen - 1
                ? encodeQueryUntilLimitCursor(params.startAt, i + 1)
                : encodeQueryUntilLimitCursor(queryResult.lastKey, 0),
          };
        }
      }

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
      limit = DEFAULT_QUERY_LIMIT,
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

    const result = await this.documentClient
      .scan({
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
      .promise();

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

    const result = await this.documentClient
      .batchWrite({
        RequestItems: {
          [this.tableName]: operations.map((operation) => {
            // TODO: optionally add the opt lock here
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
      .promise();

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

    const result = await this.documentClient
      .batchGet({
        RequestItems: {
          [this.tableName]: {
            Keys: keys,
          },
        },
      })
      .promise();

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
