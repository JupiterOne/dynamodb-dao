export type AttributeNames = Record<string, string>;
export type AttributeValues = Record<string, any>;

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

export interface Types extends BaseScanInput {
  scanIndexForward?: boolean;
  keyConditionExpression: string;
  attributeValues: AttributeValues;
  consistentRead?: boolean;
}

export interface QueryInputWithLimit extends Types {
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

export interface GetItemOptions {
  consistentRead?: boolean;
}

interface BaseBatchWriteWithExponentialBackoffParams<T, U> {
  logger: any;
  delay?: number;
  attempts?: number;
  maxRetries?: number;
  batchWriteLimit?: number;
}

export interface BatchWriteWithExponentialBackoffParams<T, U>
  extends BaseBatchWriteWithExponentialBackoffParams<T, U> {
  items: T[];
}
