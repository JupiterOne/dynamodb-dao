import { BatchPutOperation, BatchWriteOperation } from './types';

export function isBatchPutOperation<DataModel, KeySchema>(
  operation: BatchWriteOperation<DataModel, KeySchema>
): operation is BatchPutOperation<DataModel> {
  return (operation as any).put !== undefined;
}
