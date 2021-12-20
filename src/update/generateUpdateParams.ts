import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { DEFAULT_LOCK_INCREMENT, UpdateOptions } from '../index';
import { buildOptimisticLockOptions } from '../locking/buildOptimisticLockOptions';
import { AttributeNames, AttributeValues } from '../types';

export interface SaveBehavior {
  optimisticLockVersionAttribute?: string;
  optimisticLockVersionIncrement?: number;
  autoInitiateLockingAttribute?: boolean;
}

export type DataModelAsMap = { [key: string]: any };

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
    autoInitiateLockingAttribute,
  } = options;

  let conditionExpression = options.conditionExpression;
  const dataAsMap = data as DataModelAsMap;

  if (versionAttribute) {
    const providesVersion =
      versionAttribute in data && !isNaN(dataAsMap[versionAttribute]);

    if (providesVersion || autoInitiateLockingAttribute) {
      addExpressions.push(`#${versionAttribute} :${versionAttribute}Inc`);
      expressionAttributeNameMap[`#${versionAttribute}`] = versionAttribute;
      expressionAttributeValueMap[`:${versionAttribute}Inc`] =
        versionInc ?? DEFAULT_LOCK_INCREMENT;

      if (!ignoreLocking) {
        expressionAttributeValueMap[`:${versionAttribute}`] =
          dataAsMap[versionAttribute];
      }
    }

    if (!ignoreLocking) {
      ({ conditionExpression } = buildOptimisticLockOptions({
        versionAttribute,
        versionAttributeValue: dataAsMap[versionAttribute],
        conditionExpression,
      }));
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
