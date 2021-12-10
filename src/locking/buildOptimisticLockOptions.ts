import { ConditionalOptions } from '../index';

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
