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

  // 0 can be passed and is valid, so we need to be specific
  const versionIsSupplied = versionAttributeValue !== undefined;

  const lockExpression = versionIsSupplied
    ? `(#${versionAttribute} = :${versionAttribute} OR attribute_not_exists(${versionAttribute}))`
    : `attribute_not_exists(${versionAttribute})`;

  conditionExpression = conditionExpression
    ? `(${conditionExpression}) AND ${lockExpression}`
    : lockExpression;

  if (versionIsSupplied) {
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
