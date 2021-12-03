import { generateUpdateParams } from './generateUpdateParams';

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
      UpdateExpression: 'set #a0 = :a0, #a1 = :a1, #a2 = :a2',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
      },
      ExpressionAttributeValues: {
        ':a0': options.data.a,
        ':a1': options.data.b,
        ':a2': options.data.c,
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
      UpdateExpression: 'set #a0 = :a0',
      ExpressionAttributeNames: {
        '#a0': 'a',
      },
      ExpressionAttributeValues: {
        ':a0': options.data.a,
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
      UpdateExpression: 'set #a0 = :a0, #a1 = :a1 remove #a2',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
      },
      ExpressionAttributeValues: {
        ':a0': options.data.a,
        ':a1': options.data.b,
      },
    });
  }
});
test('#generateUpdateParams should increment the version number', () => {
  {
    const options = {
      tableName: 'blah2',
      key: {
        HashKey: 'abc',
      },
      data: {
        a: 123,
        b: 'abc',
        c: undefined,
        lockVersion: 1,
      },
      optimisticLockVersionAttribute: 'lockVersion',
    };

    expect(generateUpdateParams(options)).toEqual({
      TableName: options.tableName,
      Key: options.key,
      ReturnValues: 'ALL_NEW',
      ConditionExpression:
        '(#lockVersion = :lockVersion OR attribute_not_exists(lockVersion))',
      UpdateExpression:
        'add #lockVersion :lockVersionInc set #a0 = :a0, #a1 = :a1 remove #a2',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
        '#lockVersion': 'lockVersion',
      },
      ExpressionAttributeValues: {
        ':a0': options.data.a,
        ':a1': options.data.b,
        ':lockVersionInc': 1,
        ':lockVersion': 1,
      },
    });
  }
});
test('#generateUpdateParams should not increment the version number when not supplied', () => {
  {
    const options = {
      tableName: 'blah3',
      key: {
        HashKey: 'abc',
      },
      data: {
        a: 123,
        b: 'abc',
        c: undefined,
      },
      optimisticLockVersionAttribute: 'lockVersion',
    };

    expect(generateUpdateParams(options)).toEqual({
      TableName: options.tableName,
      Key: options.key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: 'set #a0 = :a0, #a1 = :a1 remove #a2',
      ConditionExpression: 'attribute_not_exists(lockVersion)',
      ExpressionAttributeNames: {
        '#a0': 'a',
        '#a1': 'b',
        '#a2': 'c',
      },
      ExpressionAttributeValues: {
        ':a0': options.data.a,
        ':a1': options.data.b,
      },
    });
  }
});
