# dynamodb-dao

This project contains code for a `DynamoDbDao` class that can be used for
creating, querying, updating, and deleting from DynamoDB table. Unlike tools
like `dynamoose`, `DynamoDbDao` is a lower level wrapper and aims not to
abstract away too many of the DynamoDB implementation details.

Also, this module leverages TypeScript type declarations so that, when possible,
methods arguments and return values are strictly typed.

## Examples

**Constructor:**

```javascript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

const ddb = new DynamoDBClient({
  apiVersion: '2012-08-10'
})

const documentClient = DynamoDBDocumentClient.from(ddb, {
  marshallOptions: {
    removeUndefinedValues: true
  }
});

// The type declaration of for the documents that we are storing
interface MyDocument {
  id: string;
  accountId: string;
  name: string;
  total?: number;
}

// Key schema should have one or two properties which correspond to
// hash key and range key.
//
// NOTE: a range key is optional and depends
// on how your DynamoDB table is configured.
interface MyDocumentKeySchema {
  // hash key
  accountId: string;

  // range key
  id: string;
}

const myDocumentDao = new DynamoDbDao<MyDocument, MyDocumentKeySchema>({
  tableName: 'my-documents',
  documentClient
});
```

**Get query:**

```javascript
const myDocument = await myDocumentDao.get({ id, accountId });
```

**Paginated query:**

```javascript
const { items, lastKey } = await myDocumentDao.query({
  index: 'NameIndex',
  keyConditionExpression: 'accountId = :accountId',
  startAt: cursor /* `cursor` is a previously returned `lastKey` */,
  scanIndexForward: true,
  attributeValues: {
    ':accountId': accountId,
  },
});
```

**Count query:**

```javascript
const count = await myDocumentDao.count({
  index: 'NameIndex',
  keyConditionExpression: 'accountId = :accountId',
  attributeValues: {
    ':accountId': input.accountId,
  },
});
```

**Put:**

```javascript
await myDocumentDao.put({
  id: 'something',
  accountId: 'abc'
  name: 'blah'
});
```

**Delete:**

```javascript
await myDocumentDao.delete({ id, accountId });
```

**Incrementing/Decrementing**

NOTE: This should only be used where overcounting and undercounting can be
tolerated. See
[the DynamoDB atomic counter documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.AtomicCounters)
for more information.

If a property does not already exist, the initial value is assigned `0` and
incremented/decremented from `0`.

```ts
// `total` will have the value `5`
const { total } = await myDocumentDao.incr(
  // The key
  {
    id: 'abc',
    accountId: 'def',
  },
  // The `number` property to increment
  'total',
  // The number to increment by. Defaults to 1.
  5
);

// `total` will have the value `-5`
const { total } = await myDocumentDao.decr(
  // The key
  {
    id: '123',
    accountId: 'def',
  },
  // The `number` property to increment
  'total',
  // The number to decrement by. Defaults to 1.
  5
);
```

When multiple values must be incremented and/or decremented in the same call:

```ts
// `total` will have the value `5` and `extra` will have the value -1.
const { extra, total } = await myDocumentDao.multiIncr(
  {
    id: 'abc',
    accountId: 'def',
  },
  {
    total: 5,
    extra: -1,
  }
);
```

**Optimistic Locking with Version Numbers**

For callers who wish to enable an optimistic locking strategy there are two
available toggles:

1. Provide the attribute you wish to be used to store the version number. This
   will enable optimistic locking on the following operations: `put`, `update`,
   and `delete`.

   Writes for documents that do not have a version number attribute will
   initialize the version number to 1. All subsequent writes will need to
   provide the current version number. If an out-of-date version number is
   supplied, an error will be thrown.

   Example of Dao constructed with optimistic locking enabled.

   ```typescript
   const dao = new DynamoDbDao<DataModel, KeySchema>({
     tableName,
     documentClient,
     {
        optimisticLockingAttribute: 'version',
        // If true, the first put or update will create and initialize
        // the 'version' attribute, otherwise it will not create it
        // This allows adopters to choose to adopt at the item level
        // or at the dao level
        autoInitiateLockingAttribute: true, // default: true
     }
   });
   ```

2. If you wish to ignore optimistic locking for a save operation, specify
   `ignoreOptimisticLocking: true` in the options on your `put`, `update`, or
   `delete`.

NOTE: Optimistic locking is NOT supported for `batchWrite` or `batchPut`
operations. Consuming those APIs for data models that do have optimistic locking
enabled may clobber your version data and could produce undesirable effects for
other callers.

This was modeled after the
[Java Dynamo client](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html)
implementation.

## Developing

The test setup requires that [docker-compose]() be installed. To run the tests,
first open one terminal and start the local DynamoDB docker container by
running:

```
yarn start:containers
```

In a second terminal run:

```
yarn test
```

To stop containers:

```
yarn stop:containers
```

## Releasing

Once you are ready to publish a new version, make sure all of your changes have
been pushed and merged to the remote repository.

Next, create a new branch and run the following command:

```
yarn version --minor (or --major or --patch)
```

This will add a commit with an updated `package.json`, and create a new tag
locally.

Then, push your branch and new tag to the remote.

```
git push && git push --tags
```

Create a pull request with the branch. Once that is merged, your new version
will be published.
