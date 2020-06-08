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
import AWS from 'aws-sdk';
import DynamoDbDao from '@jupiterone/dynamodb-dao';

const dynamodb = new AWS.DynamoDB({
  apiVersion: '2012-08-10'
});

const documentClient = new AWS.DynamoDB.DocumentClient({
  service: dynamodb
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
  5,
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
  5,
);
```

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
