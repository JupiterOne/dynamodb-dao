import { v4 as uuid } from 'uuid';
import TestContext, { DataModel, KeySchema } from './helpers/TestContext';

let context: TestContext;

beforeAll(async () => {
  context = await TestContext.setup();
});

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

test(`#multiIncr should be supported`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
  };

  await context.dao.put(data);

  const result = await context.dao.multiIncr(
    {
      id: key.id,
    },
    { version: 1 }
  );

  const expected: DataModel = {
    ...data,
    version: 2,
  };

  expect(result).toEqual(expected);
});

test(`#multiIncr should support a custom number to increment by`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
  };

  await context.dao.put(data);

  const result = await context.dao.multiIncr(
    {
      id: key.id,
    },
    { version: 5 }
  );

  const expected: DataModel = {
    ...data,
    version: 6,
  };

  expect(result).toEqual(expected);
});

test(`#multiIncr should support incrementing 2 properties at once.`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
    extra: 1,
  };

  await context.dao.put(data);

  const result = await context.dao.multiIncr(
    {
      id: key.id,
    },
    { version: 5, extra: 10 }
  );

  const expected: DataModel = {
    ...data,
    version: 6,
    extra: 11,
  };

  expect(result).toEqual(expected);
});

test(`#multiIncr should support incrementing 2 properties at once multiple times.`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
    extra: 1,
  };

  await context.dao.put(data);

  await context.dao.multiIncr(
    {
      id: key.id,
    },
    { version: 1, extra: 1 }
  );
  const result = await context.dao.multiIncr(
    {
      id: key.id,
    },
    { version: 1, extra: 1 }
  );

  const expected: DataModel = {
    ...data,
    version: 3,
    extra: 3,
  };

  expect(result).toEqual(expected);
});
