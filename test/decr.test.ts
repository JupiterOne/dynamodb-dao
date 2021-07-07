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

test(`#decr should be supported`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
  };

  await context.dao.put(data);

  const result = await context.dao.decr(
    {
      id: key.id,
    },
    'version' as any,
  );

  const expected: DataModel = {
    ...data,
    version: 0,
  };

  expect(result).toEqual(expected);
});

test(`#decr should support passing a custom number to decrement by`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 5,
  };

  await context.dao.put(data);

  const result = await context.dao.decr(
    {
      id: key.id,
    },
    'version' as any,
    3,
  );

  const expected: DataModel = {
    ...data,
    version: 2,
  };

  expect(result).toEqual(expected);
});

test(`#decr should default to value 0 if property does not exist`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
  };

  await context.dao.put(data);

  const result = await context.dao.decr(
    {
      id: key.id,
    },
    'version' as any,
  );

  const expected: DataModel = {
    ...data,
    version: -1,
  };

  expect(result).toEqual(expected);
});
