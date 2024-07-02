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

test(`#incr should be supported`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
  };

  await context.dao.put(data);

  const result = await context.dao.incr(
    {
      id: key.id,
    },
    'version' as never
  );

  const expected: DataModel = {
    ...data,
    version: 2,
  };

  expect(result).toEqual(expected);
});

test(`#incr should support a custom number to increment by`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
    version: 1,
  };

  await context.dao.put(data);

  const result = await context.dao.incr(
    {
      id: key.id,
    },
    'version' as never,
    5
  );

  const expected: DataModel = {
    ...data,
    version: 6,
  };

  expect(result).toEqual(expected);
});

test(`#incr should set 0 if the property does not exist`, async () => {
  const key: KeySchema = {
    id: uuid(),
  };

  const data: DataModel = {
    ...key,
    test: uuid(),
  };

  await context.dao.put(data);

  const result = await context.dao.incr(
    {
      id: key.id,
    },
    'version' as never
  );

  const expected: DataModel = {
    ...data,
    version: 1,
  };

  expect(result).toEqual(expected);
});
