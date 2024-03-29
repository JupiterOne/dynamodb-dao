import { v4 as uuid } from 'uuid';
import TestContext from './helpers/TestContext';

let context: TestContext;

afterAll(() => {
  if (context) {
    return context.teardown();
  }
});

describe.each([true, false])(
  'delete with locking and auto-initiate %s',
  (autoInitiateLockingAttribute) => {
    beforeAll(async () => {
      context = await TestContext.setup(true, autoInitiateLockingAttribute);
    });
    test('should require version number to remove item from the table', async () => {
      const { dao } = context;

      const key = { id: uuid() };
      const updateData = { test: uuid(), newField: uuid(), version: 1 };

      await dao.update(key, updateData);
      await dao.update(key, { ...updateData, version: 1 });

      // ensure it exists
      const item = await dao.get(key);
      expect(item).toEqual({
        ...key,
        ...updateData,
        version: 2,
      });

      await dao.delete(key, undefined, {
        version: 2,
      });

      expect(await dao.get(key)).toBeUndefined();
    });

    test('should error when version number is missing when removing item from the table', async () => {
      const { dao } = context;

      const key = { id: uuid() };
      const updateData = { test: uuid(), newField: uuid(), version: 1 };

      // put data into dynamodb, which should set the version number
      await dao.update(key, updateData);
      await dao.update(key, { ...updateData, version: 1 });

      await expect(async () => {
        await dao.delete(key);
      }).rejects.toThrow('The conditional request failed');
    });

    test('should error when version number is old when removing item from the table', async () => {
      const { dao } = context;

      const key = { id: uuid() };
      const updateData = { test: uuid(), newField: uuid(), version: 1 };

      await dao.update(key, updateData);
      await dao.update(key, { ...updateData, version: 1 });

      await expect(async () => {
        await dao.delete(key, undefined, {
          version: 1,
        });
      }).rejects.toThrow('The conditional request failed');
    });

    test('should not require version number to remove item from the table when ignore flag is set', async () => {
      const { dao } = context;

      const key = { id: uuid() };
      const updateData = { test: uuid(), newField: uuid(), version: 1 };

      await dao.update(key, updateData);

      // ensure it exists
      const item = await dao.get(key);
      expect(item).toEqual({
        ...key,
        ...updateData,
        version: 1,
      });

      await dao.delete(key, { ignoreOptimisticLocking: true });

      // ensure it deleted
      const deletedItem = await dao.get(key);
      expect(deletedItem).toEqual(undefined);
    });

    if (autoInitiateLockingAttribute) {
      test('should require version number to remove an auto-initiated item from the table', async () => {
        const { dao } = context;

        const key = { id: uuid() };
        const updateData = { test: uuid(), newField: uuid() };

        await dao.update(key, updateData);

        // ensure it exists
        const item = await dao.get(key);
        expect(item).toEqual({
          ...key,
          ...updateData,
          version: autoInitiateLockingAttribute ? 1 : undefined,
        });

        await expect(async () => {
          await dao.delete(key, undefined);
        }).rejects.toThrow('The conditional request failed');
      });
    } else {
      test('should not require version number to remove item from the table', async () => {
        const { dao } = context;

        const key = { id: uuid() };
        const updateData = { test: uuid(), newField: uuid() };

        await dao.update(key, updateData);

        // ensure it exists
        const item = await dao.get(key);
        expect(item).toEqual({
          ...key,
          ...updateData,
          version: autoInitiateLockingAttribute ? 1 : undefined,
        });
        await dao.delete(key);

        // ensure it deleted
        const deletedItem = await dao.get(key);
        expect(deletedItem).toEqual(undefined);
      });
    }
  }
);
