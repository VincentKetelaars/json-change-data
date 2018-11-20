import unittest

from main import ChangeDataDict, LookupType


class TestChangeDataDict(unittest.TestCase):

    def test_init(self):
        dic = {'x': 5, 'y': 'test', 3: 1.5}
        cdd = ChangeDataDict(dic=dic)
        # Verify the attributes are there
        self.assertEqual(cdd['x'], 5)
        self.assertEqual(cdd['y'], 'test')
        self.assertEqual(cdd[3], 1.5)

        # Verify dict functions apply
        self.assertEqual(sorted(cdd.keys()), [3, 'x', 'y'])
        
        # Verify that non attributes return error
        with self.assertRaises(KeyError):
            cdd['a']

        self.assertEqual(cdd.get('a', 1), 1)

    def test_set_and_lookup_ts(self):
        ts = 10
        cdd = ChangeDataDict(dic={1: 5}, set_ts=ts)

        # Cannot update the key with the same timestamp
        with self.assertRaises(ValueError):
            cdd[1] = 2

        cdd.set_ts = ts + 1
        cdd[1] = 2
        cdd.set_ts = ts + 2
        cdd[1] = 3

        # Lookup with last
        self.assertEqual(cdd[1], 3)

        # Lookup with first
        cdd.lookup_type = LookupType.FIRST
        self.assertEqual(cdd[1], 5)

        # Lookup with timestamp
        cdd.lookup_type = LookupType.TIMESTAMP
        cdd.lookup_ts = ts
        self.assertEqual(cdd[1], 5)
        cdd.lookup_ts = ts + 1
        self.assertEqual(cdd[1], 2)
        cdd.lookup_ts = ts + 2
        self.assertEqual(cdd[1], 3)
        cdd.lookup_ts = ts + 10
        self.assertEqual(cdd[1], 3)

        # Lookup before anything was added
        cdd.lookup_ts = ts - 5
        with self.assertRaises(KeyError):
            cdd[1]

    def test_delete(self):
        cdd = ChangeDataDict(
            dic={1: 5}, lookup_type=LookupType.FIRST, set_ts=0)
        cdd.set_ts = 1
        del cdd[1]
        self.assertEqual(cdd[1], 5)

        # Cannot delete something that does not exist
        with self.assertRaises(KeyError):
            del cdd[2]

        # Cannot lookup something that has been deleted
        cdd.lookup_type = LookupType.LAST
        with self.assertRaises(KeyError):
            cdd[1]

        # Cannot delete something that has already been deleted
        cdd.set_ts = 2
        with self.assertRaises(KeyError):
            del cdd[1]

        # Can assign a new value to deleted item
        cdd[1] = 4
        self.assertEqual(cdd[1], 4)

    def test_lazy_update(self):
        cdd = ChangeDataDict(dic={1: 5}, set_ts=0, lazy_update=True)
        cdd.set_ts = 1
        cdd[1] = 5

        # Because of the lazy_update, the first item is still the only item
        self.assertEqual(cdd.prior_ts(1), 0)

    def test_diff(self):
        cdd = ChangeDataDict(
            dic={1: 5, 2: 3}, set_ts=5)
        cdd.set_ts = 6
        cdd[1] = 6

        # The only difference is for key 1
        self.assertEqual(
            cdd.diff(LookupType.FIRST),
            {1: (6, 5)}
        )
        # Lookup before first timestamp
        not_exist = ChangeDataDict.NON_EXISTENT
        self.assertEqual(
            cdd.diff(LookupType.TIMESTAMP, lookup_ts=4),
            {1: (6, not_exist), 2: (3, not_exist)}
        )
        # Delete key 1
        cdd.set_ts = 7
        del cdd[1]
        self.assertEqual(
            cdd.diff(LookupType.FIRST),
            {1: (ChangeDataDict.DELETED, 5)}
        )


    def test_to_dict(self):
        cdd = ChangeDataDict(dic={1: 5, 2: 3}, set_ts=0)
        cdd.set_ts = 1
        cdd[1] = 6

        # By default we use LookupType.LAST
        self.assertEqual(
            cdd.to_dict(snapshot=True),
            {1: 6, 2: 3}
        )
        cdd.lookup_type = LookupType.FIRST
        self.assertEqual(
            cdd.to_dict(snapshot=True),
            {1: 5, 2: 3}
        )
        # Make sure that we can still create dicts after deleting
        del cdd[2]
        self.assertEqual(
            cdd.to_dict(snapshot=True),
            {1: 5, 2: 3}
        )
        cdd.lookup_type = LookupType.LAST
        self.assertEqual(
            cdd.to_dict(snapshot=True),
            {1: 6}
        )

    def test_to_json(self):
        cdd = ChangeDataDict(dic={1: 5}, set_ts=0)
        cdd.set_ts = 1
        cdd[1] = 4
        self.assertEqual(
            cdd.to_json(),
            '{"1": [{"ts": 0, "value": 5}, {"ts": 1, "value": 4}]}'
        )


if __name__ == '__main__':
    unittest.main()
