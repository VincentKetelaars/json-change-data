"""
The ChangeDataDict is a wrapper around a dictionary that keeps track of
historical changes. This allows for the lookup of all key value pairs that have
existed, and make use of the values at an exact point in time.

The format is as follows:
{
    "a": [{"ts": <timestamp>, "value": <obj>}, ...]
}

Where the list is in order of the sequential actions. Change actions are:
Insert, Update, Delete

# Special properties
There are special properties to each item:
ts: The timestamp of the action
value: The value linked to the key
del: In the case of a deletion it is set to True

# Optional properties
There are also optional properties that could be added:
source: The source of the change
version: To allow for version control

# Recursion
Note that the value can be a ChangeDataDict as well,
allowing for recursive dicts. However, this is not done by default, if a dict
is passed in as a value to a key, it will be treated as such.

# Lazy Updating
By default change actions will always incur a new item in the key's history,
however, the `lazy_update` parameter allows for optimizing if the value is
equal to the current value, i.e. no item will be added to the list.

# Mutable Objects
Mutable objects are allowed, but it should be clear that changes in those
objects are not captured by the ChangeDataDict. An explicit update is necessary
to capture any changes. References are not severed and are at the discretion of
the user.

# Timestamp
The timestamp defaults to the current timestamp, but can optionally be set
explicitly. It is however not permitted to use timestamps older than the latest
in use. Nor is it allowed to perform a change action with a timestamp not newer
than the previous action on a key.

# Read / Write independence
NOTE: Change actions are always appended to history, but read actions are
independent of writing. Hence if the lookup type is not latest, change actions
may not be reflected in subsequent reads.
"""

import calendar
from collections import MutableMapping
from datetime import datetime
import json


class LookupType(object):
    LAST = 1
    FIRST = 2
    TIMESTAMP = 3


class ChangeDataDict(MutableMapping):
    # TODO: For diff function, to be able to differentiate
    DELETED = (None, 'DELETED')
    NON_EXISTENT = (None, 'NON_EXISTENT')

    def __init__(self, dic=None, lookup_type=None, lookup_ts=None, set_ts=None,
                 lazy_update=False, version=None, source=None):
        self._store = dict()
        self._lookup_type = LookupType.LAST
        self._lookup_ts = None
        self._latest_ts = 0
        self._set_ts = None
        self._lazy_update = lazy_update
        self._version = version
        self._source = source

        # set_ts has to be in effect before self.update
        if set_ts is not None:
            self.set_ts = set_ts
        if dic is not None:
            self.update(dic)

        if lookup_type is not None:
            self.lookup_type = lookup_type
        if lookup_ts is not None:
            self.lookup_ts = lookup_ts

    @property
    def lookup_type(self):
        return self._lookup_type

    @lookup_type.setter
    def lookup_type(self, value):
        if value in range(1, 4):  # TODO: Easy, clean check on LookupType
            self._lookup_type = value
        else:
            raise ValueError(value)

    @property
    def lookup_ts(self):
        return self._lookup_ts

    @lookup_ts.setter
    def lookup_ts(self, value):
        if self.lookup_type == LookupType.TIMESTAMP and \
                isinstance(value, int):
            self._lookup_ts = value
        else:
            raise ValueError(value)

    @property
    def latest_ts(self):
        return self._latest_ts

    @property
    def set_ts(self):
        return self._set_ts

    @set_ts.setter
    def set_ts(self, value):
        if isinstance(value, int) and value >= self.latest_ts:
            self._set_ts = value
        else:
            raise ValueError(value)

    @property
    def lazy_update(self):
        return self._lazy_update

    @property
    def version(self):
        return self._version

    @property
    def source(self):
        return self._source

    def _get_item(self, key, lookup_type, lookup_ts=None):
        history = self._store[key]
        cur_item = None
        if lookup_type == LookupType.LAST:
            cur_item = history[-1]
        elif lookup_type == LookupType.FIRST:
            cur_item = history[0]
        elif lookup_type == LookupType.TIMESTAMP:
            # TODO: Faster implementation
            for item in history:
                if item['ts'] > lookup_ts:
                    break
                cur_item = item
        return cur_item

    def get_item(self, key):
        return self._get_item(key, self.lookup_type, lookup_ts=self.lookup_ts)

    def __getitem__(self, key):
        cur_item = self.get_item(key)
        if cur_item is None:
            raise KeyError(key)
        if cur_item.get('del', False):
            raise KeyError(key)
        return cur_item['value']

    def _create_item(self, value, delete=False, prior_ts=None):
        if self.set_ts is not None:
            ts = self.set_ts
        else:
            ts = calendar.timegm(datetime.utcnow().timetuple())
        if prior_ts is not None and prior_ts >= ts:
            raise ValueError(ts)
        self._latest_ts = ts  # Because set_ts cannot be lower, this is latest
        item = {'ts': ts, 'value': value}
        if delete:
            item['del'] = True
        if self.version is not None:
            item['version'] = self.version
        if self.source is not None:
            item['source'] = self.source
        return item

    def prior_ts(self, key):
        if key in self._store:
            return self._store[key][-1]['ts']
        return None

    def prior_value_is_equal(self, key, value):
        if key in self._store:
            prior_value = self._store[key][-1]['value']
            return prior_value == value
        return False

    def __setitem__(self, key, value):
        if self.lazy_update and self.prior_value_is_equal(key, value):
            return
        prior_ts = self.prior_ts(key)
        item = self._create_item(value, prior_ts=prior_ts)
        if key not in self._store:
            self._store[key] = []
        self._store[key].append(item)

    def prior_item_is_deleted(self, key):
        if key in self._store:
            return self._store[key][-1].get('del', False)
        return False

    def __delitem__(self, key):
        if not key in self._store:
            raise KeyError(key)
        if self.prior_item_is_deleted(key):
            raise KeyError(key)
        prior_ts = self.prior_ts(key)
        item = self._create_item(None, delete=True, prior_ts=prior_ts)
        self._store[key].append(item)

    def __iter__(self):
        for key in self._store:
            item = self.get_item(key)
            if not item.get('del', False):
                yield key

    def __len__(self):
        return len(self._store)

    def diff(self, lookup_type, lookup_ts=None):

        def to_value(item):
            if item is None:
                return self.NON_EXISTENT
            elif item.get('del', False):
                return self.DELETED
            return item['value']

        diff = {}
        for key in self._store:
            cur_item = self.get_item(key)
            comp_item = self._get_item(key, lookup_type, lookup_ts=lookup_ts)
            value = to_value(cur_item)
            comp_value = to_value(comp_item)
            if value != comp_value:
                diff[key] = (value, comp_value)
        return diff

    def to_dict(self, snapshot=False):
        # TODO: recursively handle ChangeDataDict
        if not snapshot:
            return dict(self._store)
        return dict(self.iteritems())

    def to_json(self, snapshot=False):
        return json.dumps(self.to_dict(snapshot=snapshot))
