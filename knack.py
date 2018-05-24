# Imports =====================================================================

import json
from urllib import urlencode

from helpers import requests_retry_session

# Exceptions ==================================================================


class KnackError(Exception):
    """Knack error"""
    pass

# =============================================================================


class KnackClient(object):
    """Knack Client"""

    OBJECTS_ENDPOINT = 'https://api.knack.com/v1/objects'

    # -------------------------------------------------------------------------

    def __init__(self, application_id, api_key):
        """Initialize a new Knack client session"""
        self.session = requests_retry_session()
        self.session.headers.update({
            'X-Knack-Application-Id': application_id,
            'X-Knack-REST-API-KEY': api_key,
        })
        self._objects = {}
        self._fields = {}

    # -------------------------------------------------------------------------

    @property
    def objects(self):
        """Retrieve objects"""
        if not self._objects:
            data = self.session.get(self.OBJECTS_ENDPOINT).json()
            self._objects.update({
                object['name']: object['key']
                for object in data['objects']
            })

        return self._objects

    # -------------------------------------------------------------------------

    def object_fields(self, object_name):
        """Retrieve fields of the specified object"""
        if object_name not in self._fields:
            object_key = self.objects[object_name]
            url = '/'.join((self.OBJECTS_ENDPOINT, object_key))
            data = self.session.get(url).json()
            self._fields.update({
                object_name: {
                    field['name']: field['key']
                    for field in data['object']['fields']
                }
            })

        return self._fields[object_name]

    # -------------------------------------------------------------------------

    def get(self, object_name, filters=None, count=1000):
        """Fetch object records"""
        object_key = self.objects[object_name]
        object_fields = self.object_fields(object_name)

        base_url = '/'.join((self.OBJECTS_ENDPOINT, object_key, 'records'))
        params = {'rows_per_page': count}
        if filters:
            for filter in filters:
                filter['field'] = object_fields[filter['field']]
            params.update({'filters': json.dumps(filters)})
        query = urlencode(params)
        url = '{base_url}?{query}'.format(base_url=base_url, query=query)

        response = self.session.get(url)
        if not response.ok:
            raise KnackError(response.text)

        records = []
        fields = {
            field_key: field_name
            for field_name, field_key in object_fields.items()
        }
        for each in response.json()['records']:
            record = {}
            for key, value in each.items():
                is_raw = key.endswith('_raw')
                if not is_raw and key + '_raw' in each:
                    continue
                key = key.replace('_raw', '')
                if key in fields:
                    record.update({fields[key]: value})
                else:
                    record.update({key: value})
            records.append(record)
        return records

    # -------------------------------------------------------------------------

    def get_record(self, object_name, record_id):
        """Fetch record"""
        object_key = self.objects[object_name]
        url = '/'.join((self.OBJECTS_ENDPOINT,
                        object_key, 'records', record_id))
        response = self.session.get(url)
        if not response.ok:
            raise KnackError(response.text)
        fields = {
            field_name: field_key
            for field_key, field_name in self.object_fields(object_name).items()
        }
        record = {}
        data = response.json()
        for key, value in data.items():
            is_raw = key.endswith('_raw')
            if not is_raw and key + '_raw' in data:
                continue
            key = key.replace('_raw', '')
            if key in fields:
                record.update({fields[key]: value})
            else:
                record.update({key: value})
        return record

    # -------------------------------------------------------------------------

    def update_record(self, object_name, record_id, fields):
        """Update record"""
        object_key = self.objects[object_name]
        object_fields = self.object_fields(object_name)

        url = '/'.join((self.OBJECTS_ENDPOINT,
                        object_key, 'records', record_id))

        payload = {}
        for key, value in fields.items():
            payload[object_fields[key]] = value

        headers = {'Content-Type': 'application/json'}
        response = self.session.put(
            url, headers=headers, data=json.dumps(payload))
        if not response.ok:
            raise KnackError(response.text)

        fields = {
            field_name: field_key
            for field_key, field_name in object_fields.items()
        }
        record = {}
        data = response.json()
        for key, value in data.items():
            is_raw = key.endswith('_raw')
            if not is_raw and key + '_raw' in data:
                continue
            key = key.replace('_raw', '')
            if key in fields:
                record.update({fields[key]: value})
        return record

# END =========================================================================
