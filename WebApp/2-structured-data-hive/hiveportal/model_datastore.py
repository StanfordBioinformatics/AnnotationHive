# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import current_app
from google.cloud import datastore


builtin_list = list


def init_app(app):
    pass


def get_client():
    return datastore.Client(current_app.config['PROJECT_ID'])


# [START from_datastore]
def from_datastore(entity):
    """Translates Datastore results into the format expected by the
    application.

    Datastore typically returns:
        [Entity{key: (kind, id), prop: val, ...}]

    This returns:
        {id: id, prop: val, ...}
    """
    if not entity:
        return None
    if isinstance(entity, builtin_list):
        entity = entity.pop()

    entity['id'] = entity.key.id
    return entity
# [END from_datastore]


# [START list]
def list(limit=10, cursor=None):
    ds = get_client()

    query = ds.query(
                     namespace="mvp-phase-2", 
                     kind='gvcf', 
                     order=['sample'])
    query_iterator = query.fetch(
                                 limit=limit, 
                                 start_cursor=cursor)
    page = next(query_iterator.pages)

    entities = builtin_list(map(from_datastore, page))
    next_cursor = (
        query_iterator.next_page_token.decode('utf-8')
        if query_iterator.next_page_token else None)

    return entities, next_cursor
# [END list]

# [START list]
def summary(limit=1, cursor=None):
    ds = get_client()

    query = ds.query(
                     namespace="mvp-phase-2", 
                     kind='gvcf')

    entity_counts = get_object_counts(query, {}, limit, cursor)
    print("result: ", entity_counts)
    return entity_counts
# [END list]

def hive(limit=None, cursor=None):
    ds = get_client()

    query = ds.query(
                     namespace="annotation-hive",
                     kind="proto-db",
                     order=['annotation_set_name'])

    query_iterator = query.fetch(
                                 limit=limit, 
                                 start_cursor=cursor)
    page = next(query_iterator.pages)

    entities = builtin_list(map(from_datastore, page))
    next_cursor = (
        query_iterator.next_page_token.decode('utf-8')
        if query_iterator.next_page_token else None)

    return entities, next_cursor


def read(id):
    ds = get_client()
    key = ds.key('Object', int(id))
    results = ds.get(key)
    return from_datastore(results)


# [START update]
def update(data, id=None):
    ds = get_client()
    namespace = "mvp_phase_2"
    kind = "gvcf"
    if id:
        key = ds.key(namespace, kind, int(id))
    else:
        key = ds.key(namespace, kind)

    entity = datastore.Entity(
                              key=key,
                              exclude_from_indexes=['description'])

    entity.update(data)
    ds.put(entity)
    return from_datastore(entity)


create = update
# [END update]


def delete(id):
    ds = get_client()
    key = ds.key('Book', int(id))
    ds.delete(key)

def get_object_counts(query, entity_counts, limit, next_cursor):
    """Recursive function to get all object counts
    """
    print("start: ", entity_counts)
    print(next_cursor)
    if next_cursor == False:
        print("returning")
        return entity_counts
    else:
        query_iterator = query.fetch(
                                     limit=limit, 
                                     start_cursor=next_cursor)
        page = next(query_iterator.pages)
        entities = builtin_list(map(from_datastore, page))
        # Update count of different filetypes
        for entity in entities:
            print(entity)
            if entity['filetype'] in entity_counts.keys():
                entity_counts[entity['filetype']] += 1
            else:
                entity_counts[entity['filetype']] = 1

        next_cursor = (
                       query_iterator.next_page_token.decode('utf-8')
                       if query_iterator.next_page_token else False)
        print("end: ", entity_counts)
        return get_object_counts(query, entity_counts, limit, next_cursor)
