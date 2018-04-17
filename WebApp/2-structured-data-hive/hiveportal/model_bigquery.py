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
from google.cloud import bigquery
from wtforms import Form, BooleanField, SelectField

#from time import sleep


builtin_list = list

class OldFilterForm(Form):
    builds = ['hg19', 'hg38']
    sources = ['ucsc', 'cosmic'] 

    # TODO: How do I programmatically generate wtforms fields?
    ## use setattr to create fields?
    ## use setattr to set fields as attributes after object of class
    ##     has been created
    ## http://wtforms.simplecodes.com/docs/1.0.1/specific_problems.html
    ## https://stackoverflow.com/questions/9561174/using-setattr-in-python?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
    ## Maybe I don't even need to use WTForms. Flask comes with request.form
    ##     and my impression of WTForms is that it is kind of shitty.
    ## Instead of using WTForms just use the methods used by /add in
    ##     bookshelf

    #organ_systems = [
    #                 'Endocrine', 
    #                 'Nervous', 
    #                 'Muscular',
    #                 'Digestive',
    #                 'Reproductive']

    #build_fields = []
    #for build in builds:
    #    build_field = BooleanField(build, default=True)
        #build_fields.append(build_field)
    # Builds

    Hg19 = BooleanField('Hg19', default=True)
    Hg38 = BooleanField('Hg38', default=True)

    build = SelectField('Reference Build', choices=['hg19','hg38'])

    # Sources
    UCSC = BooleanField('UCSC', default=True)
    Cosmic = BooleanField('Cosmic', default=True)

    # Signal
    eQTL = BooleanField('eQTL', default=True)

    # Organ Systems
    Endocrine = BooleanField('Endocrine', default=True)
    Nervous = BooleanField('Nervous', default=True)
    Muscular = BooleanField('Muscular', default=True)
    Digestive = BooleanField('Digestive', default=True)
    organ_systems = [Endocrine, Nervous, Muscular, Digestive]

class Section:

    def __init__(self, legend, entries=[]):
        self.legend = legend
        self.checked_entries = []
        self.unchecked_entries = []

    def add_field(self, field_type, id, name, value, display):
        entry = {
                 'type': field_type,
                 'id': id,
                 'name': name,
                 'value': value,
        }

        if display:
            self.checked_entries.append(entry)
        else:
            self.unchecked_entries.append(entry)


def old_populate_form(data={}):
    '''Populate data into form based on configuration info.

    Attr:
        form_config (list): A description of how the 
                            key:value data from the "data" object 
                            should be added to WTForms fields.

                            It is structured as:
                            form_config = [(name, field_type, [field_list])] 
        data (dict): Key = field name; Value = Diplay value
                     {
                      "circulatory": True,
                      "ucsc": True,
                      "hg19": True,
                      "hg38": False
                     }
    '''
    sections = []
    form_config = current_app.config['FILTER_CONFIG']
    #field_data = current_app.config['FILTER_DEFAULTS']


    for section_config in form_config:
        name = section_config[0]
        field_type = section_config[1]
        field_list = section_config[2]

        section = Section(name)

        for field_name in field_list:
            display = field_data[field_name]
            section.add_field(
                              field_type = field_type,
                              id = field_name,
                              name = field_name,
                              value = field_name, 
                              display = display)
        sections.append(section)
    #return sections

    sections = [
               {
                "name": "builds",
                "input_type": "radio",
                "legend": "Human Reference Builds",
                "unchecked": ["hg38"],
                "checked": ["hg19"]
               },
               {
                "name": "sources",
                "input_type": "checkbox",
                "legend": "Database Sources",
                "unchecked": [],
                "checked": ["UCSC", "cosmic"]
               }]
    return sections

def populate_form(field_data=None):
    '''Populate data into form based on configuration info.

    If data is NOT provided, all default values from config.py are used.
    If data IS provided, only the union of data and default values are 
    populated as checked. The rest are unchecked.

    If I don't separate sections into lists by input type, then I just
    have to used input type as a variable (fine) and be careful about 
    radio buttons(?). I think this is fine.

    Attr:
        form_config (list): A description of how the 
                            key:value data from the "data" object 
                            should be added to WTForms fields.

                            It is structured as:
                            form_config = [(name, field_type, [field_list])] 
        data (dict): Key = field name; Value = Diplay value
                     {
                      "circulatory": True,
                      "ucsc": True,
                      "hg19": True,
                      "hg38": False
                     }
    '''
    radio_sections = []
    checkbox_sections = []

    # QA output
    if field_data:
        print('DATA FROM FORM: ', field_data)

    # Use data posted from form if available
    form_config = current_app.config['FILTER_CONFIG']
    if field_data:
        pass
    elif not field_data:
        field_data = current_app.config['FILTER_DEFAULTS']

    for section in form_config:
        section_name = section['name']
        input_type = section['input_type']
        legend = section['legend']
        field_names = section['options']

        section['display'] = []

        if input_type == "checkbox":
            for field_name in field_names:
                if field_data.get(field_name):
                    #section['checked'].append(field_name)
                    section['display'].append(tuple((field_name, True)))
                else:
                    #section['unchecked'].append(field_name)
                    section['display'].append(tuple((field_name, False)))
            checkbox_sections.append(section)
        
        elif input_type == "radio":
            for field_name in field_names:
                if field_name == field_data.get(section_name):
                    section['display'].append(tuple((field_name, True)))
                else:
                    section['display'].append(tuple((field_name, False)))
            radio_sections.append(section)

    # QA output
    print('RADIO: ', radio_sections)
    print('CHECKBOX: ', checkbox_sections)
    
    return radio_sections, checkbox_sections

def init_app(app):
    pass

def get_client():
    return bigquery.Client(current_app.config['PROJECT_ID'])

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
def list(table_id, limit=10, cursor=None):
    print('LIST TABLE ID: {}'.format(table_id))
    print('LIST CURSOR: {}'.format(cursor))
    client = get_client()

    dataset_ref = client.dataset('annotation')
    dataset = bigquery.Dataset(dataset_ref)
    table_ref = dataset.table(table_id)
    
    #print('TABLE ID: {}'.format(table_id))
    print('TABLE REF: {}'.format(table_ref))
    table = client.get_table(table_ref)
    row_iterator = client.list_rows(
                                    table = table, 
                                    max_results = limit, 
                                    page_token = cursor)
    # Debugging lines
    print('TABLE: {}'.format(table))
    print('PAGE SIZE: {}'.format(limit))
    print('CURSOR: {}'.format(cursor))

    page = next(row_iterator.pages)

    entities = builtin_list(page)
    next_cursor = (
                   row_iterator.next_page_token
                   if row_iterator.next_page_token else None)
    # Debugging lines
    print('NEXT CURSOR: {}'.format(next_cursor))

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

def filter(data):
    print(data)
    client = get_client()

    sources = ['UCSC']
    #groups = ['gtex']   # Group is an SQL word
    signals = ['Eqtl']
    org_systems_master = ['Digestive','Nervous','Endocrine','Muscular']
    builds = ['hg19', 'hg38']

    #sources = [source for element in sources_master if element in data.keys]
    #signals = [signal for element in signals_master if element in data.keys]
    organ_systems = [system for system in org_systems_master if system in data.values()]

    query = """
            SELECT 
                AnnotationSetName,
                AnnotationSetVersion,
                AnnotationSetType,
                AnnotationSetFields,
                CreationDate,
                Build,
                AnnotationSetSize,
                Info,
                Source,
                Project,
                Signal,
                Tissue,
                OrganSystem
            FROM
                `gbsc-gcp-project-cba.annotation.annotation_gtex_curated2`
            WHERE
                Source IN UNNEST(@sources)
                AND Signal IN UNNEST(@signals)
                AND OrganSystem IN UNNEST(@organ_systems)
                AND Build IN UNNEST(@builds)
            LIMIT 100
            """
    query_params = [
        bigquery.ArrayQueryParameter('sources', 'STRING', sources),
        bigquery.ArrayQueryParameter('signals', 'STRING', signals),
        bigquery.ArrayQueryParameter('organ_systems', 'STRING', organ_systems),
        bigquery.ArrayQueryParameter('builds', 'STRING', builds)]
    
    job_config = bigquery.QueryJobConfig()

    # Run a query using a named query parameter(s)
    job_config.query_parameters = query_params

    # Write query results to a destination table
    table_id = 'webportal_results'
    table_ref = client.dataset('annotation').table(table_id)
    job_config.destination = table_ref

    # The write_disposition specifies the behavior when writing query results
    # to a table that already exists. With WRITE_TRUNCATE, any existing rows
    # in the table are overwritten by the query results.
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.create_disposition = 'CREATE_IF_NEEDED'

    print('FILTER TABLE REF: {}'.format(table_ref))
    query_job = client.query(query, job_config=job_config)
    result = query_job.result(timeout=60)
    #while not query_job.done:
    #    sleep(0.3)
    #rows = list(query_job) # Waits for the query to finish

    #print('FILTER LEN RESULT: {}'.format(len(rows)))
    print('FILTER TABLE ID: {}'.format(table_id))
    #return len(rows)


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

def get_rows(sources, groups, signals, organ_systems, limit):
    from google.cloud import bigquery

    client = bigquery.Client()

    query = """
            SELECT 
                AnnotationSetName,
                Build,
                Source,
                Group,
                Signal,
                Tissue,
                OrganSystem
            FROM
                `gbsc-gcp-project-cba.annotation.annotation_gtex_curated`
            WHERE
                Source IN UNNEST(@sources)
                AND Group IN UNNEST(@groups)
                AND Signal IN UNNEST(@signals)
                AND OrganSystem IN UNNEST(@organ_systems)
            LIMIT @limit;
            """
    query_params = [
        bigquery.ArrayQueryParameter('sources', 'STRING', sources),
        bigquery.ArrayQueryParameter('groups', 'STRING', groups),
        bigquery.ArrayQueryParameter('signals', 'STRING', signals),
        bigquery.ArrayQueryParameter('organ_systems', 'STRING', organ_systems)]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(query, job_config=job_config)

    iterator = query_job.result()
    rows = []
    while limit >= 0:
        row = iterator.next()
        rows.append()
        limit -= 1
    return rows
