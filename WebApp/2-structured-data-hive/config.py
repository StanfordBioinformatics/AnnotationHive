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

"""
This file contains all of the configuration values for the application.
Update this file with the values for your specific Google Cloud project.
You can create and manage projects at https://console.developers.google.com
"""

import os

# The secret key is used by Flask to encrypt session cookies.
SECRET_KEY = 'secret'

# There are three different ways to store the data in the application.
# You can choose 'datastore', 'cloudsql', or 'mongodb'. Be sure to
# configure the respective settings for the one you choose below.
# You do not have to configure the other data backends. If unsure, choose
# 'datastore' as it does not require any additional configuration.
DATA_BACKEND = 'bigquery'

# Google Cloud Project ID. This can be found on the 'Overview' page at
# https://console.developers.google.com
PROJECT_ID = 'gbsc-gcp-project-cba'

# CloudSQL & SQLAlchemy configuration
# Replace the following values the respective values of your Cloud SQL
# instance.
CLOUDSQL_USER = 'root'
CLOUDSQL_PASSWORD = 'your-cloudsql-password'
CLOUDSQL_DATABASE = 'bookshelf'
# Set this value to the Cloud SQL connection name, e.g.
#   "project:region:cloudsql-instance".
# You must also update the value in app.yaml.
CLOUDSQL_CONNECTION_NAME = 'your-cloudsql-connection-name'

# The CloudSQL proxy is used locally to connect to the cloudsql instance.
# To start the proxy, use:
#
#   $ cloud_sql_proxy -instances=your-connection-name=tcp:3306
#
# Port 3306 is the standard MySQL port. If you need to use a different port,
# change the 3306 to a different port number.

# Alternatively, you could use a local MySQL instance for testing.
LOCAL_SQLALCHEMY_DATABASE_URI = (
    'mysql+pymysql://{user}:{password}@127.0.0.1:3306/{database}').format(
        user=CLOUDSQL_USER, password=CLOUDSQL_PASSWORD,
        database=CLOUDSQL_DATABASE)

# When running on App Engine a unix socket is used to connect to the cloudsql
# instance.
LIVE_SQLALCHEMY_DATABASE_URI = (
    'mysql+pymysql://{user}:{password}@localhost/{database}'
    '?unix_socket=/cloudsql/{connection_name}').format(
        user=CLOUDSQL_USER, password=CLOUDSQL_PASSWORD,
        database=CLOUDSQL_DATABASE, connection_name=CLOUDSQL_CONNECTION_NAME)

if os.environ.get('GAE_INSTANCE'):
    SQLALCHEMY_DATABASE_URI = LIVE_SQLALCHEMY_DATABASE_URI
else:
    SQLALCHEMY_DATABASE_URI = LOCAL_SQLALCHEMY_DATABASE_URI

# Mongo configuration
# If using mongolab, the connection URI is available from the mongolab control
# panel. If self-hosting on compute engine, replace the values below.
MONGO_URI = \
    'mongodb://user:password@host:27017/database'

# BigQuery filter form configuration
'''
FILTER_CONFIG = [
                 ('build', 'checkbox', ['hg19', 'hg38']),
                 ('source', 'checkbox', ['UCSC', 'Cosmic']),
                 ('organ_systems', 'checkbox', [
                                                'Endocrine', 
                                                'Nervous', 
                                                'Muscular', 
                                                'Digestive'])]
'''
FILTER_CONFIG = [
                 #{
                  #"name": "annotation_type",
                  #"input_type": "radio",
                  #"legend": "Annotation Type",
                  #"options": [
                  #            "Variant-based", 
                  #            "Region-based",
                  #            "Gene-based"]
                 #},
                 {
                  "name": "build",
                  "input_type": "radio",
                  "legend" : "Reference Genomes",
                  "options" : [
                               "hg19", "hg38", "mm10", "papAnu2", "equCab2", 
                               "eboVir3", "loxAfr3", "melGal5", "cavPor3", 
                               "ailMel1", "canFam3", "gorGor5", "panTro5", 
                               "bosTau8", "chlSab2", "rhiRox1"]
                 },
                 {
                  "name": "source",
                  "input_type": "checkbox",
                  "legend": "Database Sources",
                  "options": ["UCSC", "Cosmic"]
                 },
                 {
                  "name": "organ_systems",
                  "input_type": "checkbox",
                  "legend": "Organ Systems",
                  "options": [
                              "Endocrine",
                              "Nervous",
                              "Muscular",
                              "Digestive"]
                 }]

# How do I dynamically populate these?
# Figure it out later?
# Get it by querying tables/metadata
# At some point I will get it dynamically via querying something
# Not sure I even need this
FILTER_DEFAULTS = {
                   # radio options
                   #"annotation_type": "Variant-based",
                   "build": "hg19",
                   # checkbox options
                   "UCSC": "UCSC",
                   "Cosmic": "Cosmic",
                   "Endocrine": "Endocrine",
                   "Nervous": "Nervous",
                   "Muscular": "Muscular",
                   "Digestive": "Digestive"
                  }