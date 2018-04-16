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

#from gvcflib import get_model
from hiveportal import get_model
from flask import Blueprint, redirect, render_template, request, url_for


crud = Blueprint('crud', __name__)


# [START list]
"""
@crud.route("/")
def list():
    token = request.args.get('page_token', None)
    if token:
        token = token.encode('utf-8')

    objects, next_page_token = get_model().list(cursor=token)

    return render_template(
        "list.html",
        objects=objects,
        next_page_token=next_page_token)
"""
# [END list]

@crud.route("/")
def home():
    table_id = "annotation_gtex_curated"
    return redirect(url_for('.list', table_id=table_id))


# [START list]
#@crud.route("/list")
@crud.route('/list/<table_id>')
def list(table_id):
    token = request.args.get('page_token', None)
    #if not table_id: 
    #    table_id = 'annotation_gtex_curated'
    #if token:
    #    token = token.encode('utf-8')

    objects, next_page_token = get_model().list(
                                                table_id = table_id, 
                                                cursor = token)

    return render_template(
                           "list.html",
                           objects=objects,
                           next_page_token=next_page_token)
# [END list]

@crud.route("/filter_static")
def filter_static():
    token = request.args.get('page_token', None)
    table_id = 'webportal_results'
    #table_name = 'webportal_results'
    #if token:
    #    token = token.encode('utf-8')

    get_model().filter(table_id)
    print("CRUD TABLE_ID: {}".format(table_id))

    objects, next_page_token = get_model().list(
                                                table_id = table_id,
                                                cursor = token)

    return render_template(
        "list.html",
        objects=objects,
        next_page_token=next_page_token)

@crud.route("/filter", methods=['GET', 'POST'])
def filter(): 
    #form = get_model().FilterForm(request.form)

    if request.method == 'POST':
        data = request.form.to_dict(flat=True)
        #table_id = 'gbsc-gcp-project-cba.annotation.annotation_gtex_curated'
        get_model().filter(data)
        
        #table_id = 'webportal_results'
        #objects, next_page_token = get_model().list(
        #                                            table_id = table_id)
        return redirect(url_for('.list', table_id='webportal_results'))
        #return render_template(
        #                       "list.html", 
        #                       objects=objects, 
        #                       next_page_token=next_page_token)
    else:
        #form = get_model().FilterForm()
        radio_sections, checkbox_sections = get_model().populate_form()
        return render_template(
                               "filter.html", 
                               radio_sections = radio_sections,
                               checkbox_sections = checkbox_sections)

    '''
    # QA block
    if request.method == 'POST':
        data = request.form.to_dict(flat=True)
        radio_sections, checkbox_sections = get_model().populate_form(data)
        return render_template(
                               "filter.html", 
                               radio_sections = radio_sections,
                               checkbox_sections = checkbox_sections)
    '''


@crud.route('/<id>')
def view(id):
    book = get_model().read(id)
    return render_template("view.html", book=book)


# [START add]
@crud.route('/add', methods=['GET', 'POST'])
def add():
    if request.method == 'POST':
        data = request.form.to_dict(flat=True)

        ds_object = get_model().create(data)

        return redirect(url_for('.view', id=ds_object['id']))

    return render_template("form.html", action="Add", object={})
# [END add]

# [START list]
@crud.route('/summary')
def summary():
    token = request.args.get('page_token', None)
    if token:
        token = token.encode('utf-8')

    objects = get_model().summary(cursor=token)

    return render_template(
                           "summary.html",
                           objects=objects)
# [END list]

'''
# [START list]
@crud.route('/hive')
def hive():
    token = request.args.get('page_token', None)
    if token:
        token = token.encode('utf-8')

    objects, next_page_token = get_model().hive(cursor=token)

    return render_template(
                           "hive.html",
                           objects=objects,
                           next_page_token=next_page_token)
# [END list]
'''

@crud.route("/combined", methods=['GET', 'POST'])
def combined(): 
    token = request.args.get('page_token', None)
    if request.method == 'POST':

        data = request.form.to_dict(flat=True)
        get_model().filter(data)

        radio_sections, checkbox_sections = get_model().populate_form(data)

        table_id = 'webportal_results'
        objects, next_page_token = get_model().list(
                                                    table_id = table_id,
                                                    cursor = token)

        return render_template(
                               'combined.html', 
                               objects = objects, 
                               next_page_token=next_page_token,
                               radio_sections = radio_sections,
                               checkbox_sections = checkbox_sections)
    else:
        #form = get_model().FilterForm()
        #token = request.args.get('page_token', None)
        radio_sections, checkbox_sections = get_model().populate_form()

        table_id = 'annotation_gtex_curated'
        objects, next_page_token = get_model().list(
                                                    table_id = table_id,
                                                    cursor = token)
        return render_template(
                               'combined.html', 
                               objects = objects,
                               next_page_token = next_page_token,
                               radio_sections = radio_sections,
                               checkbox_sections = checkbox_sections)


@crud.route('/<id>/edit', methods=['GET', 'POST'])
def edit(id):
    book = get_model().read(id)

    if request.method == 'POST':
        data = request.form.to_dict(flat=True)

        book = get_model().update(data, id)

        return redirect(url_for('.view', id=book['id']))

    return render_template("form.html", action="Edit", book=book)


@crud.route('/<id>/delete')
def delete(id):
    get_model().delete(id)
    return redirect(url_for('.list'))
