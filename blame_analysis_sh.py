#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#

import argparse
import logging
import json
import shelve
import datetime
import os.path
import elasticsearch
import elasticsearch.helpers
from perceval.backends import GitBlame
import sortinghat.api
import sortinghat.db.database

import urllib3
urllib3.disable_warnings()

description = """Analyze a git repository using Perceval GitBlame.

    """

def parse_args ():

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-l", "--logging", type=str, choices=["info", "debug"],
                        help = "Logging level for output")
    parser.add_argument("--logfile", type=str,
                        help = "Log file")
    parser.add_argument("--repodir", type=str,
                        help = "Directory for the repository")
    parser.add_argument("--store", type=str,
                        help = "File for storing the raw data from git blame")
    parser.add_argument("--processed", type=str,
                        help = "File for the processed data")
    parser.add_argument("--uploaded", type=str,
                        help = "File for the uploaded data")
    parser.add_argument("--store_only", action='store_true',
                        help = "Only produce the store (raw data), and stop afterwards")
    parser.add_argument("--process_only", action='store_true',
                        help = "Only process data (including producing store, if needed), and stop afterwards")
    parser.add_argument("--sortinghat", action='store_true',
                        help = "Process identities with sorting Hat")
    parser.add_argument("--assume_processed", action='store_true',
                        help = "Assume data is fully processed")
    parser.add_argument("--assume_store", action='store_true',
                        help = "Assume store (git blame raw data) was already produced")
    parser.add_argument("repouri", type=str,
                        help = "URI for the repository")
    parser.add_argument("-e", "--es_url", type=str,
                        help = "ElasticSearch url (http://user:secret@host:port/res)")
    parser.add_argument("-i", "--es_index", type=str, default="blame",
                        help = "ElasticSearch index prefix")
    parser.add_argument("--shdb", type=str,
                        help = "Sorting Hat database")
    parser.add_argument("--shuser", type=str,
                        help = "Sorting Hat database user")
    parser.add_argument("--shpasswd", type=str,
                        help = "Sorting Hat database passwd")
    parser.add_argument("--shhost", type=str,
                        help = "Sorting Hat database host")
    args = parser.parse_args()
    return args

def blame_analysis(repouri, repodir, store):
    """Analyze blame, storing data in store.

    """

#    store = shelve.open(storename)
    git_blame = GitBlame(uri=repouri, gitpath=repodir)

    try:
        nsnippet = 0
        nfile = 0
        filename = None
        for snippet in git_blame.blame():
            if filename != snippet['data']['filename']:
                # Getting snippets for a new file
                # Store the previous one, and prepare for the new one
                if filename != None:
                    store[filename] = snippets
                nfile += 1
                filename = snippet['data']['filename']
                logging.info('File %d, snippet %d: %s.', nfile, nsnippet, filename)
                snippets = []
            logging.debug(json.dumps(snippet, indent=4, sort_keys=True))
            logging.debug('')
            nsnippet += 1
            snippets.append(snippet)
        if filename != None:
            store[filename] = snippets

        print("Analyzed files: ", nfile)
        print("Analyzed snippets: ", nsnippet)
        store.sync()
    except OSError as e:
        store.sync()
        raise RuntimeError(str(e))
    except Exception as e:
        store.sync()
        raise RuntimeError(str(e))

class Identities():

    def __init__(self, user, password, database, host):

        self.db = sortinghat.db.database.Database(user, password, database, host)
        self.ids = {}

    def add(self, name, email):

        email = email.lstrip('<').rstrip('>')
        key = email + '|' + 'name'
        if key not in self.ids:
            try:
                uuid = sortinghat.api.add_identity(db=self.db, source="git",
                                                email=email, name=name)
                logging.info("Adding identity to Sorting Hat: %s, %s", name, email)

            except sortinghat.exceptions.AlreadyExistsError as e:
                logging.info("Identity already in Sorting Hat: %s, %s", name, email)
                uuid = e.uuid
            self.ids[key] = True

def first_time (time, proposed_time):
    """Check if time is first (earliest) than proposed_time.

    :param          time: Current time, could be None
    :param proposed_time: Proposed time, to be checked
    :returns:             proposed_time, if time is None, earliest, otherwise

    """

    if time is None:
        return proposed_time
    else:
        if proposed_time < time:
            return proposed_time
        else:
            return time

def last_time (time, proposed_time):
    """Check if time is last (earliest) than proposed_time.

    :param          time: Current time, could be None
    :param proposed_time: Proposed time, to be checked
    :returns:             proposed_time, if time is None, latest, otherwise

    """

    if time is None:
        return proposed_time
    else:
        if proposed_time > time:
            return proposed_time
        else:
            return time

def blame_process(store, processed, processed_files, identities=None,
                now=datetime.datetime.utcnow().timestamp()):
    """Process git blame raw data.

    Reads raw data in store, to produce data in processed, better
    organized for further analysis.

    processed could be partially filled in, in which case old data
    will be preserved, acting as a kind of cache.

    :param store:     shelve file with git blame raw data
    :param processed: shelve file with processed data
    :param processed_files: shelve file with processed data about files
    :param identities: Sorting Hat identities (Identities object)
    :param now:       timestamp considered as "now" (default: datetime.utcnow().timestamp())

    """

    nfile = 0
    nhash = 0
    files_done = 0
    errors = []

    for file in store:
        if file in processed:
            files_done += 1
            continue

        nfile += 1
        snippets = store[file]
        data = {}
        file_data = {}
        file_components = file.split('/',4)
        if len(file_components) > 1:
            dir1 = file_components[0]
        else:
            dir1 = None
        if len(file_components) > 2:
            dir2 = file_components[1]
        else:
            dir2 = None
        if len(file_components) > 3:
            dir3 = file_components[2]
        else:
            dir3 = None
        if len(file_components) > 4:
            dir4 = file_components[3]
        else:
            dir4 = None
        ext = os.path.splitext(file)[1]

        processed_files[file] = {
            'name': file,
            'dir1': dir1,
            'dir2': dir2,
            'dir3': dir3,
            'dir4': dir4,
            'ext': ext
        }
        first_commit = None
        last_commit = None
        first_author = None
        last_author = None

        for snippet in snippets:
            snippet_data = snippet['data']
            hash = snippet_data['hash']
            logging.debug("snippet_data: " + str(snippet_data))

            if hash not in data:
                nhash += 1
                try:
                    data[hash] = {
                        'file': file,
                        'hash': snippet_data['hash'],
                        'committer_time': int(snippet_data['committer-time']),
                        'author_time': int(snippet_data['author-time']),
                        'committer_tz': int(snippet_data['committer-tz'])//100,
                        'author_tz': int(snippet_data['author-tz'])//100,
                        'committer_duration': now - int(snippet_data['committer-time']),
                        'author_duration': now - int(snippet_data['author-time']),
                        'committer': snippet_data['committer'],
                        'author': snippet_data['author'],
                        'lines': int(snippet_data['lines']),
                        'summary': snippet_data['summary'],
                        'dir1': dir1,
                        'dir2': dir2,
                        'dir3': dir3,
                        'dir4': dir4,
                        'ext': ext
                        }
                    first_commit = first_time (first_commit,
                                            int(snippet_data['committer-time']))
                    first_author = first_time (first_author,
                                            int(snippet_data['author-time']))
                    last_commit = last_time (last_commit,
                                            int(snippet_data['committer-time']))
                    last_author = last_time (last_author,
                                            int(snippet_data['author-time']))

                except KeyError:
                    error = {'error': 'KeyError', 'data': snippet_data}
                    logging.debug("Error: " + str(error))
                    errors.append(error)
                if identities is not None:
                    identities.add(name=snippet_data['author'],
                                    email=snippet_data['author-mail'])
                    identities.add(name=snippet_data['committer'],
                                    email=snippet_data['committer-mail'])
            else:
                data[hash]['lines'] += int(snippet_data['lines'])
            logging.info("Files / hashes done: %d / %d.", nfile, nhash)

        processed_files[file]['first_commit'] = first_commit
        processed_files[file]['last_commit'] = last_commit
        processed_files[file]['first_author'] = first_author
        processed_files[file]['last_author'] = last_author
        processed[file] = data

    logging.info("Process finished: (files present, files done, hashes done): %d, %d, %d.",
                files_done, nfile, nhash)
    store.sync()
    if len(errors) > 0:
        print("ERRORS:")
    for error in errors:
        print(str(error))

mapping_file_hash = {
    "properties" : {
        "author": {"type": "string",
                    "index": "not_analyzed"},
        "author_time": {"type": "date",
                    "format": "epoch_second"},
        "author_tz": {"type": "integer"},
        "committer": {"type": "string",
                    "index": "not_analyzed"},
        "committer_time": {"type": "date",
                    "format": "epoch_second"},
        "committer_tz": {"type": "integer"},
        "summary": {"type": "string",
                    "index": "not_analyzed"},
        "file": {"type": "string",
                    "index": "not_analyzed"},
        "hash": {"type": "string",
                    "index": "not_analyzed"},
        "dir1": {"type": "string",
                    "index": "not_analyzed"},
        "dir2": {"type": "string",
                    "index": "not_analyzed"},
        "dir3": {"type": "string",
                    "index": "not_analyzed"},
        "dir4": {"type": "string",
                    "index": "not_analyzed"},
        "ext": {"type": "string",
                    "index": "not_analyzed"}

    }
}

mapping_file = {
    "properties" : {
        "first_author": {"type": "date",
                    "format": "epoch_second"},
        "last_author": {"type": "date",
                    "format": "epoch_second"},
        "first_commit": {"type": "date",
                    "format": "epoch_second"},
        "last_commit": {"type": "date",
                    "format": "epoch_second"},
        "file": {"type": "string",
                    "index": "not_analyzed"},
        "dir1": {"type": "string",
                    "index": "not_analyzed"},
        "dir2": {"type": "string",
                    "index": "not_analyzed"},
        "dir3": {"type": "string",
                    "index": "not_analyzed"},
        "dir4": {"type": "string",
                    "index": "not_analyzed"},
        "ext": {"type": "string",
                    "index": "not_analyzed"}

    }
}

def remove_surrogates(s, method='replace'):
    return s.encode('utf-8', 'replace').decode('utf-8')

def is_surrogate_escaped(text):
    try:
        text.encode('utf-8')
    except UnicodeEncodeError as e:
        if e.reason == 'surrogates not allowed':
            return True
    return False

class BlameUpload():

    def __init__(self, processed, uploaded, es_index, es_type):

        self.processed = processed
        self.uploaded = uploaded
        self.es_index = es_index
        self.es_type = es_type

    def generator(self):

        items_uploaded = 0
        items_to_upload = 0
        for file in self.processed:
            for hash in self.processed[file]:
                item = self.processed[file][hash]
                id = item['hash'] + item['file'].replace('/','%2F')
                if (id in self.uploaded) and self.uploaded[id]:
                    items_uploaded += 1
                    continue
                items_to_upload += 1
                for key in ['author', 'committer', 'summary']:
                    if is_surrogate_escaped(item[key]):
                        logging.info("Removing surrogates. Text: %s, file: %s, field: %s.",
                                    item[key], file, key)
                        item[key] = remove_surrogates(item[key])
                action = {
                    '_index': self.es_index,
                    '_type': self.es_type,
                    '_id': id,
                    '_source': item
                }
                logging.debug("BlameUpload: Produced item to upload for %s.", id)
                yield action
        print('BlameUpload: Items uploaded earlier: ', str(items_uploaded),
                ", uploaded now: ", str(items_to_upload))

class BlameFilesUpload():

    def __init__(self, processed, uploaded, es_index, es_type):

        self.processed = processed
        self.uploaded = uploaded
        self.es_index = es_index
        self.es_type = es_type

    def generator(self):

        items_uploaded = 0
        items_to_upload = 0
        for file in self.processed:
            id = file.replace('/','%2F')
            if (id in self.uploaded) and self.uploaded[id]:
                items_uploaded += 1
                continue
            items_to_upload += 1
            item = self.processed[file]
            action = {
                '_index': self.es_index,
                '_type': self.es_type,
                '_id': id,
                '_source': item
            }
            logging.debug("BlameFilesUpload: Produced item to upload for %s.", id)
            yield action
        print('BlameFilesUpload: BlameFilesUpload: Items uploaded earlier: ',
                str(items_uploaded), ", uploaded now: ", str(items_to_upload))

def upload_raw(processed, uploaded, es_url, es_index, es_type, es_mapping,
    uploader_class):

    es = elasticsearch.Elasticsearch([es_url])

    print("Already uploaded items: ", len(uploaded.keys()))
    if len(uploaded.keys()) == 0:
        # No keys uploaded, we can delete the index and start from scratch
        try:
            es.indices.delete(es_index)
        except elasticsearch.exceptions.NotFoundError:
            # Index could not be deleted because it was not found. Ignore.
            logging.info("Could not delete index, it was not found: %s",
                    es_index)
        es.indices.create(es_index,
                        {"mappings": {es_type: es_mapping}})

    actions = uploader_class(processed=processed, uploaded=uploaded,
                es_index=es_index, es_type=es_type).generator()

    items_uploaded = 0
    items_failed = 0
    for result in elasticsearch.helpers.streaming_bulk(client=es, actions=actions, chunk_size=500):
        id = result[1]['index']['_id']
        if result[0] == True:
            uploaded[id] = True
            items_uploaded += 1
        else:
            uploaded[id] = False
            items_failed += 1
        logging.debug("Uploaded: %s (%s)", id, result[0])
    print("Items actually uploaded: ", items_uploaded, ", items failed: ", items_failed)

def close_shelves(shelves):

    for to_close in shelves:
        to_close.close()

if __name__ == "__main__":
    args = parse_args()
    if args.logging:
        log_format = '%(levelname)s:%(message)s'
        if args.logging == "info":
            level = logging.INFO
        elif args.logging == "debug":
            level = logging.DEBUG
        if args.logfile:
            logging.basicConfig(format=log_format, level=level,
                                filename = args.logfile, filemode = "w")
        else:
            logging.basicConfig(format=log_format, level=level)

    store = shelve.open(args.store)

    if (not args.assume_store) and (not args.assume_processed) :
        blame_analysis(repouri=args.repouri, repodir=args.repodir,
                        store=store)
    if args.store_only:
        close_shelves([store])
        exit()

    processed = shelve.open(args.processed)
    processed_files = shelve.open(args.processed + "_files")
    now = datetime.datetime.utcnow().timestamp()
    if not args.assume_processed:
        try:
            if args.sortinghat:
                identities = Identities(user=args.shuser, password=args.shpasswd,
                                    database=args.shdb, host=args.shhost)
            else:
                identities = None
            blame_process(store=store, processed=processed,
                        processed_files=processed_files,
                        identities=identities, now=now)
        except:
            close_shelves([store, processed, processed_files])
            raise

    close_shelves([store])
    if args.process_only:
        close_shelves([processed, processed_files])
        exit()

    uploaded = shelve.open(args.uploaded)
    uploaded_files = shelve.open(args.uploaded + "_files")
    try:
        upload_raw(processed=processed_files, uploaded=uploaded_files,
                    es_url=args.es_url, es_index=args.es_index + "_files",
                    es_mapping=mapping_file,
                    es_type='file',
                    uploader_class=BlameFilesUpload)
        upload_raw(processed=processed, uploaded=uploaded,
                    es_url=args.es_url, es_index=args.es_index,
                    es_mapping=mapping_file_hash,
                    es_type='file_hash',
                    uploader_class=BlameUpload)
    except:
        raise
    finally:
        close_shelves([processed, processed_files, uploaded, uploaded_files])
