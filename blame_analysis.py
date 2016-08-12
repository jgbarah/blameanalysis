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

def blame_process(store, processed, now):
    """Process git blame raw data.

    Reads raw data in store, to produce data in processed, better
    organized for further analysis.

    processed could be partially filled in, in which case old data
    will be preserved, acting as a kind of cache.

    :param store:     shelve file with git blame raw data
    :param processed: shelve file with processed data
    :param now:       timestamp considered as "now"

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
                        'committer_tz': snippet_data['committer-tz'],
                        'author_tz': snippet_data['author-tz'],
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
                except KeyError:
                    error = {'error': 'KeyError', 'data': snippet_data}
                    logging.debug("Error: " + str(error))
                    errors.append(error)

            else:
                data[hash]['lines'] += int(snippet_data['lines'])
            logging.info("Files / hashes done: %d / %d.", nfile, nhash)

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
        "committer": {"type": "string",
                    "index": "not_analyzed"},
        "committer_time": {"type": "date",
                    "format": "epoch_second"},
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
            for hash in processed[file]:
                item = processed[file][hash]
                id = item['hash'] + item['file'].replace('/','%2F')
                if (id in uploaded) and uploaded[id]:
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
                logging.debug("Produced item for %s, %s.", file, hash)
                yield action
        print('Items already uploaded: ', str(items_uploaded), ", to upload: ", str(items_to_upload))

def blame_upload_raw(processed, uploaded, es_url, es_index):

    es = elasticsearch.Elasticsearch([es_url])
    es_type = 'file_hash'

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
                        {"mappings": {es_type: mapping_file_hash}})

    actions = BlameUpload(processed=processed, uploaded=uploaded,
                es_index=es_index, es_type=es_type).generator()

#    result = elasticsearch.helpers.bulk(client=es, actions=actions)
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

def blame_upload(processed, es_url, es_index):
    """Upload data to ElasticSearch.

    """

    es = elasticsearch.Elasticsearch([es_url])
    es_type = 'file_hash'

    try:
        es.indices.delete(es_index)
    except elasticsearch.exceptions.NotFoundError:
        # Index could not be deleted because it was not found. Ignore.
        logging.info("Could not delete index, it was not found: " \
                    + es_index)
    es.indices.create(es_index,
                    {"mappings": {es_type: mapping_file_hash}})

    for file in processed:
        for hash in processed[file]:
            item = processed[file][hash]
            logging.info("Uploading %s, %s", file, hash)
            # Check surrogate escaping, and remove it if needed
            try:
                res = es.index(index = es_index, doc_type = es_type,
                                id = item['hash']+item['file'].replace('/','%2F'),
                                body = item)
            except UnicodeEncodeError as e:
                if e.reason == 'surrogates not allowed':
                    logging.debug ("Surrogate found in: " + str(item))
                    item = item.encode('utf-8', "backslashreplace").decode('utf-8')
                    res = es.index(index = es_index, doc_type = es_type,
                                    id = item['hash']+item['file'], body = item)
                else:
                    raise
            logging.debug("Result: " + str(res))

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
        store.close()
        exit()

    processed = shelve.open(args.processed)
    now = datetime.datetime.utcnow().timestamp()
    if not args.assume_processed:
        try:
            blame_process(store=store, processed=processed, now=now)
        except:
            store.close()
            processed.close()
            raise

    if args.process_only:
        store.close()
        processed.close()
        exit()

    uploaded = shelve.open(args.uploaded)
    try:
        blame_upload_raw(processed=processed, uploaded=uploaded,
                        es_url=args.es_url, es_index=args.es_index)
    except:
        store.close()
        processed.close()
        uploaded.close()
        raise

    store.close()
    processed.close()
    uploaded.close()
