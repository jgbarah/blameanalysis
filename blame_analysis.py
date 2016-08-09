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
#     Santiago Dueñas <sduenas@bitergia.com>
#

import argparse
import logging
import json
import shelve
from perceval.backends import GitBlame

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
                        help = "File for storing the data produced")
    parser.add_argument("repouri", type=str,
                        help = "URI for the repository")
    args = parser.parse_args()
    return args

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
    git_blame = GitBlame(uri=args.repouri, gitpath=args.repodir)

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
        store.close()
    except OSError as e:
        store.close()
        raise RuntimeError(str(e))
    except Exception as e:
        store.close()
        raise RuntimeError(str(e))
