#!/usr/bin/env python
#
#    Copyright (C) 2020 Kenneth A. Giusti
#
#    Licensed to the Apache Software Foundation (ASF) under one
#    or more contributor license agreements.  See the NOTICE file
#    distributed with this work for additional information
#    regarding copyright ownership.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

import argparse
import logging
import sys
import time

from proton import ProtonException
from qdials import LOG, AutoLinkConfig, MgmtClient

__version__ = (0, 0, 0)
ADDRESS_TYPE = u'org.apache.qpid.dispatch.router.address'


def _main():
    parser = argparse.ArgumentParser(
        description=('Qpid Dispatch Idle Auto-Link Scrubber (v%d.%d.%d)'
                     % __version__))

    parser.add_argument("--bus",
                        default='localhost:5672',
                        help="The address of the Qpid Dispatch Router"
                        " (qdrouterd)")
    parser.add_argument('--poll',
                        type=int,
                        default=60,
                        help='Polling interval in seconds')
    parser.add_argument('--debug', action='store_true',
                        help='Enable DEBUG logging')
    parser.add_argument("--timeout", type=int, default=30,
                        help='fail if router unresponsive for timeout seconds')
    parser.add_argument("--router", type=str, default=None,
                        help='Remote router id [Advanced]')
    parser.add_argument("--edge", type=str, default=None,
                        help='Remote edge router id [Advanced]')
    parser.add_argument("--remove-outlinks", action='store_true',
                        help="Delete any corresponding 'out' autoLinks that"
                        " share the same address as the stale 'in' autoLink.")


    args = parser.parse_args()

    LOG.setLevel(logging.DEBUG if args.debug else logging.WARNING)
    hndlr = logging.StreamHandler()
    hndlr.setLevel(logging.DEBUG if args.debug else logging.WARNING)
    LOG.addHandler(hndlr)

    eligible = set()   # (record-identity, address)

    try:

        while True:

            time.sleep(args.poll)

            LOG.debug("qdials: begin poll of router %s", args.bus)

            try:
                client = MgmtClient(args.bus, timeout=args.timeout,
                                    router_id=args.router, edge_id=args.edge)
            except ProtonException:
                LOG.error("Connection to router %s failed.  Retrying..",
                          args.bus)
                eligible = set()
                continue

            # query router for all autolink records

            results = client.query(type=AutoLinkConfig.TYPE,
                                   attribute_names=AutoLinkConfig.ATTRIBUTES)
            if results is None:
                LOG.error("Query for all autoLink records failed")
                client.close()
                eligible = set()
                continue

            # build a set of autoLinks that are candidates for removal.
            # Also track out autoLinks for removal if '--remove-outlinks'
            # enabled

            candidate = set()  # (record-identity, address)
            out_links = {}     # map of address: record-identity

            # walk through the query results and extract the autoLink entries
            # with direction == 'in'

            for config in results:
                autolink = AutoLinkConfig(**config)
                if autolink.direction == 'out':
                    out_links[autolink.address] = autolink.identity;
                    continue  # done processing 'out' autoLink

                # Query the autolink's queue address to get the current
                # number of subscribers

                addr_map = client.read(ADDRESS_TYPE,
                                       identity=autolink.q_address)
                if addr_map is None:
                    LOG.debug("No active address for AutoLink address %s (%s)",
                              autolink.address, autolink.q_address)
                    continue

                subscribers = addr_map[u'subscriberCount']
                subscribers += addr_map['remoteCount']
                if subscribers == 0:
                    LOG.info("Autolink %s has no subscribers - monitoring it"
                             " for deletion", autolink.address)
                    candidate.add((autolink.identity, autolink.address))

            # Compare current candidates with candicates from last pass

            to_remove = candidate & eligible  # intersection
            LOG.info("Found %d autoLinks ready for deletion",
                     len(to_remove))

            for identity, address in to_remove:
                LOG.info("Deleting inbound autoLink address=%s record id=%s",
                         address, identity)
                client.delete(type=AutoLinkConfig.TYPE, identity=identity)

                # TODO(kgiusti): generate a signal to an external management
                # agent to dead letter any outstanding messages

                if args.remove_outlinks:
                    identity = out_links.get(address)
                    if identity is not None:
                        LOG.info("Deleting corresponding out autoLink"
                                 " address=%s record id=%s", address, identity)
                        client.delete(type=AutoLinkConfig.TYPE, identity=identity)

            client.close()
            eligible = candidate  # save for next poll

    except KeyboardInterrupt:
        pass

    return None


sys.exit(_main())
