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

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

import logging

from proton import Message, ProtonException
from proton.utils import SyncRequestResponse, BlockingConnection

LOG = logging.getLogger(__name__)


class AutoLinkConfig(object):
    """
    This class represents a configured autolink.
    """
    TYPE = u'org.apache.qpid.dispatch.router.config.autoLink'

    # the subset of autolink record fields that we need to set up an instance
    # of this class
    #
    ATTRIBUTES = [u'identity',
                  u'address',
                  u'direction',
                  u'phase']

    def __init__(self, address, identity, direction, phase):
        """
        @param address: the autolink address.
        @param identity: the id of the autolink config entry.
        @param direction: 'in' or 'out'
        @param phase: of the resulting queue address
        """
        self.address = address
        self.identity = identity
        self.direction = 'in' if direction.lower() == 'in' else 'out'
        self.q_address = "M%s%s" % (phase, address)


class MgmtClient(object):
    """
    Provides a synchronous API for management operations on a router.
    """
    def __init__(self, router_address, timeout, router_id=None, edge_id=None):
        """
        @param router_address: the network address of the router to manage.
        @param timeout: raise an error if a management operation blocks longer
        than timeout seconds.
        @param router_id: identity of remote router.  Use this if the router to
        be managed is not the router at router_address.  This can be used to
        access a remote router using a local router as a proxy.
        @param edge_id: like router_id except remote is an edge router.

        Note that router_id and edge_id are mutually exclusive
        """
        assert(not (edge_id and router_id))
        self._conn = BlockingConnection(router_address, timeout=timeout)
        if router_id:
            self._mgmt_address = u'_topo/0/%s/$management' % router_id
        elif edge_id:
            self._mgmt_address = u'_edge/%s/$management' % edge_id
        else:
            self._mgmt_address = u'$management'
        self._client = SyncRequestResponse(self._conn, self._mgmt_address)

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _request_msg(self, properties, body=None):
        """
        Create a management request message
        """
        req = Message()
        req.properties = properties
        req.body = body or {}
        return req

    def read(self, type, identity):
        """
        Return the entity of type 'type' with identity 'identity' as a map
        """
        request = self._request_msg(properties={u'operation': u'READ',
                                                u'type': type,
                                                u'identity': identity})
        try:
            response = self._client.call(request)
        except ProtonException:
            LOG.error("Read error - connection to router failed.")
            return None

        if response.properties.get(u'statusDescription') != 'OK':
            LOG.warning("Management read of type %s id %s failed: %s" % type,
                        identity, response.properties)
            return None

        return response.body

    def delete(self, type, identity):
        """
        Delete the configuration entry of type 'type' with identity 'identity'
        """
        request = self._request_msg(properties={u'operation': u'DELETE',
                                                u'type': type,
                                                u'identity': identity})
        try:
            response = self._client.call(request)
        except ProtonException:
            LOG.error("Delete error - connection to router failed.")
            return None

        status = response.properties.get(u'statusCode')
        if status != 204:   # delete failed
            LOG.warning("Management delete of type %s id %s failed: %s", type,
                        identity, response.properties)
            return status
        return None

    def query(self, type, attribute_names):
        """
        Query the router for all entities of type 'type'.  For each entity only
        return the values for the given attributes
        """

        class QueryIterator(object):
            """
            Helper object that provides an iterator over the results returned
            by the QUERY operation.

            Each iteration returns a single entity represented by a map of
            values keyed by the attribute name

            @param attribute_names: ordered list of attribute names returned by
            QUERY
            @param values: a list of ordered lists of values that correspond to
            a single entity.  The values are in the same order as attributes
            """
            def __init__(self, attribute_names, values):
                self._attribute_names = attribute_names
                self._values = values

            def __iter__(self):
                return self

            def __next__(self):
                try:
                    v = self._values.pop()
                except IndexError:
                    raise StopIteration
                return dict(zip(self._attribute_names, v))

        MAX_BATCH = 500  # limit per request message (see bug PROTON-1846)
        response_results = []
        response_attr_names = []
        offset = 0

        while True:
            request = self._request_msg(properties={u'operation': u'QUERY',
                                                    u'entityType': type,
                                                    u'offset': offset,
                                                    u'count': MAX_BATCH},
                                        body={u'attributeNames':
                                              attribute_names})
            try:
                response = self._client.call(request)
            except ProtonException:
                LOG.error("Query error - connection to router failed.")
                return None

            if response.properties.get(u'statusDescription') != 'OK':
                LOG.warning("Management query for type %s failed: %s" % type,
                            response.properties)
                return None

            if not response_attr_names:
                response_attr_names.extend(response.body[u'attributeNames'])

            response_results.extend(response.body[u'results'])

            if len(response.body[u'results']) < MAX_BATCH:
                break

            offset += MAX_BATCH

        return QueryIterator(response_attr_names, response_results)
