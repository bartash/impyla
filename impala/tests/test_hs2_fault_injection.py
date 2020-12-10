# Copyright 2020 Cloudera Inc.
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
import six
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from impala import hiveserver2 as hs2
from impala._thrift_api import get_http_transport, get_socket, get_transport, ThriftClient, ImpalaHttpClient
from impala._thrift_gen.ImpalaService import ImpalaHiveServer2Service
from impala.error import NotSupportedError
from impala.hiveserver2 import HS2Service, log
from impala.tests.util import ImpylaTestEnv

ENV = ImpylaTestEnv()

from impala.dbapi import connect


class TestHS2FaultInjection(object):
    """uses real connect function"""
    def test_old_simple_connect(self): # FIXME remove
        con = connect("localhost", ENV.http_port, use_http_transport=True, http_path="cliservice")
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def test_simple_connect(self):
        con = self._connect("localhost", ENV.http_port)
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def _connect(self, host, port):
        url = 'http://%s:%s/%s' % (host, port, "cliservice")
        transport = ImpalaHttpClient(url)
        transport.open()
        protocol = TBinaryProtocol(transport)
        service = None
        if six.PY2:
            # ThriftClient == ImpalaHiveServer2Service.Client
            service = ThriftClient(protocol)
        elif six.PY3:
            # ThriftClient == TClient
            service = ThriftClient(ImpalaHiveServer2Service, protocol)
        service = HS2Service(service, retries=3)
        return hs2.HiveServer2Connection(service, default_db=None)
