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
import logging

import six
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from impala import hiveserver2 as hs2
from impala._thrift_api import get_http_transport, get_socket, get_transport, ThriftClient, ImpalaHttpClient
from impala._thrift_gen.ImpalaService import ImpalaHiveServer2Service
from impala.error import NotSupportedError, HttpError
from impala.hiveserver2 import HS2Service, log
from impala.tests.util import ImpylaTestEnv

ENV = ImpylaTestEnv()

from impala.dbapi import connect


class FaultInjectingHttpClient(ImpalaHttpClient, object):
    """Class for injecting faults in the ImpalaHttpClient. Faults are injected by using the
    'enable_fault' method. The 'flush' method is overridden to check for injected faults
    and raise exceptions, if needed."""
    def __init__(self, *args, **kwargs):
        super(FaultInjectingHttpClient, self).__init__(*args, **kwargs)
        self.fault_code = None
        self.fault_message = None
        self.fault_enabled = False
        self.num_requests = 0
        self.fault_frequency = 0
        self.fault_enabled = False

    def enable_fault(self, http_code, http_message, fault_frequency, fault_body=None,
                     fault_headers=None):
        """Inject fault with given code and message at the given frequency.
        As an example, if frequency is 20% then inject fault for 1 out of every 5
        requests."""
        if fault_headers is None:
            fault_headers = {}
        self.fault_enabled = True
        self.fault_code = http_code
        self.fault_message = http_message
        self.fault_frequency = fault_frequency
        assert fault_frequency > 0 and fault_frequency <= 1
        self.num_requests = 0
        self.fault_body = fault_body
        self.fault_headers = fault_headers

    def disable_fault(self):
        self.fault_enabled = False

    def _check_code(self):
        if self.code >= 300:
            # Report any http response code that is not 1XX (informational response) or
            # 2XX (successful).
            raise HttpError(self.code, self.message, self.body, self.headers)

    def _inject_fault(self):
        if not self.fault_enabled:
            return False
        if self.fault_frequency == 1:
            return True
        if round(self.num_requests % (1 / self.fault_frequency)) == 1:
            return True
        return False

    def flush(self):
        ImpalaHttpClient.flush(self)
        self.num_requests += 1
        # Override code and message with the injected fault
        if self.fault_code is not None and self._inject_fault():
            self.code = self.fault_code
            self.message = self.fault_message
            self.body = self.fault_body
            self.headers = self.fault_headers
            self._check_code()


class TestHS2FaultInjection(object):
    """Class for testing the http fault injection in various rpcs used by the
    impala-shell client"""
    def setup(self):
        url = 'http://%s:%s/%s' % (ENV.host, ENV.http_port, "cliservice")
        self.transport = FaultInjectingHttpClient(url)

        # impalad = IMPALAD_HS2_HTTP_HOST_PORT.split(":")
        # self.custom_hs2_http_client = FaultInjectingImpalaHS2Client(impalad, 1024,
        #                                                             kerberos_host_fqdn=None, use_http_base_transport=True, http_path='cliservice')
        # self.transport = self.custom_hs2_http_client.transport

    def teardown(self):
        self.transport.disable_fault()
        # self.custom_hs2_http_client.close_connection()

    def connect(self):
        self.transport.open()
        protocol = TBinaryProtocol(self.transport)
        service = None
        if six.PY2:
            # ThriftClient == ImpalaHiveServer2Service.Client
            service = ThriftClient(protocol)
        elif six.PY3:
            # ThriftClient == TClient
            service = ThriftClient(ImpalaHiveServer2Service, protocol)
        service = HS2Service(service, retries=3)
        return hs2.HiveServer2Connection(service, default_db=None)
        # self.custom_hs2_http_client.connect()
        # assert self.custom_hs2_http_client.connected

    def __expect_msg_retry(self, impala_rpc_name):
        """Returns expected log message for rpcs which can be retried"""
        return ("Caught HttpError HTTP code 502: Injected Fault  in {0} (tries_left=3)".
                format(impala_rpc_name))

    def test_old_simple_connect(self): # FIXME remove
        con = connect("localhost", ENV.http_port, use_http_transport=True, http_path="cliservice")
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def test_new_simple_connect(self):
        con = self._connect("localhost", ENV.http_port)
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def test_class_connect_no_injection(self):
        con = self.connect()
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def test_connect(self, caplog):
        """Tests fault injection in ImpalaHS2Client's connect().
        OpenSession and CloseImpalaOperation rpcs fail.
        Retries results in a successful connection."""
        caplog.set_level(logging.DEBUG)
        self.transport.enable_fault(502, "Injected Fault", 0.20)
        con = self.connect()
        cur = con.cursor()

        print(caplog.text)
        for record in caplog.records:
            print(record)
        assert "Caught HttpError HTTP code 502: Injected Fault  in OpenSession (tries_left=3)" in caplog.text
        assert self.__expect_msg_retry("OpenSession") in caplog.text
        # output = capsys.readouterr()[1].splitlines()
        # assert output[1] == self.__expect_msg_retry("OpenSession")
        # assert output[2] == self.__expect_msg_retry("CloseImpalaOperation")

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
