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

from impala import hiveserver2 as hs2
from impala.error import NotSupportedError
from impala.tests.util import ImpylaTestEnv
from impala.util import warn_deprecate, warn_protocol_param

ENV = ImpylaTestEnv()

from impala.dbapi import connect, AUTH_MECHANISMS


class TestHS2FaultInjection(object):
    """uses real connect function"""
    def test_old_simple_connect(self): # FIXME remove
        con = connect("localhost", ENV.http_port, use_http_transport=True, http_path="cliservice")
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def test_simple_connect(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute('select 1')
        rows = cur.fetchall()
        assert rows == [(1,)]

    def _connect(self):
        names = ['impala.auth', 'hive.server2.auth']
        # pylint: disable=too-many-locals

        # if None is not None:
        #     warn_deprecate('use_kerberos', 'auth_mechanism="GSSAPI"')
        #     if None:
        #         auth_mechanism = 'GSSAPI'
        # if None is not None:
        #     warn_deprecate('use_ldap', 'auth_mechanism="LDAP"')
        #     if None:
        #         auth_mechanism = 'LDAP'
        # if 'NOSASL':
        #     auth_mechanism = 'NOSASL'.upper()
        # else:
        #     auth_mechanism = 'NOSASL'
        # if 'NOSASL' not in AUTH_MECHANISMS:
        #     raise NotSupportedError(
        #         'Unsupported authentication mechanism: {0}'.format('NOSASL'))
        # if None is not None:
        #     warn_deprecate('ldap_user', 'user')
        #     user = None
        # if None is not None:
        #     warn_deprecate('ldap_password', 'password')
        #     password = None
        # if None is not None:
        #     if None.lower() == 'hiveserver2':
        #         warn_protocol_param()
        #     else:
        #         raise NotSupportedError(
        #             "'{0}' is not a supported protocol; only HiveServer2 is "
        #             "supported".format(None))
        service = hs2.connect(host="localhost", port=ENV.http_port,
                              timeout=None, use_ssl=False,
                              ca_cert=None, user=None, password=None,
                              kerberos_service_name='impala',
                              auth_mechanism='NOSASL', krb_host=None,
                              use_http_transport=True,
                              http_path="cliservice",
                              auth_cookie_names=names,
                              retries=3)
        return hs2.HiveServer2Connection(service, default_db=None)
