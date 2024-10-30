# Copyright 2019 Cloudera Inc.
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
import socket
import threading
from contextlib import closing

import pytest
import requests
import six

if six.PY2:
  import httplib
from six.moves import SimpleHTTPServer
from six.moves import http_client
from six.moves import socketserver

from impala.error import HttpError
from impala.tests.util import ImpylaTestEnv

ENV = ImpylaTestEnv()

@pytest.fixture
def http_503_server():
  class RequestHandler503(SimpleHTTPServer.SimpleHTTPRequestHandler):
    """A custom http handler that checks for duplicate 'Host' headers from the most
    recent http request, and always returns a 503 http code"""

    def do_POST(self):
      # Ensure that only one 'Host' header is contained in the request before responding.
      request_headers = None
      host_hdr_count = 0
      if six.PY2:
        # The unfortunately named self.headers here is an instance of mimetools.Message that
        # contains the request headers.
        request_headers = self.headers.headers
        host_hdr_count = sum([header.startswith('Host:') for header in request_headers])
      if six.PY3:
        # In Python3 self.Headers is an HTTPMessage.
        request_headers = self.headers
        host_hdr_count = sum([header[0] == 'Host' for header in request_headers.items()])
      assert host_hdr_count == 1, "need single 'Host:' header in %s" % request_headers

      # Respond with 503.
      self.send_response(code=http_client.SERVICE_UNAVAILABLE, message="Service Unavailable")
      self.end_headers()
      self.wfile.write("extra text".encode('utf-8'))

  class TestHTTPServer503(object):
    def __init__(self):
      self.HOST = "localhost"
      self.PORT = get_unused_port()
      self.httpd = socketserver.TCPServer((self.HOST, self.PORT), RequestHandler503)

      self.http_server_thread = threading.Thread(target=self.httpd.serve_forever)
      self.http_server_thread.start()

  server = TestHTTPServer503()
  yield server

  # Cleanup after test.
  shutdown_server(server)


@pytest.yield_fixture
def http_proxy_server():
  """A fixture that creates an http proxy."""
  server = TestHTTPServerProxy(RequestHandlerProxy)
  yield server

  # Cleanup after test.
  shutdown_server(server)

class RequestHandlerProxy(SimpleHTTPServer.SimpleHTTPRequestHandler):
  """A custom http handler acts as an http proxy."""

  def __init__(self, request, client_address, server):
    http.server.SimpleHTTPRequestHandler.__init__(self, request, client_address,
                                                  server)

  def do_POST(self):

    data_string = self.rfile.read(int(self.headers['Content-Length']))

    response = requests.post(url="http://localhost:28000/cliservice",
                             headers=self.headers, data=data_string)
    self.send_response(code=response.status_code)
    # FIXME need python 3 version here
    for key, value in response.headers.iteritems():
      self.send_header(keyword=key, value=value)
    self.end_headers()
    self.wfile.write(response.content)
    self.wfile.close()

class TestHTTPServerProxy(object):
  def __init__(self, clazz):
    self.HOST = "localhost"
    self.PORT = get_unused_port()
    self.httpd = socketserver.TCPServer((self.HOST, self.PORT), clazz)

    self.http_server_thread = threading.Thread(target=self.httpd.serve_forever)
    self.http_server_thread.start()

from impala.dbapi import connect

class TestHttpConnect(object):
  def test_simple_connect(self):
    con = connect("localhost", ENV.http_port, use_http_transport=True, http_path="cliservice")
    cur = con.cursor()
    cur.execute('select 1')
    rows = cur.fetchall()
    assert rows == [(1,)]

  def test_http_interactions(self, http_503_server):
    """Test interactions with the http server when using hs2-http protocol.
    Check that there is an HttpError exception when the server returns a 503 error."""
    con = connect("localhost", http_503_server.PORT, use_http_transport=True)
    try:
      con.cursor()
      assert False, "Should have got exception"
    except HttpError as e:
      assert str(e) == "HTTP code 503: Service Unavailable"
      assert e.code == http_client.SERVICE_UNAVAILABLE
      assert e.body.decode("utf-8") == "extra text"


def get_unused_port():
  """ Find an unused port http://stackoverflow.com/questions/1365265 """
  with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
    s.bind(('', 0))
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return s.getsockname()[1]

def shutdown_server(server):
  """Helper method to shutdown a http server."""
  if server.httpd is not None:
    server.httpd.shutdown()
  if server.http_server_thread is not None:
    server.http_server_thread.join()
