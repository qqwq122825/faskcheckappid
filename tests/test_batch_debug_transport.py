"""Real SRP + verified loopback TLS exercises the debug HTTP adapters."""
import http.server
import json
from pathlib import Path
import plistlib
import queue
import select
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

import requests

from fastcheck.diagnostics import SafeDiagnostics
from fastcheck.proxy_pool import fetch_proxies
from fastcheck.protocols import gsa_status as gsa
from test_gsa_login import SyntheticServer


class DebugTransportTests(unittest.TestCase):
    def test_http_api_trace_records_actual_network_stages_without_url_or_body(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'127.0.0.1:12345'
                self.send_response(200)
                self.send_header('Content-Length',str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args): pass
        server = http.server.ThreadingHTTPServer(('127.0.0.1',0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        output = queue.Queue()
        try:
            diagnostic = SafeDiagnostics(output, role='proxy_api')
            values = fetch_proxies(f'http://127.0.0.1:{server.server_port}/?password=must-not-log',1,diagnostic)
            self.assertEqual(values, ['http://127.0.0.1:12345'])
        finally:
            server.shutdown();server.server_close();thread.join()
        records = []
        while not output.empty(): records.append(output.get()['_debug'])
        encoded = json.dumps(records)
        self.assertNotIn('must-not-log', encoded)
        self.assertNotIn('127.0.0.1:12345', encoded)
        phases = {row.get('phase') for row in records}
        self.assertTrue({'target_dns','tcp_connect','http_send','response_headers','proxy_api_body'} <= phases)

    def test_verified_tls_debug_mode_completes_real_srp_and_keeps_proofs_private(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            certificate, key = root/'cert.pem', root/'key.pem'
            config = root/'openssl.cnf'
            config.write_text('[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n'
                              '[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost\n'
                              'basicConstraints=critical,CA:TRUE\nkeyUsage=digitalSignature,keyEncipherment,keyCertSign\n')
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                            '-keyout',str(key),'-out',str(certificate),'-config',str(config)],
                           check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            synthetic = SyntheticServer(auth='trustedDeviceSecondaryAuth')
            class Handler(http.server.BaseHTTPRequestHandler):
                protocol_version = 'HTTP/1.1'
                def do_POST(self):
                    body = self.rfile.read(int(self.headers['Content-Length']))
                    payload = synthetic.post(body, {}, 5).payload
                    response = plistlib.dumps(payload)
                    self.send_response(200)
                    self.send_header('Content-Type','text/x-xml-plist')
                    self.send_header('Content-Length',str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                def log_message(self, *args): pass
            server = http.server.ThreadingHTTPServer(('127.0.0.1',0), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certificate,key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever,daemon=True)
            thread.start()
            class ProxyHandler(http.server.BaseHTTPRequestHandler):
                def do_CONNECT(self):
                    host, port = self.path.rsplit(':',1)
                    if host != 'localhost' or int(port) != server.server_port:
                        self.send_error(403)
                        return
                    upstream = socket.create_connection(('127.0.0.1',int(port)),timeout=5)
                    proxy.connects += 1
                    try:
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.flush()
                        sockets = (self.connection, upstream)
                        while True:
                            readable, _, _ = select.select(sockets,[],[],5)
                            if not readable:
                                return
                            for connection in readable:
                                data = connection.recv(16384)
                                if not data:
                                    return
                                (upstream if connection is self.connection else self.connection).sendall(data)
                    finally:
                        upstream.close()
                def log_message(self, *args): pass
            proxy = http.server.ThreadingHTTPServer(('127.0.0.1',0),ProxyHandler)
            proxy.connects = 0
            proxy_thread = threading.Thread(target=proxy.serve_forever,daemon=True)
            proxy_thread.start()
            output = queue.Queue()
            diagnostic = SafeDiagnostics(output, role='authentication', row=1, device=1)
            identity = {'X-Apple-I-MD':'private-otp','X-Apple-I-MD-M':'private-machine',
                        'X-Apple-I-MD-LU':'private-user','X-Apple-I-MD-RINFO':'17106176',
                        'X-Mme-Device-Id':'private-device','X-Mme-Client-Info':'synthetic-client',
                        'User-Agent':'synthetic-agent'}
            try:
                with patch.object(gsa,'GSA_URL',f'https://localhost:{server.server_port}/gsa'), \
                     patch.object(gsa,'gsa_ca_file',return_value=str(certificate)), \
                     patch.dict('os.environ',{'FASTCHECK_MOCK':''}):
                    gsa.close_session()
                    result = gsa.check('owned@example.invalid','synthetic-password',f'http://127.0.0.1:{proxy.server_port}',
                                       timeout=5,retries=0,identity=identity,diagnostics=diagnostic)
                self.assertEqual(result['status'],'need_2fa')
                self.assertTrue(result['password_verified'])
                self.assertGreater(proxy.connects,0)
            finally:
                gsa.close_session()
                proxy.shutdown();proxy.server_close();proxy_thread.join()
                server.shutdown();server.server_close();thread.join()
            records = []
            while not output.empty(): records.append(output.get()['_debug'])
            phases = {row.get('phase') for row in records}
            self.assertTrue({'tls_handshake','http_init','http_complete','srp_challenge'} <= phases)
            self.assertTrue({'proxy_dns','proxy_tcp_connect'} <= phases)
            connections = [row for row in records if row.get('event')=='connection']
            self.assertTrue(all(row['target_dns_mode']=='remote' for row in connections))
            body = json.dumps(records)
            for secret in ('owned@example.invalid','synthetic-password','private-otp','private-machine'):
                self.assertNotIn(secret,body)
