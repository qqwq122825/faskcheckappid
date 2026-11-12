"""Session-local requests/urllib3 tracing. No process-wide monkey patches.

Private urllib3 TLS helpers are rebound with private global dictionaries so their
existing certificate/hostname verification remains intact. The only substituted
operations are timing boundaries; ordinary requests sessions are untouched.
"""
from contextlib import contextmanager
import http.client
import socket
import time
from types import FunctionType

from urllib3 import connection as connection_module
from urllib3.util import connection as connection_util
from urllib3.util import ssl_ as ssl_util

from .timing import emit, phase
from .tls import VerifiedTLSAdapter


def _rebind(function, **overrides):
    namespace = dict(function.__globals__)
    for name in overrides:
        if name not in namespace:
            raise RuntimeError("Unsupported urllib3 timing hook")
    namespace.update(overrides)
    bound = FunctionType(function.__code__, namespace, function.__name__,
                         function.__defaults__, function.__closure__)
    bound.__kwdefaults__ = function.__kwdefaults__
    return bound


class _Stages:
    """Adjacent spans, including errors in the currently executing span."""
    def __init__(self, callback):
        self.callback, self.current = callback, None

    def switch(self, name):
        if self.current is not None:
            self.current.__exit__(None, None, None)
        self.current = phase(self.callback, name)
        self.current.__enter__()

    def __enter__(self):
        return self

    def __exit__(self, *error):
        if self.current is not None:
            return self.current.__exit__(*error)


class NetworkTrace:
    def __init__(self, callback, resolver=None):
        self.callback = callback
        self.resolver = resolver
        self.request_id = self.connection_id = 0
        self._connection_serial = 0
        self.request_phase = "http_init"
        self.seen_connection = False
        self.traffic_enabled = False
        self.count_sends = False

    def record(self, event):
        emit(self.callback, dict(event, request_id=self.request_id,
             connection_id=self.connection_id, request_phase=self.request_phase))

    @contextmanager
    def request(self, name):
        self.request_id += 1
        self.request_phase = name
        self.seen_connection = False
        self.traffic_enabled = bool(getattr(self.callback, "traffic_enabled", False))
        self.sent_http_bytes = self.received_headers = 0
        self.response = None
        self.send_complete = True
        self.request_sent = False
        started = time.perf_counter()
        error = None
        try:
            yield
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            if self.traffic_enabled:
                response = self.response
                body_bytes = None
                try:
                    body_bytes = response.raw.tell() if response is not None else 0
                except Exception:
                    pass
                event = {"event": "http_request", "ms": round((time.perf_counter() - started) * 1000, 3),
                         "sent_http_bytes": self.sent_http_bytes,
                         "received_http_header_bytes": self.received_headers,
                         "send_complete": self.send_complete and self.request_sent,
                         "response_complete": bool(response is not None and getattr(response, "_content_consumed", False))}
                if type(body_bytes) is int:
                    event.update(received_body_bytes=body_bytes,
                                 received_http_bytes=self.received_headers + body_bytes)
                if response is not None:
                    event["http"] = response.status_code
                if error:
                    event["error_type"] = error
                self.record(event)

    def connection(self, connection, reused=False):
        if not reused:
            self._connection_serial += 1
            self.connection_id = self._connection_serial
            connection._timing_connection_id = self.connection_id
        else:
            self.connection_id = connection._timing_connection_id
        self.seen_connection = True
        options = getattr(connection, "_socks_options", {})
        self.record({"event": "connection", "reused": reused,
                     "target_dns_mode": "remote" if options.get("rdns") or getattr(connection,"proxy",None) is not None else "local"})


class _SocketModule:
    def __init__(self, callback, dns_phase, socket_factory=None, resolver=None):
        self.callback, self.dns_phase = callback, dns_phase
        self.socket = socket_factory or socket.socket
        self.resolver = resolver

    def __getattr__(self, name):
        return getattr(socket, name)

    def getaddrinfo(self, *args, **kwargs):
        with phase(self.callback, self.dns_phase):
            return (self.resolver or socket.getaddrinfo)(*args, **kwargs)


def _tls_wrapper(trace):
    def wrap(*args, **kwargs):
        with _Stages(trace.record) as stages:
            stages.switch("tls_setup")

            def handshake(*args, **kwargs):
                stages.switch("tls_handshake")
                return ssl_util._ssl_wrap_socket_impl(*args, **kwargs)

            ssl_wrap = _rebind(ssl_util.ssl_wrap_socket, _ssl_wrap_socket_impl=handshake)
            verify = _rebind(connection_module._ssl_wrap_socket_and_match_hostname,
                             ssl_wrap_socket=ssl_wrap)
            return verify(*args, **kwargs)
    return wrap


def _socks_connect(trace, destination, timeout, source_address, socket_options, options):
    # Import only for SOCKS sessions, matching requests' optional dependency.
    import socks

    destination = (destination[0].strip("[]"), destination[1])

    class TimedSocksSocket(socks.socksocket):
        def connect(self, dest_pair, catch_errors=None):
            # PySocks' base connect invokes socket.connect via super(), so the
            # TCP boundary must be here, before invoking its negotiator.
            try:
                with phase(trace.record, "proxy_tcp_connect"):
                    socket.socket.connect(self, self._timing_proxy_address)
            except OSError as exc:
                self.close()
                raise socks.ProxyConnectionError("Proxy TCP connection failed", exc) from exc
            try:
                negotiator = self._proxy_negotiators[self.proxy[0]]
                if self.proxy[0] == socks.SOCKS5:
                    negotiator(self, *dest_pair)
                else:
                    with phase(trace.record, "socks4_connect"):
                        negotiator(self, *dest_pair)
            except OSError as exc:
                self.close()
                raise socks.GeneralProxyError("Socket error", exc) from exc
            except socks.ProxyError:
                self.close()
                raise

        def _SOCKS5_request(self, *args):
            with _Stages(trace.record) as stages:
                self._timing_stages = stages
                stages.switch("socks_auth")
                return super()._SOCKS5_request(*args)

        def _write_SOCKS5_address(self, *args):
            # Authentication has completed. The CONNECT request is still in
            # PySocks' buffered writer and is flushed after this method returns.
            self._timing_stages.switch("proxy_target_connect")
            write_address = _rebind(socks.socksocket._write_SOCKS5_address,
                socket=_SocketModule(trace.record, "target_dns"))
            return write_address(self, *args)

    host = options["proxy_host"].strip("[]")
    with phase(trace.record, "proxy_dns"):
        addresses = socket.getaddrinfo(host, options["proxy_port"], 0, socket.SOCK_STREAM)
    error = None
    for family, kind, protocol, _, address in addresses:
        sock = None
        try:
            sock = TimedSocksSocket(family, kind, protocol)
            sock._timing_proxy_address = address
            for option in socket_options or ():
                sock.setsockopt(*option)
            if isinstance(timeout, (int, float)):
                sock.settimeout(timeout)
            sock.set_proxy(options["socks_version"], host, options["proxy_port"],
                           options["rdns"], options["username"], options["password"])
            if source_address:
                sock.bind(source_address)
            sock.connect(destination)
            return sock
        except (OSError, socks.ProxyError) as exc:
            error = exc
            if sock is not None:
                sock.close()
    if error is not None:
        raise error
    raise OSError("getaddrinfo returns an empty list")


def _connection_classes(trace, socks_proxy=False):
    if socks_proxy:
        from urllib3.contrib.socks import SOCKSConnection, SOCKSHTTPSConnection
        http_base, https_base = SOCKSConnection, SOCKSHTTPSConnection
    else:
        http_base, https_base = connection_module.HTTPConnection, connection_module.HTTPSConnection

    class TimedResponse(http.client.HTTPResponse):
        def begin(self):
            if self.headers is not None:
                return
            with phase(trace.record, "response_first_byte"):
                if not self.fp.peek(1):
                    raise http.client.RemoteDisconnected("Remote end closed connection without response")
            with phase(trace.record, "response_headers"):
                if not trace.traffic_enabled:
                    return super().begin()
                original = self.fp

                class HeaderReader:
                    def __getattr__(self, name):
                        return getattr(original, name)

                    def readline(self, *args, **kwargs):
                        line = original.readline(*args, **kwargs)
                        trace.received_headers += len(line)
                        return line

                self.fp = HeaderReader()
                try:
                    return super().begin()
                finally:
                    # No headers or body contents are retained by the counter.
                    self.fp = original

    class TimedConnection:
        response_class = TimedResponse

        def _new_conn(self):
            trace.connection(self)
            if socks_proxy:
                from urllib3.contrib.socks import SOCKSConnection

                class SocksModule:
                    def __getattr__(self, name):
                        import socks
                        return getattr(socks, name)

                    def create_connection(self_module, destination, **kwargs):
                        return _socks_connect(trace, destination, self.timeout,
                            self.source_address, self.socket_options, self._socks_options)

                # Retain urllib3's original timeout/ProxyError translation.
                return _rebind(SOCKSConnection._new_conn, socks=SocksModule())(self)

            proxy = self.proxy is not None
            tcp_phase = "proxy_tcp_connect" if proxy else "tcp_connect"

            class TimedSocket(socket.socket):
                def connect(self, address):
                    with phase(trace.record, tcp_phase):
                        return super().connect(address)

            create = _rebind(connection_util.create_connection,
                socket=_SocketModule(trace.record, "proxy_dns" if proxy else "target_dns", TimedSocket,
                                     trace.resolver if not proxy else None))

            class ConnectionModule:
                create_connection = staticmethod(create)

            return _rebind(connection_module.HTTPConnection._new_conn, connection=ConnectionModule)(self)

        def request(self, *args, **kwargs):
            if self.sock is None:
                self.connect()
            if not trace.seen_connection:
                trace.connection(self, reused=True)
            with phase(trace.record, "http_send"):
                trace.count_sends = True
                try:
                    result = super().request(*args, **kwargs)
                    trace.request_sent = True
                    return result
                finally:
                    trace.count_sends = False

        def send(self, data):
            try:
                result = super().send(data)
            except BaseException:
                if trace.traffic_enabled and trace.count_sends:
                    trace.send_complete = False
                raise
            if trace.traffic_enabled and trace.count_sends:
                if isinstance(data, (bytes, bytearray, memoryview)):
                    trace.sent_http_bytes += len(data)
                elif isinstance(data, str):
                    trace.sent_http_bytes += len(data.encode("iso-8859-1"))
                else:
                    trace.send_complete = False
            return result

        def _tunnel(self):
            with phase(trace.record, "http_proxy_tunnel"):
                return super()._tunnel()

    class TimedHTTPConnection(TimedConnection, http_base):
        pass

    class TimedHTTPSConnection(TimedConnection, https_base):
        connect = _rebind(connection_module.HTTPSConnection.connect,
                          _ssl_wrap_socket_and_match_hostname=_tls_wrapper(trace))
        _connect_tls_proxy = _rebind(connection_module.HTTPSConnection._connect_tls_proxy,
                          _ssl_wrap_socket_and_match_hostname=_tls_wrapper(trace))

    return TimedHTTPConnection, TimedHTTPSConnection


def _instrument_manager(manager, trace, socks_proxy=False):
    http, https = _connection_classes(trace, socks_proxy)
    pools = dict(manager.pool_classes_by_scheme)
    for scheme, connection in (("http", http), ("https", https)):
        pools[scheme] = type("Timed" + scheme.upper() + "Pool", (pools[scheme],), {"ConnectionCls": connection})
    manager.pool_classes_by_scheme = pools


class TimingAdapter(VerifiedTLSAdapter):
    def __init__(self, trace, **kwargs):
        self.trace = trace
        super().__init__(diagnostics=trace.record, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        _instrument_manager(self.poolmanager, self.trace)

    def send(self, *args, **kwargs):
        response = super().send(*args, **kwargs)
        if self.trace.traffic_enabled:
            self.trace.response = response
        return response

    def proxy_manager_for(self, proxy, **kwargs):
        existing = proxy in self.proxy_manager
        manager = super().proxy_manager_for(proxy, **kwargs)
        if not existing:
            _instrument_manager(manager, self.trace, proxy.lower().startswith("socks"))
        return manager
