#!/usr/bin/env python3

"""
Minimal MQTT v3.1.1 QoS0 publisher implemented with stdlib only.

We intentionally avoid third-party dependencies (e.g. paho-mqtt) so that this can
run in GitHub Actions without pip installs.
"""

from __future__ import annotations

import os
import socket
import struct
import time
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple


class MqttError(RuntimeError):
    pass


@dataclass(frozen=True)
class TcpBroker:
    host: str
    port: int


@dataclass(frozen=True)
class PublishResult:
    sent: int
    start_ts_ms: int
    end_ts_ms: int


def parse_tcp_broker_url(broker_url: str) -> TcpBroker:
    broker_url = broker_url.strip()
    if not broker_url.startswith("tcp://"):
        raise MqttError(f"unsupported broker_url scheme: {broker_url}")
    rest = broker_url[len("tcp://") :]
    if ":" not in rest:
        raise MqttError(f"broker_url missing port: {broker_url}")
    host, port_str = rest.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError as e:
        raise MqttError(f"invalid broker_url port: {broker_url}") from e
    if port <= 0 or port > 65535:
        raise MqttError(f"invalid broker_url port: {broker_url}")
    return TcpBroker(host=host, port=port)


def _encode_remaining_length(n: int) -> bytes:
    # MQTT "Remaining Length" variable encoding.
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n > 0:
            digit |= 0x80
        out.append(digit)
        if n == 0:
            break
    return bytes(out)


def _encode_utf8(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("!H", len(b)) + b


def _build_connect_packet(client_id: str, keepalive_secs: int = 60) -> bytes:
    # MQTT v3.1.1 CONNECT packet.
    proto_name = _encode_utf8("MQTT")
    proto_level = b"\x04"
    connect_flags = b"\x02"  # clean session
    keepalive = struct.pack("!H", keepalive_secs)
    payload = _encode_utf8(client_id)
    vh = proto_name + proto_level + connect_flags + keepalive
    remaining = _encode_remaining_length(len(vh) + len(payload))
    return b"\x10" + remaining + vh + payload


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise MqttError("socket closed while reading")
        buf.extend(chunk)
    return bytes(buf)


def _read_connack(sock: socket.socket) -> None:
    # CONNACK: 0x20 0x02 <ack_flags> <return_code>
    fixed = _read_exact(sock, 2)
    if fixed[0] != 0x20 or fixed[1] != 0x02:
        raise MqttError(f"unexpected CONNACK header: {fixed!r}")
    payload = _read_exact(sock, 2)
    rc = payload[1]
    if rc != 0:
        raise MqttError(f"CONNACK error return_code={rc}")


def _build_publish_packet(topic: str, payload: bytes) -> bytes:
    # QoS0, retain=false -> fixed header 0x30.
    vh = _encode_utf8(topic)
    remaining = _encode_remaining_length(len(vh) + len(payload))
    return b"\x30" + remaining + vh + payload


def publish_qos0(
    broker_url: str,
    topic: str,
    payloads: Iterable[bytes],
    publish_count: int = 0,
    duration_secs: int = 0,
    rate_per_sec: int = 0,
    client_id: Optional[str] = None,
    connect_timeout_secs: float = 10.0,
    keepalive_secs: int = 60,
) -> int:
    """
    Publish QoS0 messages for either a fixed count or a fixed duration.

    Returns the number of messages sent.
    """
    if publish_count <= 0 and duration_secs <= 0:
        return 0
    payload_list = list(payloads)
    if not payload_list:
        raise MqttError("payloads is empty")

    broker = parse_tcp_broker_url(broker_url)
    if client_id is None:
        client_id = f"perf-daily-{os.getpid()}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sent = 0
    try:
        sock.settimeout(connect_timeout_secs)
        sock.connect((broker.host, broker.port))
        sock.settimeout(None)
        sock.sendall(_build_connect_packet(client_id, keepalive_secs=keepalive_secs))
        _read_connack(sock)

        start = time.time()
        i = 0
        while True:
            now = time.time()
            if duration_secs > 0 and (now - start) >= duration_secs:
                break
            if duration_secs <= 0 and i >= publish_count:
                break

            payload = payload_list[i % len(payload_list)]
            sock.sendall(_build_publish_packet(topic, payload))
            sent += 1
            i += 1

            if rate_per_sec > 0:
                # Basic pacing: next send at start + i/rate.
                target = start + (i / float(rate_per_sec))
                delay = target - time.time()
                if delay > 0:
                    time.sleep(delay)
    finally:
        try:
            sock.close()
        except Exception:
            pass

    return sent


def publish_qos0_with_timing(
    broker_url: str,
    topic: str,
    payloads: Iterable[bytes],
    publish_count: int = 0,
    duration_secs: int = 0,
    rate_per_sec: int = 0,
    client_id: Optional[str] = None,
    connect_timeout_secs: float = 10.0,
    keepalive_secs: int = 60,
) -> PublishResult:
    """
    Publish QoS0 messages and also return the "load window" timestamps.

    start_ts_ms/end_ts_ms are recorded around the actual send loop, so they are a
    better alignment boundary for metrics than the surrounding orchestration.
    """
    if publish_count <= 0 and duration_secs <= 0:
        now = int(time.time() * 1000)
        return PublishResult(sent=0, start_ts_ms=now, end_ts_ms=now)

    payload_list = list(payloads)
    if not payload_list:
        raise MqttError("payloads is empty")

    broker = parse_tcp_broker_url(broker_url)
    if client_id is None:
        client_id = f"perf-daily-{os.getpid()}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sent = 0
    start_ms = 0
    end_ms = 0
    try:
        sock.settimeout(connect_timeout_secs)
        sock.connect((broker.host, broker.port))
        sock.settimeout(None)
        sock.sendall(_build_connect_packet(client_id, keepalive_secs=keepalive_secs))
        _read_connack(sock)

        # Alignment boundary: starts when we enter the send loop.
        start_ms = int(time.time() * 1000)
        start = time.time()
        i = 0
        while True:
            now = time.time()
            if duration_secs > 0 and (now - start) >= duration_secs:
                break
            if duration_secs <= 0 and i >= publish_count:
                break

            payload = payload_list[i % len(payload_list)]
            sock.sendall(_build_publish_packet(topic, payload))
            sent += 1
            i += 1

            if rate_per_sec > 0:
                # Basic pacing: next send at start + i/rate.
                target = start + (i / float(rate_per_sec))
                delay = target - time.time()
                if delay > 0:
                    time.sleep(delay)
        end_ms = int(time.time() * 1000)
    finally:
        if end_ms == 0:
            end_ms = int(time.time() * 1000)
        if start_ms == 0:
            start_ms = end_ms
        try:
            sock.close()
        except Exception:
            pass

    return PublishResult(sent=sent, start_ts_ms=start_ms, end_ts_ms=end_ms)
