#!/usr/bin/env python3
"""Send one RESP request and read exactly one bounded RESP2 response frame."""

from __future__ import annotations

import os
import re
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable


class TransportError(RuntimeError):
    """The transport did not deliver one valid, safely bounded RESP frame."""


@dataclass(frozen=True)
class Limits:
    header_bytes: int = 1024
    bulk_bytes: int = 64 * 1024 * 1024
    array_elements: int = 1_000_000
    recursion_depth: int = 128
    frame_bytes: int = 128 * 1024 * 1024


DEFAULT_LIMITS = Limits()
_INTEGER = re.compile(rb"(?:0|-?[1-9][0-9]*)\Z")
_LENGTH = re.compile(rb"(?:-1|0|[1-9][0-9]*)\Z")


class _Budget:
    def __init__(self, maximum: int) -> None:
        self.maximum = maximum
        self.used = 0

    def consume(self, length: int, context: str) -> None:
        if length > self.maximum - self.used:
            raise TransportError(
                f"transport_limit context={context} limit=frame_bytes maximum={self.maximum}"
            )
        self.used += length


def _read_exact(stream: BinaryIO, length: int, context: str, budget: _Budget) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            received = length - remaining
            raise TransportError(
                f"transport_truncated context={context} expected={length} received={received}"
            )
        budget.consume(len(chunk), context)
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_header(
    stream: BinaryIO, kind: bytes, limits: Limits, budget: _Budget
) -> bytes:
    header = bytearray()
    while not header.endswith(b"\r\n"):
        if len(header) >= limits.header_bytes:
            raise TransportError(
                f"transport_limit context={kind.decode()}_header "
                f"limit=header_bytes maximum={limits.header_bytes}"
            )
        header.extend(_read_exact(stream, 1, f"{kind.decode()}_header", budget))
    return bytes(header)


def _validate_text_payload(payload: bytes, kind: bytes) -> None:
    if any(byte < 0x20 or byte == 0x7F for byte in payload):
        raise TransportError(
            f"transport_invalid context={kind.decode()}_payload control_byte=true"
        )


def _read_frame(
    stream: BinaryIO, limits: Limits, budget: _Budget, depth: int
) -> bytes:
    if depth > limits.recursion_depth:
        raise TransportError(
            f"transport_limit context=array limit=recursion_depth "
            f"maximum={limits.recursion_depth}"
        )

    kind = _read_exact(stream, 1, "frame_prefix", budget)
    if kind not in {b"+", b"-", b":", b"$", b"*"}:
        raise TransportError(f"transport_invalid context=frame_prefix value={kind!r}")
    header = _read_header(stream, kind, limits, budget)
    payload = header[:-2]
    frame = bytearray(kind + header)
    if kind in {b"+", b"-"}:
        _validate_text_payload(payload, kind)
        return bytes(frame)
    if kind == b":":
        if not _INTEGER.fullmatch(payload):
            raise TransportError(
                f"transport_invalid context=integer_payload value={payload!r}"
            )
        return bytes(frame)

    if not _LENGTH.fullmatch(payload):
        raise TransportError(
            f"transport_invalid context={kind.decode()}_length value={payload!r}"
        )
    count = int(payload)
    if count == -1:
        return bytes(frame)
    if kind == b"$":
        if count > limits.bulk_bytes:
            raise TransportError(
                f"transport_limit context=bulk limit=bulk_bytes maximum={limits.bulk_bytes}"
            )
        body = _read_exact(stream, count + 2, "bulk_body", budget)
        if not body.endswith(b"\r\n"):
            raise TransportError("transport_invalid context=bulk_body missing_crlf=true")
        frame.extend(body)
        return bytes(frame)

    if count > limits.array_elements:
        raise TransportError(
            f"transport_limit context=array limit=array_elements "
            f"maximum={limits.array_elements}"
        )
    for _ in range(count):
        frame.extend(_read_frame(stream, limits, budget, depth + 1))
    return bytes(frame)


def read_one_frame(stream: BinaryIO, limits: Limits = DEFAULT_LIMITS) -> bytes:
    """Read one recursively complete RESP2 frame without an elapsed-time gate."""

    return _read_frame(stream, limits, _Budget(limits.frame_bytes), 0)


def encode_request(args: Iterable[str]) -> bytes:
    encoded = [arg.encode() for arg in args]
    parts = [f"*{len(encoded)}\r\n".encode()]
    for arg in encoded:
        parts.extend((f"${len(arg)}\r\n".encode(), arg, b"\r\n"))
    return b"".join(parts)


def request_one_frame(port: int, args: Iterable[str]) -> bytes:
    """Use OS transport failures; protocol framing, not silence, ends a response."""

    try:
        connection = socket.create_connection(("127.0.0.1", port))
    except OSError as error:
        raise TransportError(
            f"transport_os_error operation=connect detail={error}"
        ) from error
    with connection:
        try:
            connection.sendall(encode_request(args))
        except OSError as error:
            raise TransportError(
                f"transport_os_error operation=send detail={error}"
            ) from error
        try:
            with connection.makefile("rb") as stream:
                return read_one_frame(stream)
        except OSError as error:
            raise TransportError(
                f"transport_os_error operation=read detail={error}"
            ) from error


def main() -> None:
    port = int(os.environ["RESP_PORT"])
    output = Path(os.environ["RESP_OUT"])
    args = os.environ["RESP_ARGS"].splitlines()
    try:
        frame = request_one_frame(port, args)
    except TransportError as error:
        print(
            f"resp_one_frame: classification=transport_error {error}",
            file=sys.stderr,
        )
        raise SystemExit(75) from error
    output.write_bytes(frame)


if __name__ == "__main__":
    main()
